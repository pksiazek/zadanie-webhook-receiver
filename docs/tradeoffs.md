# Kompromisy (tradeoffy) — szczegóły

[← README](../README.md)

Pełne uzasadnienia decyzji, skrócone w
[README](../README.md#kompromisy-tradeoffy). Źródło:
[dokument projektowy](superpowers/specs/2026-10-25-webhook-receiver-design.md).

## Spis treści

- [Readiness API niezależne od workera](#readiness-api-niezależne-od-workera)
- [`synchronous=FULL` zamiast `NORMAL`](#synchronousfull-zamiast-normal)
- [Schemat](#schemat)
- [SQLite współdzielony przez kilka kontenerów](#sqlite-współdzielony-przez-kilka-kontenerów)
- [Semantyka: co najmniej raz z efektem dokładnie raz](#semantyka-co-najmniej-raz-z-efektem-dokładnie-raz)
- [Retencja: dwa progi, GDPR, plik nie maleje](#retencja-dwa-progi-gdpr-plik-nie-maleje)
- [Jeden proces uvicorn](#jeden-proces-uvicorn)
- [Duplikat bez porównania treści](#duplikat-bez-porównania-treści)
- [Klucze `.` i `..`](#klucze--i-)
- [Gwarancje: założenia i restart workera](#gwarancje-założenia-i-restart-workera)
- [Logi JSON na stdout, zapisywane synchronicznie](#logi-json-na-stdout-zapisywane-synchronicznie)

## Readiness API niezależne od workera

`/ready` API sprawdza tylko bazę i schemat. **Przy niedziałających
workerach `/ready` jest zielone**, a API dalej przyjmuje webhooki (`202`).

To cel architektury: przyjmowanie nie zależy od przetwarzania, a zapisane
zdarzenia poczekają w kolejce.

**Jak wtedy wykryć problem:**

- healthcheck workera w `docker compose ps` (`unhealthy` albo brak
  kontenerów),
- rosnący `webhook_queue_oldest_pending_age_seconds`.

**Świadome ograniczenie:** przy pustej kolejce API **nie odróżnia
bezczynnego workera od martwego**.

- Nie ma tabeli heartbeatów (usunięta jako zbędna złożoność; wykrycie
  awarii przejmuje healthcheck Dockera).
- Liczbę żywych workerów pokazuje za to metryka `webhook_worker_up` w
  OpenTelemetry Collectorze (jedna seria na proces,
  → [operacje](operations.md#ile-workerów-działa)). API z niej nie
  korzysta.

## `synchronous=FULL` zamiast `NORMAL`

`202` jest obietnicą trwałości.

- W trybie WAL `NORMAL` przetrwa awarię procesu, ale przy zaniku
  zasilania lub awarii systemu może zgubić ostatnie zatwierdzone
  transakcje — czyli zdarzenia, którym już odpowiedziano `202`.
- `FULL` robi fsync przy każdym commicie.

**Koszt zmierzony:** ok. 2 ms na zapis przy 100 tx/s (0,4 ms bez przerw),
`NORMAL` 0,2 ms. Wobec potrzebnych ≤ ~50 tx/s to pomijalne, a p99 `POST` w
teście obciążeniowym wyniosło 19,3 ms.

## Schemat

Jedna tabela `webhook_events` (`STRICT`) i cztery indeksy częściowe
([DDL](../src/webhook_receiver/common/db.py)).

### Kolumny

- **`id INTEGER PRIMARY KEY` obok `idempotency_key UNIQUE`.**
  - `id` to tylko alias ukrytego `rowid` (bez dodatkowej kolumny i
    indeksu).
  - Indeks `UNIQUE` na kluczu istniałby także przy kluczu jako PK.
  - `WITHOUT ROWID` jest odradzane dla dużych wierszy (payload do
    256 KiB), a indeksy częściowe trzymałyby wtedy cały klucz (do 255 B)
    zamiast 8-bajtowego `rowid`.
  - Worker operuje na `id`, API wyłącznie na kluczu.
- **Czasy jako INTEGER (ms od epoki)** — poprawne porównania i prosta
  arytmetyka (`now + lease`, odstęp ponowienia, progi retencji). Podgląd:
  `datetime(col/1000, 'unixepoch')`.
- **`status` jako TEXT + `CHECK`** — czytelność dla operatora w `sqlite3`
  ważniejsza niż kilka bajtów.
- **JSON jako TEXT** — payload tylko zapisujemy i czytamy w całości.
- **`STRICT`** wymusza deklarowane typy.
- **`run_at` zamiast dwóch terminów** — termin podjęcia dla
  `pending`/`retrying` i wygaśnięcie rezerwacji dla `processing` w jednej
  kolumnie i jednym indeksie.

### Indeksy

**Indeksy częściowe dokładnie pod zapytania.** SQLite używa indeksu
częściowego tylko, gdy warunek zapytania zawiera dosłownie warunek
indeksu. Dlatego:

- `ix_events_ready (run_at, status) WHERE status IN ('pending','retrying')`
  — rezerwacja i gauge'e (jako indeks pokrywający),
- `ix_events_leased (run_at) WHERE status = 'processing'` — odzyskiwanie,
- `ix_events_finished (status, finished_at)` — usuwanie wierszy w
  retencji.

**Osobny indeks do czyszczenia payloadu:**
`ix_events_purgeable (finished_at) WHERE status = 'completed' AND
payload_json IS NOT NULL`.

- Zawiera tylko wiersze, które wciąż mają payload.
- Bez niego każda porcja przechodziła przez wszystkie wyczyszczone już
  wiersze `completed` w wieku 7–30 dni — pod blokadą zapisu.
- Planer bez statystyk `ANALYZE` wybiera `ix_events_finished`, dlatego
  zapytanie wskazuje indeks przez `INDEXED BY`.
- Koszt: jeden wpis indeksu więcej przy zakończeniu zdarzenia.
- Istniejące bazy dostają indeks przy starcie (`CREATE … IF NOT EXISTS`).

Test jednostkowy sprawdza `EXPLAIN QUERY PLAN` każdego z tych zapytań.

### Kontrola numeru próby i co usunięto

- **Kontrola numeru próby zamiast `lease_owner`** — `attempts` jest
  znacznikiem rezerwacji. Identyfikator workera trafia do logów, spanów i
  `result_json`, nie do schematu.
- **Usunięte jako zbędne:**
  - `max_attempts` (wartość z konfiguracji),
  - `updated_at`,
  - `payload_purged_at` (wynika z `payload_json IS NULL`),
  - tabela heartbeatów.

## SQLite współdzielony przez kilka kontenerów

**Dlaczego to działa:** kontenery na jednym hoście współdzielą jądro
Linuksa.

- SQLite synchronizuje procesy blokadami plików (POSIX `fcntl`).
- W trybie WAL także plikiem pamięci współdzielonej `-shm` mapowanym przez
  `mmap`.
- Dla jądra nie ma różnicy, czy procesy są w jednym kontenerze, czy w
  kilku.

**Warunki i ograniczenia:**

- **Tylko jeden host.** Workerów nie da się rozłożyć na kilka maszyn.
  Baza na NFS lub innym sieciowym systemie plików jest niedozwolona —
  blokady i `mmap` są tam zawodne i grożą uszkodzeniem bazy.
- **Nazwany wolumen, nie bind mount.** Na macOS bind mount przechodzi
  przez warstwę współdzielenia plików z hostem, gdzie semantyka blokad nie
  jest gwarantowana. Nazwany wolumen leży na lokalnym systemie plików
  maszyny wirtualnej.
- **Prawo zapisu do katalogu bazy we wszystkich kontenerach** (SQLite
  tworzy obok pliki `-wal` i `-shm`) — jeden obraz i ten sam UID (10001)
  dla api i workerów.
- **Jeden pisarz naraz** — pułap ok. 2200–2300 tx/s przy nasyceniu i
  niesprawiedliwa blokada ([benchmark 3](results.md#3-rywalizacja-o-zapis)).
  - `busy_timeout=5000` łagodzi chwilową rywalizację.
  - Przy trwałym przeciążeniu pojawiają się błędy (`503` w API) i
    opóźnienia, nie cicha utrata danych.

**Poprawność logiczna nie zależy od liczby kontenerów:** atomowa
rezerwacja (`UPDATE … RETURNING` w `BEGIN IMMEDIATE`) i warunkowy zapis
wyniku. Wariant z osobnymi kontenerami na wspólnym wolumenie potwierdza
scenariusz `scaling` (3000 / 3000 przyjętych przy każdym N, 0 błędów
blokady).

## Semantyka: co najmniej raz z efektem dokładnie raz

Opisana w
[Co naprawdę gwarantujemy](../README.md#co-naprawdę-gwarantujemy-dokładnie-raz):

- zapis zdarzenia dokładnie raz,
- wykonanie co najmniej raz (rezerwacja z terminem wygaśnięcia),
- zapis wyniku dokładnie raz (kontrola numeru próby).

**Kompromis:** po awarii workera symulacja może zostać wykonana ponownie,
a zdarzenie czeka do `LEASE_SECONDS` (30 s) na odzyskanie.

Krótsza rezerwacja skraca to opóźnienie, ale musi być dłuższa od
najdłuższej próby (walidowane: `LEASE_SECONDS > SIMULATED_MAX_SECONDS`).
Inaczej pojawią się fałszywe „lease expired”.

## Retencja: dwa progi, GDPR, plik nie maleje

| Dane | Domyślnie | Co się dzieje |
|---|---|---|
| `payload_json` i `result_json` zdarzeń `completed` | 7 dni od `finished_at` | ustawiane na `NULL`, `payload_purged: true` |
| cały wiersz `completed` | 30 dni | usuwany |
| cały wiersz `failed` (z payloadem — do diagnozy) | 30 dni | usuwany |
| zdarzenia niezakończone | — | nigdy nie są usuwane |

- **Czyszczenie payloadu służy przede wszystkim GDPR** (minimalizacja
  przechowywania danych osobowych — w przykładach są e-mail oraz imię i
  nazwisko), nie odzyskiwaniu miejsca.
- **Payload zdarzeń `failed` zostaje dłużej.**
  - Może zawierać dane osobowe, a jest trzymany 30 dni do diagnozy —
    dłużej niż 7 dni dla `completed`.
  - Okres skraca `RETENTION_FAILED_DAYS` (usuwa wtedy cały wiersz).
  - Osobny próg czyszczenia payloadu albo maskowanie dla `failed` →
    [TODO](../README.md#todo).
- **Okno retencji klucza = okno ochrony przed duplikatami.**
  - Po wyczyszczeniu payloadu duplikat jest nadal rozpoznawany (`200`,
    bez wyniku).
  - Po usunięciu wiersza ten sam klucz zostanie przyjęty jako nowe
    zdarzenie.
- **Plik bazy nie maleje.**
  - SQLite oznacza zwolnione strony jako wolne i używa ich ponownie, więc
    retencja **ogranicza wzrost** pliku, ale go nie zmniejsza.
  - `VACUUM` blokuje bazę na cały czas przebudowy, więc nie nadaje się do
    pracy w tle.
  - `auto_vacuum=INCREMENTAL` → [TODO](../README.md#todo).
- **Kto i jak często.**
  - Retencję uruchamia każdy worker co `RETENTION_INTERVAL_SECONDS`
    (10 min).
  - Porcjami po 1000 wierszy (`WHERE id IN (SELECT … LIMIT 1000)`, bo
    `DELETE … LIMIT` wymaga opcji kompilacji SQLite).
  - Operacja jest idempotentna, więc kilka workerów naraz jest bezpieczne.

## Jeden proces uvicorn

API działa jako jeden proces uvicorn (bez `--workers N`). Rate limiter i
cache statystyk kolejki (backpressure, gauge'e) żyją w pamięci procesu.

- Kilka procesów lub instancji API miałoby osobne limity per `X-Source`
  (efektywny limit × liczba procesów) i osobne cache.
- Jeden proces wystarcza z dużym zapasem: p99 `POST` ≤ 21 ms przy
  3000/min, CPU API poniżej 1% w `scaling`.
- Skalowanie API wymaga współdzielonego limitera
  (→ [TODO](../README.md#todo)).

## Duplikat bez porównania treści

Duplikat = ten sam `idempotency_key`, **niezależnie od treści** —
dosłownie według zadania („ten sam webhook wysłany wielokrotnie nie może
być przetworzony więcej niż raz”). Ponowienie nie jest porównywane z
oryginałem ani zapisywane.

**Kompromis:** nadawca, który przez błąd użyje tego samego klucza dla
innego zdarzenia, dostanie `200` bez sygnału błędu, a drugie zdarzenie
przepadnie.

Wykrywanie niezgodności (hash treści + `409` albo metryka) →
[TODO](../README.md#todo).

## Klucze `.` i `..`

Klucz może być dowolnym napisem do 255 znaków. W ścieżce `GET` jest
kodowany procentowo (np. `evt/1` → `evt%2F1`; trasa używa konwertera
`{idempotency_key:path}`).

**Ograniczenie: kluczy `.` i `..` nie da się odpytać przez ścieżkę.**
Klienci HTTP i proxy normalizują takie segmenty — `POST` je przyjmie, ale
`GET` nie trafi w zdarzenie.

- Klucze ze znakami sterującymi są dozwolone (w logach JSON są zapisywane
  jako sekwencje ucieczki).
- Docelowe rozwiązanie zależy od uzgodnienia formatu klucza z nadawcami
  (→ [TODO](../README.md#todo)).

## Gwarancje: założenia i restart workera

**Założenie:** prawdziwy efekt uboczny (zamiast symulacji) musiałby być
idempotentny względem `idempotency_key`. Wykonanie może się bowiem
powtórzyć, gdy worker padnie po wykonaniu, a przed zapisem wyniku.

**Restart lub awaria workera:**

- Zdarzenia w `processing` czekają na wygaśnięcie rezerwacji (do 30 s),
  potem są odzyskiwane.
- Łagodne zamykanie (SIGTERM) przestaje pobierać pracę i czeka na trwające
  próby do `SHUTDOWN_GRACE_SECONDS` (10 s).
- `stop_grace_period: 20s` w compose pilnuje, żeby Docker nie zabił
  workera wcześniej.

**Granica ochrony przed duplikatami to retencja:** po usunięciu wiersza
(30 dni od zakończenia) ten sam klucz zostanie przyjęty jako nowe
zdarzenie ([retencja](#retencja-dwa-progi-gdpr-plik-nie-maleje)).

## Logi JSON na stdout, zapisywane synchronicznie

Logi to JSON na stdout (12-factor). Proces nie zarządza plikami ani
rotacją, a wpisy zbiera Docker (`docker compose logs`).

**Koszt:** zapis jest synchroniczny i odbywa się w ścieżce żądania API
oraz próby workera.

- Każdy wpis kosztuje czas i CPU — im więcej wpisów, tym większy narzut.
- Sterownik `json-file` Dockera dokłada zapis na dysk.

**Pomiary:** test obciążeniowy działał z logami na stdout — wtedy jeszcze
z włączonym logiem dostępu uvicorna (wyłączonym później), czyli z większą
liczbą wpisów niż obecnie — i spełnił kryteria. Kosztu samych logów nie
mierzyliśmy osobno.

Zapis poza ścieżką żądania (`QueueHandler`/`QueueListener`, plik z
rotacją albo logi OTLP przez Collector, próbkowanie częstych wpisów) i
pomiar jego wpływu → [TODO](../README.md#todo).
