# Webhook Receiver Service — dokument projektowy

- **Data:** 2026-10-25
- **Status:** zaakceptowany (2026-10-25)
- **Zadanie źródłowe:** <https://gist.github.com/kaluzaaa/fcd035829013d9952f71a9a6a59a4d2f#file-gist_webhook_receiver-md>
- **Przebieg ustaleń:** [2026-10-25-webhook-receiver-brainstorming.md](2026-10-25-webhook-receiver-brainstorming.md)

Oznaczenia: **[ustalone]** — decyzja podjęta wspólnie z autorem zadania;
**[decyzja projektanta]** — wybór projektanta, do weryfikacji przy akceptacji;
**[dla wykonawcy]** — szczegół pozostawiony implementującemu.

## 1. Cel i uzasadnienie

Zadanie rekrutacyjne: prototyp serwisu HTTP w Pythonie, który przyjmuje
webhooki (JSON z `idempotency_key`), gwarantuje ich przetworzenie i udostępnia
status każdego zdarzenia. Oceniający patrzą na **sposób myślenia**:
niezawodność, idempotencję, rozdzielenie przyjmowania webhooków od przetwarzania, model
danych, obserwowalność i jawnie nazwane kompromisy. Kod ma być działający,
zgodny z PEP 8, ale ważniejsza jest architektura i uzasadnienia w README.

Zakres obejmuje **wszystkie punkty bonusowe** [ustalone]: testy jednostkowe,
rate limiting, metryki + health/readiness, retry dla nieudanych webhooków,
test obciążeniowy.

Proces wytwarzania: pełna dyscyplina superpowers (plan → TDD → code review →
weryfikacja przed ukończeniem) [ustalone]. Repozytorium publiczne na GitHubie:
`zadanie-webhook-receiver` [ustalone].

## 2. Cele, nie-cele, anty-cele

### Cele

1. Każdy webhook, który dostał odpowiedź `202`, zostanie przetworzony —
   także po restarcie lub awarii procesu.
2. Duplikat (ten sam `idempotency_key`, bez porównywania treści) nie jest
   przetwarzany ponownie; dostaje aktualny status.
3. Klient może w każdej chwili sprawdzić status zdarzenia.
4. Poprawne działanie przy wielu równoczesnych webhookach; przyjmowanie
   wytrzymuje ~1000 webhooków/min z niskim opóźnieniem.
5. Zdefiniowana, konfigurowalna retencja danych.
6. Ślad (trace) obejmujący drogę od `POST` do wyniku przetwarzania, mimo
   granicy procesów.

### Nie-cele

- Prawdziwa logika biznesowa (przetwarzanie to symulacja `sleep(2–5 s)`).
- Uwierzytelnianie nadawców / weryfikacja podpisów HMAC (→ TODO).
- Wdrożenie wielohostowe, wiele instancji API ze współdzielonym limitem
  (→ TODO).
- Pełny stos do oglądania telemetrii (Grafana/Tempo/Prometheus z
  dashboardami) (→ TODO).

### Anty-cele (porażka, nawet jeśli „działa”)

- Zgubienie zdarzenia, któremu odpowiedziano `202`.
- Ponowne przetworzenie duplikatu w normalnym trybie pracy.
- Uzależnienie przyjmowania od tempa przetwarzania — rosnąca kolejka nie
  może wydłużać odpowiedzi na `POST` ani powodować przy nim błędów.
- Obietnica „exactly once” w README bez wyjaśnienia, co naprawdę jest
  gwarantowane.

## 3. Ograniczenia

- **Python 3.14** [ustalone] — w devcontainerze, obrazie Dockera i CI; baza
  obrazów Debian trixie (devcontainer
  `mcr.microsoft.com/devcontainers/python:3.14-trixie` — tag `1-3.14-trixie`
  nie istnieje; Docker `python:3.14-slim`, który jest oparty na trixie). 3.15 odrzucony jako zbyt świeży (wydany ~2 tyg.
  temu — ryzyko braku gotowych wheeli zależności). Zmiana obrazu
  devcontainera razem z dodaniem docker-in-docker (jedna przebudowa).
- FastAPI [ustalone], SQLite [wymóg zadania]. **Minimalna wersja biblioteki
  SQLite: 3.37** (`RETURNING` od 3.35, `STRICT` od 3.37) [ustalone];
  Python używa systemowej `libsqlite3` obrazu (bookworm: 3.40.1, trixie:
  3.46.1 — obie zweryfikowane).
  Każdy proces sprawdza `sqlite3.sqlite_version` przy starcie i kończy się
  czytelnym błędem poniżej minimum. Najnowszego SQLite (3.54.1) nie
  dołączamy — wymagałoby kompilacji lub osobnej paczki, a nie daje
  potrzebnych nam funkcji.
- Dockerfile [wymóg], README z architekturą, tradeoffami, instrukcją
  uruchomienia i sekcją TODO [wymóg].
- Budżet zadania 2–4 h; zakres bonusowy świadomie go przekracza [ustalone].
- SQLite działa poprawnie między procesami tylko na lokalnym systemie plików
  jednego hosta (nie na NFS) — determinuje granicę skalowania. W compose
  baza leży na **nazwanym wolumenie**, nie na bind mouncie.
- Devcontainer nie ma Dockera. Żeby Dockerfile, compose i test obciążeniowy
  dało się zweryfikować w środowisku pracy, **pierwszym krokiem
  implementacji jest dodanie feature `docker-in-docker` do devcontainera**
  i jego przebudowa [ustalone]. Wybrane zamiast `docker-outside-of-docker`,
  bo nie daje dostępu do demona i plików hosta.
- API uruchamiane jako **jeden proces uvicorn** (bez `--workers N`): rate
  limiter i cache licznika zaległości żyją w pamięci procesu. Skalowanie API
  wymagałoby współdzielonego stanu (→ TODO) [decyzja projektanta].

## 4. Semantyka „dokładnie raz” — co naprawdę gwarantujemy

Dokładnie-raz end-to-end jest niemożliwe bez współpracy efektu ubocznego.
Gwarantujemy [ustalone — do opisania w README]:

- **Zapis zdarzenia dokładnie raz:** `UNIQUE(idempotency_key)` w SQLite;
  zapis i decyzja „nowe/duplikat” w jednej transakcji.
- **Wykonanie co najmniej raz:** jeśli worker padnie w trakcie, rezerwacja
  (lease) wygasa i zdarzenie jest podejmowane ponownie.
- **Efekt dokładnie raz:** zapis wyniku jest warunkowy — tylko ze stanu
  `processing` i tylko dla aktualnego numeru próby (`attempts` działa jak
  znacznik rezerwacji — kontrola numeru próby, patrz §8), więc spóźniony worker z wygasłą rezerwacją
  nie nadpisze wyniku. Prawdziwy efekt uboczny musiałby
  być idempotentny względem klucza — to opisujemy jako założenie.
- **Restart lub awaria API między `COMMIT` a wysłaniem `202`:** klient nie
  dostaje odpowiedzi i ponawia; ponowienie trafia na istniejący klucz i
  dostaje `200` z aktualnym statusem — zdarzenie nie ginie i nie jest
  przetwarzane dwa razy. Awaria przed `COMMIT` = brak zapisu, ponowienie
  zapisuje zdarzenie jako nowe.

## 5. Wybrane podejście

### Rozważone podejścia

| | Opis | Plusy | Minusy |
|---|---|---|---|
| A | Jeden proces: FastAPI + pula zadań asyncio w tym samym procesie | najprostsze, 1 kontener | API i przetwarzanie dzielą pętlę zdarzeń i los; brak niezależnego skalowania; przy zalewie przetwarzanie spowalnia przyjmowanie |
| **B** | **Osobne procesy `api` i `worker`, wspólny kod i plik SQLite; SQLite jako magazyn i kolejka** | izolacja przyjmowania od przetwarzania; `--scale worker=N`; prawdziwa granica procesów dla `traceparent`; scenariusz „kolejkuje i schodzi” wynika z architektury | więcej elementów; opóźnienie pollingu (~0,5 s); konkurencja o zapis w SQLite między procesami; tylko jeden host |
| C | Zewnętrzny broker (Redis/Celery) + SQLite jako magazyn | skala, gotowe retry | zadanie narzuca SQLite; dual-write → wymaga outboxa; dodatkowa infrastruktura — odrzucone, do TODO jako ścieżka skalowania |

**Wybrane: B** [ustalone]. Uzasadnienie: bezpośrednio realizuje cel testu
obciążeniowego (przyjmowanie stabilne, przetwarzanie kolejkuje się i
odrabia), a SQLite jako kolejka eliminuje problem dual-write — wiersz ze
statusem `pending` *jest* zadaniem, zapisanym w tej samej transakcji co
zapis zdarzenia.

Dodatkowo [decyzja projektanta]: przełącznik konfiguracyjny
`EMBEDDED_WORKER=true` uruchamiający worker w procesie API — wyłącznie dla
wygody lokalnego developmentu i testów; tryb domyślny i docker compose to
osobne procesy. W tym trybie nie startuje pomocniczy serwer workera (9100),
a metryki workera trafiają na `/metrics` API.

### Architektura

```
 nadawcy ──HTTP──► [ api (FastAPI, uvicorn) ] ──INSERT (tx)──► [ SQLite (WAL) ]
                     │ rate limit per X-Source                   ▲   │
                     │ walidacja, idempotencja                    │   │ rezerwacja (UPDATE…RETURNING)
                     │ /metrics /health /ready                    │   ▼
                                                     [ worker(s) (asyncio, N slotów) ]
                                                       │ symulacja, retry, lease,
                                                       │ retencja
                                                       │ /metrics /health (port pomocniczy)
           OpenTelemetry: traces + metrics ──► konsola (domyślnie) / OTLP ──► Jaeger
```

## 6. API HTTP

`POST /webhooks` wymaga nagłówka `X-Source` (identyfikator systemu
nadawcy) [ustalone]. **Powodem jest rate limiting** — limit liczony jest
per nadawca, więc nadawcę trzeba znać; dodatkowo `X-Source` służy jako
etykieta w metrykach, logach i śladach. Brak → `400`. `GET` statusu
nagłówka nie wymaga.

### `POST /webhooks`

Body:

```json
{
  "idempotency_key": "evt-001",
  "event_type": "order.created",
  "payload": { "...": "..." }
}
```

Walidacja: `idempotency_key` — dowolny niepusty string, maks. 255 znaków,
bez ograniczenia zestawu znaków [ustalone]. W ścieżce `GET` klucz jest
kodowany procentowo (RFC 3986, np. `evt/1` → `evt%2F1`). Uvicorn/Starlette
dekodują procenty **przed** routingiem, więc `evt%2F1` trafia do routera
jako `evt/1` — trasa musi używać konwertera ścieżki:
`@app.get("/webhooks/{idempotency_key:path}")` (domyślny parametr nie
dopasowuje `/` i daje `404`). Pusty klucz (`GET /webhooks/`) → `404`.
`Location` i `status_url` (względne, np. `/webhooks/evt%2F1`) budowane
przez `urllib.parse.quote(key, safe="")`, więc klient może ich użyć
wprost. Ograniczenie (do README i punktu TODO o formacie klucza): kluczy
`.` i `..` nie da się odpytać przez ścieżkę, bo klienci HTTP i proxy
normalizują takie segmenty. Klucze ze znakami sterującymi są dozwolone —
w logach JSON są escapowane przez serializator.
`X-Source` — niepusty string, maks. 64 znaki;
`event_type` — niepusty string; `payload` — obiekt JSON. Limit rozmiaru
body: 256 KiB → `413` [decyzja projektanta] — wymaga własnego middleware
(Starlette/uvicorn nie limitują rozmiaru body): sprawdzenie `Content-Length` oraz
liczenie bajtów przy czytaniu strumienia (żądania chunked). Błędy
walidacji → `422`.

Odpowiedzi:

| Sytuacja | Kod | Body |
|---|---|---|
| nowe zdarzenie, zapisane trwale | `202 Accepted` | `{idempotency_key, status: "pending", status_url}` + nagłówek `Location` |
| duplikat (ten sam `idempotency_key`) | `200 OK` | aktualny status (jak w `GET`), pole `duplicate: true` |
| brak `X-Source` | `400` | opis błędu |
| przekroczony limit nadawcy | `429` + `Retry-After` [ustalone] | opis |
| przekroczony próg zaległości (backpressure) | `503` + `Retry-After` [decyzja projektanta] | opis |

**Kolejność kontroli w `POST`** [decyzja projektanta, po recenzji]:
`X-Source` (`400`) → rozmiar (`413`) → walidacja body (`422`) → rate
limit (`429`; duplikaty też się liczą) → backpressure → zapis. Przy
przekroczonym progu zaległości serwis najpierw sprawdza, czy klucz już
istnieje: **duplikat dostaje `200` z aktualnym statusem, a nie `503`**
[ustalone] — nie dodaje pracy, a nadawca w pętli ponowień nie pogłębia
zalewu. `503` dostają tylko nowe zdarzenia. `Retry-After` dla `503`:
stała konfigurowalna (domyślnie 5 s) [decyzja projektanta].

**Duplikat = ten sam `idempotency_key`, niezależnie od treści** [ustalone —
dosłownie wg zadania]. Treść ponowienia nie jest porównywana; zwracany jest
status pierwotnego zdarzenia, a ponowienie nie jest zapisywane ani
przetwarzane. Świadomy kompromis (do README): nadawca, który przez błąd
użyje tego samego klucza dla innego zdarzenia, nie dostanie sygnału, a
drugie zdarzenie przepadnie — wykrywanie niezgodności treści (hash +
`409` lub metryka) → TODO.

`idempotency_key` jest **globalnie unikalny** [ustalone]. `X-Source`
służy wyłącznie do rate limitingu i obserwowalności (metryki, logi,
atrybuty spanów) — nie jest częścią tożsamości zdarzenia. W rekordzie
zapisujemy źródło pierwszego zgłoszenia; ponowienie z innego źródła to
zwykły duplikat (`200`).

`202` zwracane **dopiero po zatwierdzeniu transakcji** na dysku.

### `GET /webhooks/{idempotency_key}`

Nie wymaga `X-Source` (klucz jest globalny). Zwraca:

```json
{
  "idempotency_key": "evt-001",
  "source": "shop",
  "event_type": "order.created",
  "status": "retrying",
  "attempts": 1,
  "max_attempts": 5,
  "next_attempt_at": "2026-10-25T10:00:08Z",
  "last_error": "simulated failure (fail_once)",
  "result": null,
  "created_at": "...",
  "finished_at": null,
  "payload_purged": false
}
```

Mapowanie na schemat (§7): czasy w API w ISO 8601 UTC (w bazie INTEGER ms);
`max_attempts` z konfiguracji; `next_attempt_at` = `run_at`, gdy status to
`pending` lub `retrying` (inaczej `null`); `finished_at` = moment wejścia w
`completed` lub `failed`; `payload_purged` = `finished_at IS NOT NULL AND
payload_json IS NULL`. Nieznany klucz → `404`.

### Endpointy operacyjne

- `GET /health` — liveness: proces odpowiada.
- `GET /ready` — readiness API: baza dostępna (zapytanie) i schemat
  zainicjalizowany; inaczej `503` z listą niespełnionych warunków.
  **Brak workera nie wpływa na gotowość API** — przyjmowanie ma działać
  niezależnie od przetwarzania (to cel architektury B). API nie śledzi
  żywotności workerów (brak tabeli heartbeatów — decyzja użytkownika):
  awarię workera wykrywa healthcheck Dockera na jego `/health`, a brak
  postępu przetwarzania — rosnący `webhook_queue_oldest_pending_age_seconds`
  [ustalone — wymaga jawnego opisu w README, patrz §15].
- `GET /metrics` — metryki w formacie Prometheus (eksporter OpenTelemetry),
  **w tym gauge'e kolejki** (`webhook_queue_depth{status}`,
  `webhook_queue_oldest_pending_age_seconds`, definicje w §12) liczone z
  tego samego cache co backpressure. To jedno miejsce, z którego
  dyżurny i test obciążeniowy czytają stan kolejki, niezależnie od liczby
  workerów [decyzja projektanta, po recenzji].

## 7. Model danych (SQLite)

Jedna tabela [ustalone po przeglądzie schematu z użytkownikiem]:

```sql
CREATE TABLE webhook_events (
    id               INTEGER PRIMARY KEY,         -- alias rowid, wewnętrzny
    idempotency_key  TEXT    NOT NULL UNIQUE,
    source           TEXT    NOT NULL,            -- X-Source 1. zgłoszenia
    event_type       TEXT    NOT NULL,
    payload_json     TEXT,                        -- NULL po retencji
    status           TEXT    NOT NULL CHECK (status IN
                       ('pending','processing','retrying','completed','failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,  -- też znacznik rezerwacji
    run_at           INTEGER NOT NULL,            -- ms; termin podjęcia / wygaśnięcia rezerwacji
    last_error       TEXT,
    result_json      TEXT,
    traceparent      TEXT,                        -- W3C trace context z POST
    created_at       INTEGER NOT NULL,            -- ms
    finished_at      INTEGER                      -- ms; completed/failed
) STRICT;

-- pobieranie pracy, licznik zaległości, wiek najstarszego należnego
CREATE INDEX ix_events_ready ON webhook_events (run_at, status)
    WHERE status IN ('pending', 'retrying');
-- odzyskiwanie wygasłych rezerwacji, licznik processing
CREATE INDEX ix_events_leased ON webhook_events (run_at)
    WHERE status = 'processing';
-- retencja
CREATE INDEX ix_events_finished ON webhook_events (status, finished_at)
    WHERE finished_at IS NOT NULL;
```

Uzasadnienia (do README):

- **`id` obok `idempotency_key`.** W SQLite każda zwykła tabela ma ukryty
  `rowid`; `id INTEGER PRIMARY KEY` jest tylko jego aliasem — nie dodaje
  kolumny ani indeksu. Indeks `UNIQUE` na kluczu istniałby także przy
  `idempotency_key TEXT PRIMARY KEY`. Jedyna alternatywa bez duplikacji
  klucza to `WITHOUT ROWID`, ale dokumentacja SQLite odradza ją dla dużych
  wierszy (payload do 256 KiB), a indeksy częściowe przechowywałyby wtedy
  cały klucz (do 255 B) zamiast 8-bajtowego `rowid`. `id` jest wewnętrzny —
  worker operuje na nim, API wyłącznie na `idempotency_key`.
- **Czasy jako `INTEGER` (Unix epoch, ms).** SQLite nie ma typu daty;
  liczby dają poprawne porównania i prostą arytmetykę (`now + lease`,
  backoff, progi retencji) bez parsowania. Podgląd w CLI:
  `datetime(col/1000, 'unixepoch')`.
- **`status` jako `TEXT` + `CHECK`** — świadomie: czytelność dla operatora w
  `sqlite3` ważniejsza niż kilka bajtów enuma liczbowego.
- **JSON jako `TEXT`** — funkcje JSON w SQLite działają na TEXT; binarny
  JSONB (od 3.45) nie jest potrzebny — payload tylko zapisujemy i
  odczytujemy w całości.
- **`STRICT`** — SQLite domyślnie przyjmie np. tekst w kolumnie liczbowej;
  `STRICT` (≥ 3.37) wymusza deklarowane typy.
- **`run_at` zamiast osobnych terminów** (wzorzec „visibility timeout”, jak
  w SQS): dla `pending`/`retrying` to moment, od którego wolno podjąć; dla
  `processing` — moment wygaśnięcia rezerwacji. Jedna kolumna zamiast
  dwóch.
- **Dwa indeksy częściowe, dokładnie pod zapytania.** SQLite używa indeksu
  częściowego tylko wtedy, gdy warunek zapytania zawiera **dosłownie**
  warunek indeksu („SQLite does not do algebra”). Dlatego
  `ix_events_ready` ma warunek `status IN ('pending','retrying')`
  identyczny z rezerwacją zdarzeń i gauge'ami, a `ix_events_leased` — `status =
  'processing'` identyczny z odzyskiwaniem. Kolumna `status` w
  `ix_events_ready` czyni indeks pokrywającym dla licznika per status.
  Zweryfikowane `EXPLAIN QUERY PLAN` (rezerwacja, odzyskiwanie, gauge'e,
  retencja — bez `SCAN` tabeli). **Wymóg:** test jednostkowy asertujący
  plan zapytania dla każdego z tych zapytań, żeby regresja nie została
  niezauważona do benchmarku nr 2.
- **Bez `lease_owner`** — wystarcza kontrola numeru próby (§8).
- Usunięte jako zbędne: `max_attempts` (wartość globalna z konfiguracji),
  `updated_at` (żadna logika jej nie używa; historię pokazują logi i
  ślady), `payload_purged_at` (wynika z `payload_json IS NULL`), tabela
  heartbeatów workerów (patrz §6 `/ready`).
- Status `retrying` (= „czeka na podjęcie, `attempts > 0`” — także po
  odzyskaniu wygasłej rezerwacji, patrz §8) zostaje — daje operatorowi
  czytelny podział kolejki w metrykach.

Ustawienia połączenia: `journal_mode=WAL`, `busy_timeout=5000` oraz **`synchronous=FULL`** [ustalone — decyzja
zapisywana w README w sekcji tradeoffów]. Uzasadnienie: `202` jest
obietnicą trwałości; w trybie WAL `synchronous=NORMAL` przetrwa awarię
procesu, ale przy zaniku zasilania/awarii systemu może zgubić ostatnie
zatwierdzone transakcje — czyli zdarzenia, którym już odpowiedziano `202`.
`FULL` wykonuje fsync przy każdym commicie. Koszt zmierzony w recenzji:
~0,75 ms/transakcję (~1300 tx/s przy 3 procesach piszących) wobec
potrzebnych ~17 INSERT/s + porównywalnej liczby UPDATE — pomijalny;
potwierdzamy w teście obciążeniowym. Transakcje zapisujące otwierane
`BEGIN IMMEDIATE`, żeby uniknąć zakleszczeń przy podnoszeniu blokady.

**Pułapki modułu `sqlite3`** (dotyczą każdego modułu z bazą) [decyzja
projektanta, po recenzji]:

- `sqlite3.connect(autocommit=False)` (Python ≥ 3.12) wystawia zwykły
  `BEGIN` (DEFERRED). Transakcja „czytaj, potem pisz” kończy się wtedy
  przy konkurencji błędem `SQLITE_BUSY_SNAPSHOT`, którego `busy_timeout`
  nie obsługuje. **Wymóg:** `autocommit=True` + jawne `BEGIN IMMEDIATE` /
  `COMMIT` / `ROLLBACK` w jednym pomocniku transakcji.
- Wiersze z `UPDATE … RETURNING` pobieramy (`fetchall()`) **przed**
  `COMMIT` — inaczej `cannot commit transaction - SQL statements in
  progress`.
- `synchronous` i `busy_timeout` są **per połączenie** — ustawiane przy
  każdym `connect` w API i workerze. `journal_mode=WAL` jest trwały w
  pliku, ale przy równoczesnym starcie kilku procesów na świeżym
  wolumenie przełączenie może się nie udać — sprawdzamy zwrot pragmy i
  ponawiamy.
- Połączenie **per wątek** (`threading.local`): endpointy FastAPI `def`
  działają w puli wątków, a worker wykonuje operacje bazy przez
  `asyncio.to_thread`. Bez współdzielenia połączeń między wątkami (bez
  `check_same_thread=False`).

Migracje: idempotentny skrypt `CREATE … IF NOT EXISTS` wykonywany przy
starcie każdego procesu, w `BEGIN IMMEDIATE` z `busy_timeout` (kilka
procesów startuje równocześnie) [decyzja projektanta]; narzędzie migracji
→ TODO.

## 8. Cykl życia zdarzenia (statusy)

```
 POST (nowy klucz) ──► pending ──rezerwacja──► processing ──sukces──► completed
                                              │ │
                                              │ ├─błąd, próby < max──────────► retrying ──(run_at)──rezerwacja──► processing
                                              │ └─lease wygasła, próby < max─► retrying
                                              │
                                              └─błąd lub lease wygasła, próby = max──► failed
```

| Przejście | Kiedy dokładnie |
|---|---|
| → `pending` | w transakcji `INSERT` nowego zdarzenia, przed odpowiedzią `202`; `run_at = now`, `attempts = 0` |
| `pending`/`retrying` → `processing` | worker atomowo rezerwuje: jeden `UPDATE … WHERE id IN (SELECT id … WHERE status IN ('pending','retrying') AND run_at <= now ORDER BY run_at LIMIT k) RETURNING *`; ustawia `run_at = now + LEASE`, `attempts += 1` |
| `processing` → `completed` | symulacja zakończona sukcesem; `UPDATE … WHERE id = ? AND status = 'processing' AND attempts = ?` (numer próby z rezerwacji) zapisuje wynik, status i `finished_at` w jednej instrukcji |
| `processing` → `retrying` | symulacja rzuciła błąd i `attempts < MAX_ATTEMPTS`; ta sama kontrola numeru próby; `run_at = now + backoff(attempts)`; `last_error` zapisany |
| `processing` → `failed` | błąd przy `attempts = MAX_ATTEMPTS`; ta sama kontrola numeru próby; stan końcowy, `finished_at` ustawione |
| `processing` → `retrying` (odzyskanie) | rezerwacja wygasła (`status = 'processing' AND run_at <= now` — worker padł / restart) i `attempts < MAX_ATTEMPTS`; `run_at = now`, `last_error = "lease expired"`; osobna instrukcja wykonywana przez worker przed rezerwacją (liczy `webhook_lease_recoveries_total`) [ustalone] |
| `processing` → `failed` (wygasła) | rezerwacja wygasła przy `attempts = MAX_ATTEMPTS`; `last_error = "lease expired"`, `finished_at` ustawione |

**Kontrola numeru próby (fencing).** Każda rezerwacja zwiększa
`attempts`, więc numer próby jednoznacznie identyfikuje bieżącą
rezerwację. Worker, którego rezerwacja wygasła, ma nieaktualny numer — jego
`UPDATE` nie trafia w żaden wiersz i wynik jest odrzucany (log + metryka).
Identyfikator workera trafia do logów, spanów i `result_json`, nie do
schematu.

Parametry domyślne (konfigurowalne) [decyzja projektanta]:
`MAX_ATTEMPTS=5`, `LEASE_SECONDS=30` (> maks. czasu symulacji 5 s;
konfiguracja jest walidowana przy starcie: `LEASE_SECONDS >
SIMULATED_MAX_SECONDS`, inaczej fałszywe „lease expired”),
backoff `min(BACKOFF_BASE · 2^(attempts−1), BACKOFF_MAX)` z jitterem ±20%,
`BACKOFF_BASE=2 s`, `BACKOFF_MAX=60 s`. Baza i limit backoffu są
konfigurowalne, żeby testy i demo `fail_always` nie trwały ~50 s.

Duplikat w dowolnym stanie dostaje aktualny status.

## 9. Worker

- Pętla asyncio z pulą `WORKER_CONCURRENCY` równoległych slotów (nie
  wątków — przetwarzanie to `sleep`, więc wystarcza asyncio); domyślnie
  20. Domyślna konfiguracja compose: **3 workery × 20 slotów** = 60
  slotów, co przy 17 zdarzeniach/s × średnio 3,5 s ≈ 60 pokrywa docelowy
  ruch i od razu demonstruje skalowanie poziome [ustalone].
- **Dobiera pracę natychmiast po zwolnieniu dowolnego slotu**
  (`asyncio.wait(..., return_when=FIRST_COMPLETED)` lub semafor + sygnał),
  porcją równą liczbie wolnych slotów. `POLL_INTERVAL` (0,5 s) dotyczy
  tylko sytuacji, gdy ostatnia rezerwacja zwróciła 0 wierszy [decyzja projektanta,
  po recenzji]. Uzasadnienie: dobieranie wyłącznie w rytmie pollingu
  wydłuża cykl slotu o średnio ~0,25 s, co dla 3×20 daje ~950/min — poniżej
  docelowego 1000/min.
- Operacje SQLite wykonywane poza pętlą zdarzeń (`asyncio.to_thread` lub
  dedykowany wątek) [dla wykonawcy].
- Graceful shutdown (SIGTERM): przestaje pobierać, czeka na trwające zadania
  do `SHUTDOWN_GRACE` (10 s); niedokończone odzyskają się po wygaśnięciu
  rezerwacji. W compose `stop_grace_period: 20s` (> `SHUTDOWN_GRACE`), żeby
  Docker nie zabił workera SIGKILL-em w trakcie kończenia.
- Zadanie retencji uruchamiane periodycznie w workerze (co 10 min) — patrz
  §11. Przy wielu workerach każdy może je uruchomić; operacja jest
  idempotentna, więc to bezpieczne.
- Pomocniczy serwer HTTP na `WORKER_METRICS_PORT` (domyślnie 9100) z
  `/metrics` (metryki procesu workera: czasy przetwarzania, wyniki prób) i
  `/health` (używany przez healthcheck Dockera; samo
  `prometheus_client.start_http_server` obsługuje tylko `/metrics`, więc
  potrzebny mały własny serwer) — dostępny w sieci compose, **niepublikowany na hoście**, żeby
  `--scale worker=N` nie kolidował o port [decyzja projektanta, po
  recenzji].
- Skalowanie: `docker compose up --scale worker=N`; poprawność gwarantuje
  atomowa rezerwacja zdarzeń w SQLite.

### Symulacja przetwarzania

`await asyncio.sleep(uniform(2, 5))`, potem zapis wyniku, np.
`{"processed_by": worker_id, "duration_ms": …, "attempt": n}`.

Deterministyczne wyzwalacze błędów w `payload.simulate` [ustalone]:

- `"fail_once"` — próba nr 1 kończy się błędem, kolejna sukcesem
  (pokazuje `processing → retrying → processing → completed`).
- `"fail_always"` — każda próba kończy się błędem; po `MAX_ATTEMPTS`
  zdarzenie przechodzi w `failed`.

Na potrzeby testów: czas symulacji konfigurowalny
(`SIMULATED_MIN_SECONDS`, `SIMULATED_MAX_SECONDS`) [decyzja projektanta].

## 10. Ochrona przed przeciążeniem

- **Rate limiting per `X-Source`** [ustalone]: token bucket w pamięci procesu
  API; domyślnie 50 żądań/s, burst 100 na źródło [decyzja projektanta —
  wartości powyżej oczekiwanego ruchu, żeby test 1000/min z kilku źródeł nie
  był dławiony]; przekroczenie → `429` + `Retry-After`. Ograniczenie: limit
  per instancja API; współdzielony limit (Redis) → TODO. Słownik kubełków
  per `X-Source` ograniczony (TTL/LRU), żeby dowolne wartości nagłówka nie
  powiększały pamięci bez końca.
- **Backpressure na zaległości** [decyzja projektanta]: jeśli liczba wierszy
  `pending`+`retrying` przekracza `MAX_BACKLOG` (domyślnie 100 000) → `503` +
  `Retry-After` (tylko dla nowych zdarzeń — duplikaty dostają `200`, §6). Licznik odświeżany co ~1 s (cache), nie liczony przy każdym
  żądaniu. Próg jest daleko powyżej scenariusza testu „burst”, więc nie
  zakłóca demonstracji kolejkowania.
- `GET` statusu nie podlega limitowi zapisu [dla wykonawcy: może mieć własny,
  łagodniejszy limit].

## 11. Retencja

Dwa progi [ustalone], wartości konfigurowalne [domyślne — ustalone jako
propozycja]:

| Dane | Domyślnie | Co się dzieje |
|---|---|---|
| `payload_json` i `result_json` zdarzeń `completed` | 7 dni od `finished_at` | ustawiane na `NULL` (cel: GDPR — patrz niżej) |
| cały wiersz `completed` (klucz + status) | 30 dni od `finished_at` | wiersz usuwany |
| cały wiersz `failed` (z payloadem — do diagnozy) | 30 dni od `finished_at` | wiersz usuwany |

Zdarzenia niezakończone nigdy nie są usuwane.

**Czyszczenie payloadu służy przede wszystkim GDPR** (minimalizacja
przechowywania danych osobowych — w przykładach e-mail, imię i nazwisko),
nie odzyskiwaniu miejsca. Plik bazy **nie maleje**: SQLite oznacza zwolnione
strony jako wolne i używa ich ponownie dla nowych danych, więc retencja
**ogranicza wzrost** pliku, ale go nie zmniejsza [ustalone — do README].
Zwracanie miejsca systemowi (`PRAGMA auto_vacuum=INCREMENTAL` +
okresowe `incremental_vacuum`) → TODO jako opcja; `VACUUM` blokuje bazę na
cały czas przebudowy, więc nie nadaje się do pracy w tle.

Konsekwencje (opisane w README): okno retencji klucza = okno ochrony przed
duplikatami. Duplikat po wyczyszczeniu payloadu nadal jest rozpoznawany
(klucz zostaje), dostaje status bez wyniku (`payload_purged: true`). Po
usunięciu wiersza ten sam klucz zostanie potraktowany jak nowe zdarzenie.
Payload trzymamy krócej, bo może zawierać dane osobowe (e-mail, imię i
nazwisko w przykładach).

Usuwanie porcjami (np. po 1000 wierszy na transakcję), żeby nie blokować
zapisu na długo — przenośnie przez `WHERE id IN (SELECT id … LIMIT 1000)`
(`DELETE/UPDATE … LIMIT` wymaga opcji kompilacji, której nie ma każdy
build SQLite).

## 12. Obserwowalność

**OpenTelemetry jako główny mechanizm** [ustalone].

- **Traces:**
  - `opentelemetry-instrumentation-fastapi` — span dla każdego żądania,
    z wyłączeniem `/health`, `/ready` i `/metrics`
    (`OTEL_PYTHON_FASTAPI_EXCLUDED_URLS`) — inaczej healthchecki i
    próbkowanie `/metrics` co 1 s zalewają ślady.
  - Przy zapisie nowego zdarzenia bieżący kontekst zapisywany w kolumnie
    `traceparent` (format W3C) [ustalone — obowiązkowe].
  - Worker przy każdej próbie odtwarza kontekst z `traceparent` i tworzy
    **jeden span `webhook.process` na próbę**, jako bezpośrednie dziecko
    (już zakończonego) spana żądania — próby dzieli czas i proces, więc nie
    ma długo żyjącego spana-rodzica. Atrybuty: `attempt`,
    `idempotency_key`, `source`, `event_type`, wynik/błąd. Efekt: jeden ślad
    od `POST` przez wszystkie próby.
  - Atrybuty spanów nie zawierają payloadu (dane osobowe).
- **Metryki** (OTel SDK, eksport Prometheus na `/metrics`):
  - `webhooks_received_total{source, outcome=accepted|duplicate|rate_limited|backpressure|invalid}`
  - `webhook_processing_duration_seconds` (histogram), `{outcome}`
  - `webhook_attempts_total{outcome=success|retry|failed}`
  - `webhook_queue_depth{status}` — gauge tylko dla `pending`, `retrying`,
    `processing` (liczenie stanów końcowych skanowałoby całą tabelę);
    zapytania zgodne z indeksami z §7.
  - `webhook_queue_oldest_pending_age_seconds` — `now − MIN(run_at)` dla
    pracy **należnej i niepodjętej**: wiersze `pending`/`retrying` z
    `run_at <= now` oraz `processing` z wygasłą rezerwacją (`run_at <=
    now`); 0, gdy takich nie ma. Z działającymi workerami ≈
    `POLL_INTERVAL`; bez workerów rośnie liniowo — najważniejszy sygnał
    „nikt nie przetwarza / kolejka nie nadąża”. Liczenie od `run_at`, a
    nie `created_at`, żeby backoff nie dawał fałszywych alarmów.
  - Callbacki gauge'y (`ObservableGauge`) czytają wyłącznie cache
    odświeżany co ~1 s — nie wykonują zapytań w wątku scrape'u.
  - `webhook_lease_recoveries_total`
  - Uwaga implementacyjna: eksporter Prometheus z OTel sam dokleja `_total`
    do liczników i jednostkę do histogramów — instrumenty nazywamy bez
    sufiksów (`webhooks_received`, `webhook_processing_duration` z jednostką
    `s`), żeby na `/metrics` wyszły nazwy jak wyżej. Pakiety
    instrumentacji (`0.xxbN`) przypięte do pasującej wersji SDK.
  - Gauge'e kolejki na `/metrics` API (patrz §6), metryki przetwarzania na
    `/metrics` workera.
- **Logi:** strukturalny JSON na **stdout** (spany z eksportera
  konsolowego idą na **stderr**, żeby nie mieszać strumieni), w każdym wpisie `trace_id`,
  `span_id`, `idempotency_key`, `source`; zdarzenia: zapis, duplikat,
  rezerwacja, wynik próby, odzyskanie rezerwacji, retencja.
- **Eksport:** domyślnie konsola (span exporter na stderr) — działa bez
  infrastruktury [ustalone]. Ustawienie `OTEL_EXPORTER_OTLP_ENDPOINT`
  przełącza na OTLP — wymaga pakietu
  `opentelemetry-exporter-otlp-proto-http` i kodu wybierającego exporter
  na podstawie zmiennej. **`docker compose` zawiera Jaeger all-in-one** (UI
  `localhost:16686`) jako stały element — wymagany przez test
  obciążeniowy (§13) [ustalone; wcześniejszy warunek „o ile tanie”
  zniesiony po recenzji — to ~10 linii compose]. Wariant awaryjny pomiaru
  bez Jaegera: `OTEL_TRACES_EXPORTER=none`.
- **Ile workerów działa:**
  - Bazowo: `docker compose ps` — liczba kontenerów workera w stanie
    `healthy` (healthcheck na ich `/health`); pośrednio pod obciążeniem
    `webhook_queue_depth{status="processing"}` ≈ liczba workerów × 20.
    Ograniczenie: liczby workerów nie widać w żadnej metryce, a metryki
    workerów (port 9100) nie są zbierane centralnie.
  - **Rozszerzenie opcjonalne — OpenTelemetry Collector w compose**
    [ustalone: rozszerzenie albo TODO, **decyzja w trakcie
    implementacji**]: API i workery wysyłają metryki (i ślady) przez OTLP do
    Collectora, który wystawia jeden zbiorczy endpoint Prometheus i
    przekazuje ślady do Jaegera; każdy worker raportuje gauge
    `webhook_worker_up` z `service.instance.id`, więc liczba workerów =
    liczba serii. Realizowane jako ostatnie, opcjonalne zadanie planu —
    jeśli zostanie pominięte, trafia do TODO.
  - README opisuje, jak sprawdzić liczbę workerów w wariancie, który
    zostanie zrealizowany.

## 13. Test obciążeniowy

Cele [ustalone]:

1. Przyjmowanie wytrzymuje **1000 webhooków/min** z niskim opóźnieniem
   odpowiedzi.
2. Powyżej przepustowości przetwarzania zdarzenia **kolejkują się**, a po
   spadku ruchu kolejka **spokojnie schodzi** do zera; nic nie ginie.

Narzędzie [decyzja projektanta]: własny skrypt asyncio + `httpx` z modelem
otwartym (stałe tempo napływu, niezależne od czasu odpowiedzi), 5 wartości
`X-Source` (przy 3000/min ≈ 10 żądań/s na źródło — poniżej limitu 50/s,
więc rate limiting nie fałszuje wyniku). Głębokość kolejki próbkowana co
1 s z `/metrics` API. Wszystkie tempa, czasy i progi to flagi skryptu.

Środowisko: `docker compose` (api + **3 workery × 20 slotów** + Jaeger) w
devcontainerze z docker-in-docker [ustalone]; podczas pomiaru ślady
eksportowane do Jaegera (OTLP), nie do konsoli, bo wypisywanie spanów na
konsolę zawyża p99 [ustalone].

Oczekiwana przepustowość przetwarzania: 1 worker × 20 slotów / 3,5 s ≈
5,7/s ≈ 340/min; 3 workery ≈ 17/s ≈ 1030/min.

**Scenariusz `steady`** [ustalone]: 1000/min przez 3 min, potem faza
schodzenia kolejki do 0 (limit 2 min) przed weryfikacją kompletności.
Kryteria: p99 opóźnienia `POST` < 100 ms, zero odpowiedzi innych niż 2xx.
Uwaga: przy 3×20 przetwarzanie ma tylko ~3% zapasu nad 1000/min (przy
dobieraniu pracy natychmiast po zwolnieniu slotu, §9), więc kolejka może
się lekko wahać — kryteria dotyczą przyjmowania.

**Scenariusz `burst`** [ustalone]:

| Faza | Ruch | Koniec fazy | Oczekiwane | Limit (porażka po przekroczeniu) |
|---|---|---|---|---|
| 1 | 3000/min (6000 żądań) | po 2 min | kolejka rośnie do ~3900; p99 `POST` < 100 ms, zero nie-2xx | stałe 2 min |
| 2 | 100/min | kolejka (`pending`+`retrying`) ≤ ~100 | ~4 min | 10 min |
| 3 | 0 | kolejka = 0 (łącznie z `processing`) | kilka sekund | 2 min |
| całość | | | ~6 min | 15 min (twardy) |

**Scenariusz `scaling`** [ustalone] — dowód skalowalności poziomej (sam
przebieg na 3×20 pokazuje tylko, że ta konfiguracja wystarcza):

- dla N = 1, 2, 3 workerów (po 20 slotów), każdy przebieg na czystej bazie;
- orkiestracja: **skrypt po stronie hosta** (`scripts/scaling.sh`), który
  dla każdego N robi `docker compose down -v`, `up --scale worker=N` i
  uruchamia kontener generatora (profil `loadtest`); sam kontener nie ma
  dostępu do demona Dockera (celowo) [decyzja projektanta, po recenzji];
- zalanie kolejki nadmiarem pracy (3000 zdarzeń wysłanych tempem 200/s z 5
  źródeł — 40/s na źródło, poniżej limitu 50/s; generator ponawia `429`,
  żeby żadne zdarzenie nie przepadło), następnie pomiar przez 2 min tempa kończenia zdarzeń
  (spadek `pending`+`retrying`+`processing` na minutę), z pominięciem
  krótkiej rozgrzewki;
- oczekiwane ≈ 340 / 690 / 1030 zdarzeń/min;
- kryterium: przepustowość dla N ≥ 85% wartości liniowej
  (N × wynik dla 1 workera). Wynik poniżej oznacza, że wąskim gardłem
  staje się SQLite (rywalizacja o zapis) — to także wartościowy wynik do
  README, z analizą.

Na końcu `steady` i `burst`: weryfikacja, że **wszystkie wysłane klucze
mają status `completed`**.

Raport: p50/p95/p99 i maks. opóźnienia `POST` per faza, rozkład kodów
odpowiedzi, przebieg głębokości kolejki w czasie, czasy trwania faz,
tabela przepustowości dla N = 1, 2, 3, wynik kryteriów (PASS/FAIL). Wyniki
(z opisem sprzętu) trafiają do README.

## 13a. Benchmarki

Uzupełnienie testu obciążeniowego [ustalone — wszystkie cztery]: test
obciążeniowy mierzy cały system, benchmarki — koszt pojedynczych operacji
w izolacji, żeby decyzje projektowe opierały się na liczbach.

| # | Benchmark | Co mierzy | Uzasadnia |
|---|---|---|---|
| 1 | `synchronous=FULL` vs `NORMAL` | czas transakcji zapisu zdarzenia (INSERT) w obu trybach | decyzję o `FULL` (§7) |
| 2 | rezerwacja przy rosnącej kolejce | czas pobrania porcji pracy przy 0 / 10 000 / 100 000 oczekujących | indeksy częściowe `ix_events_ready` / `ix_events_leased`; rezerwacja nie zwalnia przy burst i blisko `MAX_BACKLOG` |
| 3 | rywalizacja o zapis | przepustowość transakcji przy 1 / 2 / 4 / 8 procesach piszących | górną granicę skalowania workerów na SQLite; interpretację wyniku `scaling` (§13) |
| 4 | retencja | czas usunięcia / wyczyszczenia porcji 1000 wierszy | rozmiar porcji retencji i czas blokady zapisu (§11) |

Forma: skrypt(y) w `benchmarks/`, uruchamiane ręcznie, na pliku bazy na
dysku (nie `:memory:`), z wieloma powtórzeniami i raportem mediany i p95.
Wyniki (z opisem sprzętu) w tabeli w README. **Poza CI** — wyniki na
współdzielonych maszynach GitHuba są zbyt zaszumione. Kod benchmarków
podlega linterowi (§15a) [ustalone].

## 14. Testy

**Testy są twardym wymaganiem** [ustalone]:

- **Testy jednostkowe obowiązkowe** dla każdego modułu z logiką
  (repozytorium, przejścia statusów, backoff, rate limiter, retencja,
  worker, API), pisane **przed kodem** (TDD).
- **Deterministyczne:** wstrzykiwany zegar, brak prawdziwych `sleep`
  dłuższych niż ułamki sekundy, osobna baza dla każdego testu.
- Podział: `tests/unit` (szybkie, izolowane) i `tests/integration` (API +
  worker + prawdziwy SQLite, scenariusz restartu).
- **Zielone testy są warunkiem ukończenia każdego zadania** w planie i
  blokują CI.
- **Próg pokrycia 85%** (`pytest-cov`, `fail_under = 85`) egzekwowany w CI,
  liczony **tylko dla pakietu aplikacji** (`--cov=<pakiet>`), bez
  `benchmarks/` i skryptu obciążeniowego.

Narzędzie: pytest. Kluczowe obszary:

- repozytorium: zapis nowego / duplikat (także z inną treścią); atomowa rezerwacja (dwóch
  konkurentów nie dostaje tego samego wiersza); warunkowy zapis wyniku po
  wygaśnięciu rezerwacji; odzyskiwanie rezerwacji; przejścia statusów i
  wyczerpanie prób;
- backoff i rate limiter (z wstrzykiwanym zegarem);
- API przez `TestClient`/`httpx`: kody odpowiedzi z tabeli w §6;
- worker: `fail_once`, `fail_always`, z krótkim czasem symulacji;
- retencja: czyszczenie payloadu i usuwanie wierszy po progach;
- propagacja `traceparent` (in-memory span exporter);
- plany zapytań: `EXPLAIN QUERY PLAN` dla rezerwacji, odzyskiwania, gauge'y i
  retencji — bez `SCAN` tabeli (§7);
- dane testowe: **pięć przykładowych webhooków z treści zadania**
  (`evt-001`…`evt-005`) użytych dosłownie;
- duplikat przy przekroczonym progu zaległości dostaje `200`, nowe
  zdarzenie `503` (§6);
- `GET` kluczy z `/`, `%`, `?`, `#` i znakami UTF-8 (§6);
- test „restartu”: zdarzenie w `processing` z wygasłą rezerwacją zostaje
  dokończone przez nowy worker.

## 15. Dostarczane artefakty

- Kod (pakiet Pythona, `pyproject.toml`, konfiguracja ruff — §15a).
- `.github/workflows/ci.yml` — lint, testy, build obrazu (§15a).
- `benchmarks/` — cztery benchmarki z §13a.
- `Dockerfile` (jeden obraz, dwie komendy: `api` i `worker`):
  `CMD`/`ENTRYPOINT` w formie exec (skrypt wejściowy kończy się `exec`),
  żeby proces dostawał SIGTERM; użytkownik inny niż root; katalog bazy
  tworzony w obrazie z właścicielem tego użytkownika (Docker kopiuje
  właściciela do pustego nazwanego wolumenu przy pierwszym montowaniu).
- `docker-compose.yml`: `api`, `worker` (`deploy.replicas: 3`, nadpisywane
  przez `--scale`), `jaeger`; nazwany wolumen z plikiem bazy; healthcheck
  API na `/ready`; healthcheck workera na jego `/health` (port 9100 w sieci
  compose); healthchecki przez `python -c "urllib.request…"` (obraz
  `python:3.14-slim` nie ma `curl` ani `wget`); `stop_grace_period: 20s`
  dla workera.
- `scripts/scaling.sh` — orkiestracja scenariusza `scaling` (§13).
- Zmiana devcontainera: obraz `mcr.microsoft.com/devcontainers/python:3.14-trixie`
  i feature `docker-in-docker` (jedna przebudowa).
- **Przenośność Dockera** [ustalone]: `Dockerfile` i `docker-compose.yml`
  nie mogą zależeć od devcontainera — mają działać na dowolnym hoście z
  Docker Engine i Compose v2 (Linux, macOS z Docker Desktop/OrbStack,
  Windows z WSL2). W praktyce: oficjalny wieloarchitekturowy obraz bazowy
  (amd64 i arm64), brak ścieżek i bind mountów specyficznych dla
  devcontainera, baza na nazwanym wolumenie, porty konfigurowalne
  zmiennymi. Test obciążeniowy i benchmarki uruchamialne także jako
  kontener (profil compose `loadtest`), żeby nie wymagały Pythona na
  hoście. README zawiera instrukcję dla „gołego” hosta z Dockerem;
  `docker build` w CI weryfikuje budowanie poza devcontainerem.
- README: architektura, gwarancje (§4), tradeoffy, uruchomienie (lokalnie i
  w Dockerze), przykładowe wywołania `curl` na **pięciu przykładowych
  webhookach z treści zadania** (`evt-001`…`evt-005`, dosłownie;
  `payload.simulate` jako dodatek), przykładowe zapytania `sqlite3` dla
  operatora (lista `failed`, zaległe), wyniki testu obciążeniowego i benchmarków,
  założenia skalowalności, sekcja **TODO**. Sekcja tradeoffów musi jawnie
  opisywać co najmniej [ustalone]:
  - **Readiness API niezależne od workera** — co oznacza dla operatora
    (`/ready` zielone przy niedziałającym workerze), jak wtedy wykryć
    problem (healthcheck workera widoczny w `docker compose ps`;
    rosnący `webhook_queue_oldest_pending_age_seconds`), świadome
    ograniczenie (przy pustej kolejce API nie odróżnia bezczynnego workera
    od martwego — brak tabeli heartbeatów) i dlaczego tak wybrano.
  - **`synchronous=FULL`** — dlaczego nie `NORMAL`, z pomiarem kosztu.
  - **Schemat** — uzasadnienia z §7: `id` obok klucza, czasy INTEGER ms,
    `STRICT`, scalenie terminów w `run_at`, dwa indeksy częściowe pod
    zapytania, kontrola numeru próby.
  - **SQLite współdzielony przez kilka kontenerów: ograniczenia**
    [ustalone].

    Dlaczego to działa: kontenery na jednym hoście współdzielą jądro
    Linuksa. SQLite synchronizuje procesy blokadami plików (POSIX `fcntl`),
    a w trybie WAL także plikiem pamięci współdzielonej `-shm` mapowanym
    przez `mmap`. Dla jądra nie ma różnicy, czy procesy są w jednym
    kontenerze, czy w kilku.

    Ograniczenia i warunki poprawności:

    - **Tylko jeden host.** Workerów nie da się rozłożyć na kilka maszyn.
      Baza na NFS lub innym sieciowym systemie plików jest niedozwolona,
      bo blokady i `mmap` są tam zawodne i grożą uszkodzeniem bazy. Dalsze
      skalowanie to PostgreSQL (`SELECT … FOR UPDATE SKIP LOCKED`) albo
      broker z outboxem (→ TODO).
    - **Nazwany wolumen Dockera, nie bind mount.** Na macOS (OrbStack,
      Docker Desktop) bind mount przechodzi przez warstwę współdzielenia
      plików z hostem, gdzie semantyka blokad nie jest gwarantowana.
      Nazwany wolumen leży na lokalnym systemie plików linuksowej maszyny
      wirtualnej.
    - **Prawo zapisu do katalogu bazy we wszystkich kontenerach**, nie
      tylko do pliku, bo SQLite tworzy obok pliki `-wal` i `-shm`.
      Zapewnia to jeden obraz i ten sam UID dla api i workerów.
    - **Jeden pisarz naraz.** SQLite serializuje transakcje zapisujące.
      Przy większej liczbie workerów rośnie czas oczekiwania na blokadę i
      przepustowość przestaje rosnąć liniowo. Przy ~17 zdarzeniach/s zapas
      jest duży, a sufit wyznacza benchmark nr 3 (rywalizacja przy
      1/2/4/8 procesach).
    - **`busy_timeout=5000`** łagodzi chwilową rywalizację, bo proces czeka
      zamiast od razu dostać `database is locked`. Przy trwałym
      przeciążeniu zapisu pojawią się jednak błędy i opóźnienia, a nie
      cicha utrata danych.
    - **Poprawność logiczna nie zależy od liczby kontenerów:** atomowe
      pobieranie pracy (`UPDATE … RETURNING` w `BEGIN IMMEDIATE`) i
      warunkowy zapis wyniku (kontrola numeru próby `attempts`).

    Stan weryfikacji: recenzent zmierzył 3 procesy piszące w jednym
    kontenerze (WAL, `synchronous=FULL`, 600/600 wierszy, ~1300 tx/s).
    Wariant z osobnymi kontenerami na wspólnym wolumenie potwierdzają
    scenariusz `scaling` (§13) i benchmark nr 3 (§13a). README podaje ich
    wyniki jako dowód.
  - Semantykę „dokładnie raz” z restartem API (§4), dwuprogową retencję
    (§11), jeden proces uvicorn (§3).
- Ten dokument i zapis brainstormingu w `docs/superpowers/specs/`.

## 15a. Jakość kodu i CI

**PEP 8 jako twarde wymaganie, egzekwowane linterem** [ustalone]:

- Narzędzie: **ruff** (lint + formatowanie; już w devcontainerze).
- Reguły w `pyproject.toml`: `E`, `W` (pycodestyle — PEP 8), `N`
  (pep8-naming), `F` (pyflakes), `I` (kolejność importów), `B`
  (bugbear), `UP` (składnia Pythona 3.14).
- **Długość linii 79** — dosłownie wg PEP 8 (zamiast domyślnych 88 w ruff).
- `ruff check` i `ruff format --check` muszą przechodzić bez błędów dla
  całego kodu: aplikacji, testów, skryptu obciążeniowego i benchmarków. `# noqa` tylko z
  komentarzem uzasadniającym.
- Zielony linter jest warunkiem ukończenia **każdego** zadania w planie
  implementacji.

**GitHub Actions** [ustalone]: workflow uruchamiany przy każdym pushu i
pull requeście, z krokami:

1. `ruff check` i `ruff format --check`,
2. `pytest` z pokryciem (testy jednostkowe i integracyjne z krótkim czasem
   symulacji; build pada poniżej 85% pokrycia — §14),
3. `docker build` obrazu — weryfikuje Dockerfile [decyzja projektanta].

Test obciążeniowy nie jest częścią CI (trwa kilkanaście minut i zależy od
sprzętu) — uruchamiany ręcznie, wyniki w README. Status workflow
widoczny jako badge w README [decyzja projektanta].

## 16. Założenia skalowalności (do README)

- 1000/min ≈ 17/s; przy średnio 3,5 s przetwarzania potrzeba ~60
  równoległych slotów → domyślnie 3 workery × 20 slotów; więcej przez
  `--scale worker=N`. Liniowość skalowania weryfikuje scenariusz
  `scaling` (§13).
- Zapis do SQLite w WAL: jeden pisarz naraz; przy ~17 INSERT/s +
  porównywalnej liczbie UPDATE workerów jest duży zapas — weryfikujemy
  pomiarem, łącznie z kosztem `synchronous=FULL`.
- Granica: jeden host. Dalej: PostgreSQL (`SELECT … FOR UPDATE SKIP LOCKED`)
  lub broker z outboxem (→ TODO).

## 16a. Struktura kodu

[ustalone z użytkownikiem po akceptacji, przy przeglądzie planu]: jedna
aplikacja — jeden pakiet `webhook_receiver`, jeden obraz, dwa punkty
wejścia (`python -m webhook_receiver api|worker`). Podpakiety: `common/`
(kontrakt bazy i infrastruktura: config, clock, models, db, repository,
telemetry, logging), `api/` (tylko proces api: `app.py`, schemas,
body_limit, ratelimit, queue_stats), `worker/` (tylko proces worker:
`app.py`, http, backoff, simulation). Główne pliki obu aplikacji nazywają
się tak samo: `api/app.py` (`create_app`) i `worker/app.py`
(`create_worker`). Reguła importów: `api` i `worker` importują tylko z
`common`, nigdy z siebie nawzajem (pilnuje test architektury); tryb
`EMBEDDED_WORKER` składa `__main__.py`. Szczegóły w planie implementacji.

## 17. Pozostawione wykonawcy

- Biblioteka do konfiguracji (np. `pydantic-settings`) i do logów JSON.
- Sposób wykonywania operacji SQLite poza pętlą zdarzeń.
- Szczegóły skryptu testu obciążeniowego i formatu raportu.
- Treść sekcji TODO w README (minimum: HMAC, współdzielony rate limit,
  pełny stos obserwowalności, PostgreSQL/broker, narzędzie migracji,
  endpoint do ręcznego ponowienia `failed`, endpoint listujący zdarzenia
  `failed`/zaległe dla operatora, dead-letter/alerty,
  wykrywanie niezgodności treści przy ponowieniu z tym samym kluczem,
  `auto_vacuum=INCREMENTAL` jako opcja odzyskiwania miejsca — §11).
  Obowiązkowo także punkt [ustalone]: **doprecyzować z nadawcami format
  `idempotency_key`** (zestaw znaków, długość, struktura) i na tej podstawie
  dobrać najlepsze rozwiązanie — obecne (dowolny string + kodowanie
  procentowe w ścieżce), ograniczony zestaw znaków albo klucz poza ścieżką
  (parametr zapytania / wewnętrzny identyfikator).
