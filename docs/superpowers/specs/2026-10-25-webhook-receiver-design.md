# Webhook Receiver Service — dokument projektowy

- **Data:** 2026-10-25
- **Status:** do akceptacji
- **Zadanie źródłowe:** <https://gist.github.com/kaluzaaa/fcd035829013d9952f71a9a6a59a4d2f#file-gist_webhook_receiver-md>
- **Przebieg ustaleń:** [2026-10-25-webhook-receiver-brainstorming.md](2026-10-25-webhook-receiver-brainstorming.md)

Oznaczenia: **[ustalone]** — decyzja podjęta wspólnie z autorem zadania;
**[decyzja projektanta]** — wybór projektanta, do weryfikacji przy akceptacji;
**[dla wykonawcy]** — szczegół pozostawiony implementującemu.

## 1. Cel i uzasadnienie

Zadanie rekrutacyjne: prototyp serwisu HTTP w Pythonie, który przyjmuje
webhooki (JSON z `idempotency_key`), gwarantuje ich przetworzenie i udostępnia
status każdego zdarzenia. Oceniający patrzą na **sposób myślenia**:
niezawodność, idempotencję, rozdzielenie przyjęcia od przetwarzania, model
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
2. Duplikat (ten sam klucz, ten sam payload) nie jest przetwarzany ponownie;
   dostaje aktualny status.
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
- Przyjmowanie, które zwalnia lub pada, bo przetwarzanie nie nadąża.
- Obietnica „exactly once” w README bez wyjaśnienia, co naprawdę jest
  gwarantowane.

## 3. Ograniczenia

- Python 3.12 (devcontainer), FastAPI [ustalone], SQLite [wymóg zadania]
  (wersja biblioteki 3.40 — dostępne `RETURNING`, WAL).
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

- **Przyjęcie dokładnie raz:** `UNIQUE(idempotency_key)` w SQLite;
  zapis i decyzja „nowe/duplikat” w jednej transakcji.
- **Wykonanie co najmniej raz:** jeśli worker padnie w trakcie, rezerwacja
  (lease) wygasa i zdarzenie jest podejmowane ponownie.
- **Efekt dokładnie raz:** zapis wyniku jest warunkowy — tylko ze stanu
  `processing` i tylko dla aktualnego numeru próby (`attempts` działa jak
  token ogrodzenia, patrz §8), więc spóźniony worker z wygasłą rezerwacją
  nie nadpisze wyniku. Prawdziwy efekt uboczny musiałby
  być idempotentny względem klucza — to opisujemy jako założenie.

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
przyjęcie.

Dodatkowo [decyzja projektanta]: przełącznik konfiguracyjny
`EMBEDDED_WORKER=true` uruchamiający worker w procesie API — wyłącznie dla
wygody lokalnego developmentu i testów; tryb domyślny i docker compose to
osobne procesy.

### Architektura

```
 nadawcy ──HTTP──► [ api (FastAPI, uvicorn) ] ──INSERT (tx)──► [ SQLite (WAL) ]
                     │ rate limit per X-Source                   ▲   │
                     │ walidacja, idempotencja                    │   │ claim (UPDATE…RETURNING)
                     │ /metrics /health /ready                    │   ▼
                                                     [ worker(s) (asyncio, N slotów) ]
                                                       │ symulacja, retry, lease,
                                                       │ retencja
                                                       │ /metrics /health (port pomocniczy)
           OpenTelemetry: traces + metrics ──► konsola (domyślnie) / OTLP ──► Jaeger
```

## 6. API HTTP

Wszystkie endpointy webhooków wymagają nagłówka `X-Source` (identyfikator
systemu nadawcy) [ustalone]. Brak → `400`.

### `POST /webhooks`

Ciało:

```json
{
  "idempotency_key": "evt-001",
  "event_type": "order.created",
  "payload": { "...": "..." }
}
```

Walidacja: `idempotency_key` — dowolny niepusty string, maks. 255 znaków,
bez ograniczenia zestawu znaków [ustalone]. W ścieżce `GET` klucz jest
kodowany procentowo (RFC 3986, np. `evt/1` → `evt%2F1`); serwer dekoduje
całą ścieżkę po `/webhooks/` (łącznie z `/`), a `Location` i `status_url`
zwracane są już zakodowane, więc klient może ich użyć wprost.
`X-Source` — niepusty string, maks. 64 znaki;
`event_type` — niepusty string; `payload` — obiekt JSON. Limit rozmiaru
ciała: 256 KiB → `413` [decyzja projektanta]. Błędy walidacji → `422`.

Odpowiedzi:

| Sytuacja | Kod | Ciało |
|---|---|---|
| nowe zdarzenie, zapisane trwale | `202 Accepted` | `{idempotency_key, status: "pending", status_url}` + nagłówek `Location` |
| duplikat (ten sam `idempotency_key`) | `200 OK` | aktualny status (jak w `GET`), pole `duplicate: true` |
| brak `X-Source` | `400` | opis błędu |
| przekroczony limit nadawcy | `429` + `Retry-After` [ustalone] | opis |
| przekroczony próg zaległości (backpressure) | `503` + `Retry-After` [decyzja projektanta] | opis |

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
  "attempts": 2,
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
  `webhook_queue_oldest_pending_age_seconds`) liczone z tego samego cache
  co backpressure. To jedno miejsce, z którego
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
    attempts         INTEGER NOT NULL DEFAULT 0,  -- też token ogrodzenia
    run_at           INTEGER NOT NULL,            -- ms; termin podjęcia / wygaśnięcia rezerwacji
    last_error       TEXT,
    result_json      TEXT,
    traceparent      TEXT,                        -- W3C trace context z POST
    created_at       INTEGER NOT NULL,            -- ms
    finished_at      INTEGER                      -- ms; completed/failed
) STRICT;

-- pobieranie pracy i odzyskiwanie wygasłych rezerwacji — jeden indeks
CREATE INDEX ix_events_due ON webhook_events (run_at)
    WHERE status IN ('pending', 'retrying', 'processing');
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
- **JSON jako `TEXT`** — funkcje JSON w SQLite 3.40 działają na TEXT.
- **`STRICT`** — SQLite domyślnie przyjmie np. tekst w kolumnie liczbowej;
  `STRICT` (≥ 3.37) wymusza deklarowane typy.
- **`run_at` zamiast osobnych terminów** (wzorzec „visibility timeout”, jak
  w SQS): dla `pending`/`retrying` to moment, od którego wolno podjąć; dla
  `processing` — moment wygaśnięcia rezerwacji. Jedna kolumna i jeden
  indeks obsługują i pobieranie pracy, i odzyskiwanie.
- **Bez `lease_owner`** — ogrodzenie zapewnia numer próby (§8).
- Usunięte jako zbędne: `max_attempts` (wartość globalna z konfiguracji),
  `updated_at` (żadna logika jej nie używa; historię pokazują logi i
  ślady), `payload_purged_at` (wynika z `payload_json IS NULL`), tabela
  heartbeatów workerów (patrz §6 `/ready`).
- Status `retrying` (formalnie „`pending` z `attempts > 0`”) zostaje —
  daje operatorowi czytelny podział kolejki w metrykach.

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

Migracje: idempotentny skrypt `CREATE … IF NOT EXISTS` wykonywany przy
starcie każdego procesu [decyzja projektanta]; narzędzie migracji → TODO.

## 8. Cykl życia zdarzenia (statusy)

```
 POST (nowy klucz) ──► pending ──claim──► processing ──sukces──► completed
                          ▲                   │ │
       lease wygasła,     │                   │ └─błąd, próby < max──► retrying ──(run_at)──claim──► processing
       próby < max ───────┘                   │
                                              └─błąd lub lease wygasła, próby = max──► failed
```

| Przejście | Kiedy dokładnie |
|---|---|
| → `pending` | w transakcji `INSERT` nowego zdarzenia, przed odpowiedzią `202`; `run_at = now`, `attempts = 0` |
| `pending`/`retrying` → `processing` | worker atomowo rezerwuje: jeden `UPDATE … WHERE id IN (SELECT id … WHERE status IN ('pending','retrying') AND run_at <= now ORDER BY run_at LIMIT k) RETURNING *`; ustawia `run_at = now + LEASE`, `attempts += 1` |
| `processing` → `completed` | symulacja zakończona sukcesem; `UPDATE … WHERE id = ? AND status = 'processing' AND attempts = ?` (numer próby z claimu) zapisuje wynik, status i `finished_at` w jednej instrukcji |
| `processing` → `retrying` | symulacja rzuciła błąd i `attempts < MAX_ATTEMPTS`; ten sam warunek ogrodzenia; `run_at = now + backoff(attempts)`; `last_error` zapisany |
| `processing` → `failed` | błąd przy `attempts = MAX_ATTEMPTS`; ten sam warunek ogrodzenia; stan końcowy, `finished_at` ustawione |
| `processing` → `pending` | rezerwacja wygasła (`status = 'processing' AND run_at <= now` — worker padł / restart) i `attempts < MAX_ATTEMPTS`; osobna instrukcja wykonywana przez worker przed claimem (liczy `webhook_lease_recoveries_total`) |
| `processing` → `failed` (wygasła) | rezerwacja wygasła przy `attempts = MAX_ATTEMPTS`; `last_error = "lease expired"`, `finished_at` ustawione |

**Ogrodzenie (fencing) przez numer próby.** Każdy claim zwiększa
`attempts`, więc numer próby jednoznacznie identyfikuje bieżącą
rezerwację. Worker, którego rezerwacja wygasła, ma nieaktualny numer — jego
`UPDATE` nie trafia w żaden wiersz i wynik jest odrzucany (log + metryka).
Identyfikator workera trafia do logów, spanów i `result_json`, nie do
schematu.

Parametry domyślne (konfigurowalne) [decyzja projektanta]:
`MAX_ATTEMPTS=5`, `LEASE_SECONDS=30` (> maks. czasu symulacji 5 s),
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
- Pobiera pracę porcjami (tyle, ile wolnych slotów); gdy brak pracy — śpi
  `POLL_INTERVAL` (0,5 s); gdy pracy jest dużo — dobiera od razu.
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
  `/health` (używany przez healthcheck Dockera) — dostępny w sieci compose, **niepublikowany na hoście**, żeby
  `--scale worker=N` nie kolidował o port [decyzja projektanta, po
  recenzji].
- Skalowanie: `docker compose up --scale worker=N`; poprawność gwarantuje
  atomowy claim w SQLite.

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
  per instancja API; współdzielony limit (Redis) → TODO.
- **Backpressure na zaległości** [decyzja projektanta]: jeśli liczba wierszy
  `pending`+`retrying` przekracza `MAX_BACKLOG` (domyślnie 100 000) → `503` +
  `Retry-After`. Licznik odświeżany co ~1 s (cache), nie liczony przy każdym
  żądaniu. Próg jest daleko powyżej scenariusza testu „burst”, więc nie
  zakłóca demonstracji kolejkowania.
- `GET` statusu nie podlega limitowi zapisu [dla wykonawcy: może mieć własny,
  łagodniejszy limit].

## 11. Retencja

Dwa progi [ustalone], wartości konfigurowalne [domyślne — ustalone jako
propozycja]:

| Dane | Domyślnie | Co się dzieje |
|---|---|---|
| `payload_json` i `result_json` zdarzeń `completed` | 7 dni od `finished_at` | ustawiane na `NULL` |
| cały wiersz `completed` (klucz + status) | 30 dni od `finished_at` | wiersz usuwany |
| cały wiersz `failed` (z payloadem — do diagnozy) | 30 dni od `finished_at` | wiersz usuwany |

Zdarzenia niezakończone nigdy nie są usuwane.

Konsekwencje (opisane w README): okno retencji klucza = okno ochrony przed
duplikatami. Duplikat po wyczyszczeniu payloadu nadal jest rozpoznawany
(klucz zostaje), dostaje status bez wyniku (`payload_purged: true`). Po
usunięciu wiersza ten sam klucz zostanie potraktowany jak nowe zdarzenie.
Payload trzymamy krócej, bo może zawierać dane osobowe (e-mail, imię i
nazwisko w przykładach).

Usuwanie porcjami (np. po 1000 wierszy na transakcję), żeby nie blokować
zapisu na długo.

## 12. Obserwowalność

**OpenTelemetry jako główny mechanizm** [ustalone].

- **Traces:**
  - `opentelemetry-instrumentation-fastapi` — span dla każdego żądania.
  - Przy przyjęciu nowego zdarzenia bieżący kontekst zapisywany w kolumnie
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
  - `webhook_queue_depth{status}` (gauge odczytywany z bazy)
  - `webhook_queue_oldest_pending_age_seconds` (gauge — najważniejszy sygnał
    „kolejka nie nadąża”)
  - `webhook_lease_recoveries_total`
  - Uwaga implementacyjna: eksporter Prometheus z OTel sam dokleja `_total`
    do liczników i jednostkę do histogramów — instrumenty nazywamy bez
    sufiksów (`webhooks_received`, `webhook_processing_duration` z jednostką
    `s`), żeby na `/metrics` wyszły nazwy jak wyżej. Pakiety
    instrumentacji (`0.xxbN`) przypięte do pasującej wersji SDK.
  - Gauge'e kolejki na `/metrics` API (patrz §6), metryki przetwarzania na
    `/metrics` workera.
- **Logi:** strukturalny JSON na stdout, w każdym wpisie `trace_id`,
  `span_id`, `idempotency_key`, `source`; zdarzenia: przyjęcie, duplikat,
  claim, wynik próby, odzyskanie rezerwacji, retencja.
- **Eksport:** domyślnie konsola (span exporter do stdout) — działa bez
  infrastruktury [ustalone]. Ustawienie `OTEL_EXPORTER_OTLP_ENDPOINT`
  przełącza na OTLP. `docker compose` zawiera Jaeger all-in-one (UI
  `localhost:16686`), o ile to tanie; jeśli okaże się kosztowne — zostaje
  konsola, a Jaeger trafia do TODO [ustalone].
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
stdout zawyża p99 [ustalone].

Oczekiwana przepustowość przetwarzania: 1 worker × 20 slotów / 3,5 s ≈
5,7/s ≈ 340/min; 3 workery ≈ 17/s ≈ 1030/min.

**Scenariusz `steady`** [ustalone]: 1000/min przez 3 min.
Kryteria: p99 opóźnienia `POST` < 100 ms, zero odpowiedzi innych niż 2xx.
Uwaga: przy 3×20 przetwarzanie ma tylko ~3% zapasu nad 1000/min, więc
kolejka może się lekko wahać — kryteria dotyczą przyjmowania.

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
- zalanie kolejki nadmiarem pracy (3000 zdarzeń wysłanych szybko, bez
  dalszego napływu), następnie pomiar przez 2 min tempa kończenia zdarzeń
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

## 14. Testy

pytest; TDD w trakcie implementacji [ustalone]. Kluczowe obszary:

- repozytorium: zapis nowego / duplikat (także z inną treścią); atomowy claim (dwóch
  konkurentów nie dostaje tego samego wiersza); warunkowy zapis wyniku po
  wygaśnięciu rezerwacji; odzyskiwanie rezerwacji; przejścia statusów i
  wyczerpanie prób;
- backoff i rate limiter (z wstrzykiwanym zegarem);
- API przez `TestClient`/`httpx`: kody odpowiedzi z tabeli w §6;
- worker: `fail_once`, `fail_always`, z krótkim czasem symulacji;
- retencja: czyszczenie payloadu i usuwanie wierszy po progach;
- propagacja `traceparent` (in-memory span exporter);
- test „restartu”: zdarzenie w `processing` z wygasłą rezerwacją zostaje
  dokończone przez nowy worker.

## 15. Dostarczane artefakty

- Kod (pakiet Pythona, `pyproject.toml`, konfiguracja ruff — §15a).
- `.github/workflows/ci.yml` — lint, testy, build obrazu (§15a).
- `Dockerfile` (jeden obraz, dwie komendy: `api` i `worker`).
- `docker-compose.yml`: `api`, `worker`, `jaeger`; nazwany wolumen z plikiem
  bazy; healthcheck API na `/ready`; healthcheck workera na jego `/health`
  (port 9100 w sieci compose); `stop_grace_period: 20s` dla workera.
- Zmiana devcontainera: feature `docker-in-docker`.
- README: architektura, gwarancje (§4), tradeoffy, uruchomienie (lokalnie i
  w Dockerze), przykładowe wywołania `curl`, wyniki testu obciążeniowego,
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
    `STRICT`, scalenie terminów w `run_at`, ogrodzenie numerem próby.
  - Semantykę „dokładnie raz” (§4), dwuprogową retencję (§11), jeden
    proces uvicorn (§3), granicę jednego hosta (§16).
- Ten dokument i zapis brainstormingu w `docs/superpowers/specs/`.

## 15a. Jakość kodu i CI

**PEP 8 jako twarde wymaganie, egzekwowane linterem** [ustalone]:

- Narzędzie: **ruff** (lint + formatowanie; już w devcontainerze).
- Reguły w `pyproject.toml`: `E`, `W` (pycodestyle — PEP 8), `N`
  (pep8-naming), `F` (pyflakes), `I` (kolejność importów), `B`
  (bugbear), `UP` (składnia Pythona 3.12).
- **Długość linii 79** — dosłownie wg PEP 8 (zamiast domyślnych 88 w ruff).
- `ruff check` i `ruff format --check` muszą przechodzić bez błędów dla
  całego kodu: aplikacji, testów i skryptu obciążeniowego. `# noqa` tylko z
  komentarzem uzasadniającym.
- Zielony linter jest warunkiem ukończenia **każdego** zadania w planie
  implementacji.

**GitHub Actions** [ustalone]: workflow uruchamiany przy każdym pushu i
pull requeście, z krokami:

1. `ruff check` i `ruff format --check`,
2. `pytest` (testy jednostkowe i integracyjne z krótkim czasem symulacji),
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

## 17. Pozostawione wykonawcy

- Struktura pakietów i nazwy modułów.
- Biblioteka do konfiguracji (np. `pydantic-settings`) i do logów JSON.
- Sposób wykonywania operacji SQLite poza pętlą zdarzeń.
- Szczegóły skryptu testu obciążeniowego i formatu raportu.
- Treść sekcji TODO w README (minimum: HMAC, współdzielony rate limit,
  pełny stos obserwowalności, PostgreSQL/broker, narzędzie migracji,
  endpoint do ręcznego ponowienia `failed`, dead-letter/alerty,
  wykrywanie niezgodności treści przy ponowieniu z tym samym kluczem).
  Obowiązkowo także punkt [ustalone]: **doprecyzować z nadawcami format
  `idempotency_key`** (zestaw znaków, długość, struktura) i na tej podstawie
  dobrać najlepsze rozwiązanie — obecne (dowolny string + kodowanie
  procentowe w ścieżce), ograniczony zestaw znaków albo klucz poza ścieżką
  (parametr zapytania / wewnętrzny identyfikator).
