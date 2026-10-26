# Operacje i diagnostyka

[← README](../README.md)

Dla operatora i oceniającego:

- demonstracje zachowania (awaria workera, restart API, ponowienia, rate
  limiting),
- szczegóły API,
- obserwowalność,
- zapytania SQLite,
- uruchamianie testów, testu obciążeniowego i benchmarków.

Wszystkie polecenia zakładają stos z [README](../README.md#uruchomienie)
(`docker compose up -d --build --wait`).

## Spis treści

- [Demonstracje](#demonstracje)
- [Pętla restartów przy błędzie konfiguracji](#pętla-restartów-przy-błędzie-konfiguracji)
- [Zatrzymanie (`docker compose stop`)](#zatrzymanie-docker-compose-stop)
- [API — szczegóły](#api--szczegóły)
- [Przejścia statusów](#przejścia-statusów)
- [Obserwowalność](#obserwowalność)
- [SQLite dla operatora](#sqlite-dla-operatora)
- [Testy, test obciążeniowy, benchmarki](#testy-test-obciążeniowy-benchmarki)

## Demonstracje

### Awaria workera i restart API

Awaria workera w trakcie próby i odzyskanie rezerwacji:

```bash
curl -s -X POST localhost:8000/webhooks -H 'Content-Type: application/json' \
  -H 'X-Source: shop' \
  -d '{"idempotency_key": "evt-crash", "event_type": "demo.crash", "payload": {}}'
sleep 1
docker compose kill worker        # SIGKILL wszystkich workerów w trakcie próby
curl -s localhost:8000/webhooks/evt-crash   # "status":"processing","attempts":1
docker compose start worker
sleep 35                          # rezerwacja (30 s) wygasa, zdarzenie odzyskane
curl -s localhost:8000/webhooks/evt-crash   # "status":"completed","attempts":2
docker compose logs worker | grep lease_recovered
```

Restart API nie wpływa na zapisane zdarzenia:

```bash
docker compose restart api && docker compose up -d --wait
curl -s localhost:8000/webhooks/evt-crash   # nadal "completed"
```

### Ponowienia i rate limiting

#### Ponowienia

Deterministyczne wyzwalacze błędów w `payload.simulate` (dodatek do
przykładów z zadania):

```bash
H='Content-Type: application/json'

# fail_once: próba 1 pada, próba 2 kończy się sukcesem
curl -s -X POST localhost:8000/webhooks -H "$H" -H 'X-Source: shop' \
  -d '{"idempotency_key": "evt-fail-once", "event_type": "demo",
       "payload": {"simulate": "fail_once"}}'
for i in $(seq 1 15); do
  curl -s localhost:8000/webhooks/evt-fail-once \
    | grep -o '"status":"[a-z]*","attempts":[0-9]*'
  sleep 1
done
# pending/0 → processing/1 → retrying/1 (last_error "simulated failure
# (fail_once)", odstęp ok. 2 s) → processing/2 → completed/2

# fail_always: każda próba pada; po MAX_ATTEMPTS=5 próbach status failed
curl -s -X POST localhost:8000/webhooks -H "$H" -H 'X-Source: shop' \
  -d '{"idempotency_key": "evt-fail-always", "event_type": "demo",
       "payload": {"simulate": "fail_always"}}'
sleep 65; curl -s localhost:8000/webhooks/evt-fail-always
# "status":"failed","attempts":5,"last_error":"simulated failure (fail_always)"
```

- `fail_always` trwa ok. 35–60 s: 5 prób po 2–5 s plus odstępy ok. 2, 4,
  8 i 16 s.
- Odstęp to `min(BACKOFF_BASE_SECONDS · 2^(attempts−1),
  BACKOFF_MAX_SECONDS)` z losowym rozrzutem ±20%.

#### Rate limiting

150 równoległych żądań z jednego źródła (kubełek: 100 żądań na start,
potem 50/s):

```bash
H='Content-Type: application/json'
seq 1 150 | xargs -P 20 -I@ curl -s -o /dev/null -w '%{http_code}\n' \
  -X POST localhost:8000/webhooks -H "$H" -H 'X-Source: flood' \
  -d '{"idempotency_key": "rl-@", "event_type": "demo", "payload": {}}' \
  | sort | uniq -c
# ok. 100–110 x 202, reszta 429 (z nagłówkiem Retry-After)
```

## Pętla restartów przy błędzie konfiguracji

Wszystkie kontenery mają `restart: unless-stopped`.

Konsekwencja: błąd konfiguracji przy starcie (np. `LEASE_SECONDS` ≤
`SIMULATED_MAX_SECONDS`) daje **pętlę restartów**. Widać ją:

- w `docker compose ps` (status `Restarting`),
- w `docker compose logs <usługa>` (błąd walidacji konfiguracji).

## Zatrzymanie (`docker compose stop`)

- **Workery** po SIGTERM kończą trwające próby (do
  `SHUTDOWN_GRACE_SECONDS`) i kończą się kodem 0.
- **`api`** kończy się kodem 143 — uvicorn po czystym zamknięciu ponownie
  zgłasza SIGTERM; to nie jest błąd.

## API — szczegóły

### `POST /webhooks`

Kody odpowiedzi i kolejność kontroli: [README](../README.md#api).

**Nagłówki:** `Content-Type: application/json`, **`X-Source`** (wymagany,
1–64 znaki).

**Body:**

```json
{"idempotency_key": "evt-001", "event_type": "order.created", "payload": {"...": "..."}}
```

- `idempotency_key` — niepusty napis, maks. 255 znaków, dowolne znaki;
  **globalnie unikalny** (nie w obrębie nadawcy).
- `event_type` — niepusty napis.
- `payload` — obiekt JSON.
- Inne pola są ignorowane.
- Limit body: 256 KiB (`MAX_BODY_BYTES`), sprawdzany na `Content-Length`
  i przy czytaniu strumienia (żądania chunked).

**`Retry-After`:**

- dla obu `503`: `BACKPRESSURE_RETRY_AFTER_SECONDS` (5 s),
- dla `429`: czas do odnowienia jednego żetonu (min. 1 s).

#### Rate limiting per `X-Source`

Token bucket w pamięci procesu API: `RATE_LIMIT_PER_SECOND` = 50 żądań/s,
`RATE_LIMIT_BURST` = 100 na źródło.

**Rola `X-Source`.** Nagłówek jest wymagany właśnie dlatego — limit
liczony jest per nadawca, więc nadawcę trzeba znać.

- Dodatkowo służy jako etykieta w metrykach, logach i śladach.
- Nie jest częścią tożsamości zdarzenia: w wierszu zapisywane jest źródło
  pierwszego zgłoszenia, a ponowienie z innego źródła to zwykły duplikat.

**Granice limitera:**

- **Pamięć limitera jest ograniczona.** Kubełki trzyma LRU do
  `RATE_LIMIT_MAX_SOURCES` (10 000) źródeł, więc dowolne wartości
  nagłówka nie powiększają jej bez końca.
- **Konsekwencja LRU:** przy ponad 10 000 aktywnych źródeł najdawniej
  używany kubełek jest usuwany, a jego źródło wraca z pełnym kubełkiem
  (burst). Przy celowym zalewie losowymi `X-Source` limit per źródło
  przestaje chronić.
- **Metryki nie są ograniczone.** `webhooks_received_total` ma osobną
  serię dla każdej wartości `X-Source`, więc nieuwierzytelniony nadawca
  może dowolnie zwiększać ich liczbę (→ [TODO](../README.md#todo)).
- **Jeden proces:** limit działa w obrębie jednego procesu API (patrz
  [jeden proces uvicorn](tradeoffs.md#jeden-proces-uvicorn)).

### `GET /webhooks/{idempotency_key}`

- Nie wymaga `X-Source`.
- Klucz w ścieżce kodowany procentowo (RFC 3986), np. `evt/1` →
  `/webhooks/evt%2F1` — tak samo jak w `Location` i `status_url`.
- Nieznany klucz → `404`.

Przykład (`curl -s localhost:8000/webhooks/evt-001` po zakończeniu):

```json
{"idempotency_key":"evt-001","source":"shop","event_type":"order.created",
 "status":"completed","attempts":1,"max_attempts":5,"next_attempt_at":null,
 "last_error":null,
 "result":{"processed_by":"700e5b8a1962-7","duration_ms":3760,"attempt":1},
 "created_at":"2026-10-26T19:32:45.711Z","finished_at":"2026-10-26T19:32:49.546Z",
 "payload_purged":false}
```

| Pole | Znaczenie |
|---|---|
| `status` | `pending`, `processing`, `retrying`, `completed`, `failed` ([statusy](#przejścia-statusów)) |
| `attempts` / `max_attempts` | wykonane próby / limit z konfiguracji (`MAX_ATTEMPTS`) |
| `next_attempt_at` | termin kolejnej próby dla `pending`/`retrying`, inaczej `null` |
| `last_error` | błąd ostatniej nieudanej próby (`simulated failure (…)`, `lease expired`) |
| `result` | wynik (`processed_by`, `duration_ms`, `attempt`) albo `null` |
| `created_at`, `finished_at` | ISO 8601 UTC; `finished_at` dla `completed`/`failed` |
| `payload_purged` | `true`, gdy retencja wyczyściła payload i wynik |

### Endpointy operacyjne

| Endpoint | Proces | Znaczenie |
|---|---|---|
| `GET /health` | api | liveness: proces odpowiada |
| `GET /ready` | api | readiness: baza odpowiada i schemat istnieje |
| `GET /metrics` | api | Prometheus, w tym gauge'e kolejki |
| `GET /health` (port 9100) | worker | pętla workera wykonała iterację w ciągu 10 × `POLL_INTERVAL_SECONDS` |
| `GET /metrics` (port 9100) | worker | metryki przetwarzania tego workera |

Uwagi do tabeli:

- `/ready` w razie problemu zwraca `503` z listą `failed`. **Nie zależy od
  workerów** ([dlaczego](tradeoffs.md#readiness-api-niezależne-od-workera)).
- `/health` workera odpytuje healthcheck Dockera.

## Przejścia statusów

| Przejście | Kiedy dokładnie |
|---|---|
| → `pending` | zapis nowego zdarzenia, w tej samej transakcji, przed `202`; ustawia `run_at = now`, `attempts = 0` |
| `pending`/`retrying` → `processing` | worker atomowo rezerwuje porcję (zapytanie niżej); ustawia `run_at = now + LEASE_SECONDS`, `attempts += 1` |
| `processing` → `completed` | sukces; wynik, status i `finished_at` zapisuje jedna instrukcja z kontrolą numeru próby |
| `processing` → `retrying` | błąd przy `attempts < MAX_ATTEMPTS`; ustawia `run_at = now + odstęp` i zapisuje `last_error` |
| `processing` → `failed` | błąd przy `attempts = MAX_ATTEMPTS`; to stan końcowy |
| `processing` → `retrying` / `failed` | rezerwacja wygasła (`run_at <= now`), bo worker padł; ustawia `last_error = "lease expired"` |

Rezerwacja to jedno zapytanie:

```sql
UPDATE … WHERE id IN (SELECT id … WHERE status IN ('pending','retrying')
  AND run_at <= now ORDER BY run_at LIMIT k) RETURNING *
```

Odzyskanie rezerwacji wykonuje każdy worker przed rezerwacją
(`webhook_lease_recoveries_total`).

**`run_at` łączy dwa terminy** (wzorzec „visibility timeout”, jak w SQS):

- dla `pending`/`retrying` to moment, od którego wolno podjąć zdarzenie,
- dla `processing` — moment wygaśnięcia rezerwacji.

## Obserwowalność

### Metryki

#### API

`curl -s localhost:8000/metrics | grep ^webhook`:

| Metryka | Znaczenie |
|---|---|
| `webhooks_received_total{source, outcome}` | `outcome` = `accepted`, `duplicate`, `rate_limited`, `backpressure`, `db_busy`, `invalid` |
| `webhook_queue_depth{status}` | liczba zdarzeń `pending`, `retrying`, `processing` |
| `webhook_queue_oldest_pending_age_seconds` | wiek najstarszej pracy **należnej i niepodjętej** |

Uwagi do tabeli:

- `413` nie jest liczone w `webhooks_received_total`.
- Stanów końcowych w `webhook_queue_depth` nie liczymy — wymagałoby to
  skanu tabeli.
- `webhook_queue_oldest_pending_age_seconds`:
  - obejmuje `pending`/`retrying` z `run_at <= now` oraz `processing` z
    wygasłą rezerwacją,
  - liczony od `run_at`, więc odstęp ponowienia nie daje fałszywych
    alarmów,
  - z działającymi workerami ≈ 0; bez workerów rośnie liniowo —
    **najważniejszy sygnał „nikt nie przetwarza”**.

Gauge'e czytają cache odświeżany co ~1 s (ten sam, którego używa
backpressure), więc odczyt metryk nie wykonuje zapytań do bazy.

#### Worker

Port 9100 w sieci compose; w trybie `EMBEDDED_WORKER` na `/metrics` API.

| Metryka | Znaczenie |
|---|---|
| `webhook_attempts_total{outcome}` | `success`, `retry`, `failed` |
| `webhook_processing_duration_seconds{outcome}` | histogram czasu próby |
| `webhook_lease_recoveries_total` | odzyskane rezerwacje (po awarii workera) |
| `webhook_stale_results_total` | wyniki odrzucone przez kontrolę numeru próby |
| `webhook_worker_up` | 1, dopóki proces workera działa (jedna seria na proces) |

Liczniki pojawiają się na `/metrics` dopiero po pierwszym zdarzeniu danego
rodzaju. Metryki workera odczytuje się per kontener:

```bash
docker compose exec --index 1 worker python -c \
  "import urllib.request; print(urllib.request.urlopen('http://localhost:9100/metrics').read().decode())" \
  | grep ^webhook
```

### Collector (wszystkie metryki naraz)

Compose zawiera OpenTelemetry Collector
(`otel/opentelemetry-collector-contrib:0.163.2`, konfiguracja
`otel-collector.yaml`):

- API i workery wypychają do niego metryki i ślady przez OTLP/HTTP co 5 s
  (`OTEL_METRICS_EXPORTER=otlp`, `OTEL_METRIC_EXPORT_INTERVAL=5000`).
- Ślady przekazuje do Jaegera (OTLP gRPC).
- Metryki wszystkich procesów wystawia na jednym endpoincie Prometheus:
  `localhost:8889/metrics`.

Te same nazwy metryk co wyżej, z dodatkowymi etykietami:

- `job` — `webhook-api` albo `webhook-worker` (`service.name`),
- `instance` — `<hostname>-<pid>` procesu (`service.instance.id`),
- `otel_scope_*` — zakres instrumentacji (stały).

```bash
curl -s localhost:8889/metrics | grep '^webhook_attempts_total'
# webhook_attempts_total{instance="05f377d57b85-7",job="webhook-worker",...,outcome="success"} 42
```

**Wygasanie serii.** Seria, której żaden proces nie odświeżył przez 30 s
(`metric_expiration`), znika z endpointu. Lokalne `/metrics` (API i
worker :9100) działają dalej — test obciążeniowy próbkuje `/metrics` API.

**Ograniczenia:**

- Collector nie przechowuje historii — to tylko punkt zbiorczy do
  odpytywania przez Prometheus (Prometheus/Grafana →
  [TODO](../README.md#todo)).
- Obraz jest distroless (bez powłoki), więc nie ma healthchecka; API
  startuje po `service_started` Collectora.

**Gdy Collector nie działa:**

- eksportery OTLP w tle ponawiają wysyłkę, a potem odrzucają dane —
  żądania na nie nie czekają,
- czeka tylko końcowe wypchnięcie przy zamykaniu, ograniczone przez
  `OTEL_EXPORTER_OTLP_TIMEOUT` (w compose 3 s).

### Ślady (Jaeger)

Compose zawiera Jaeger v2 (`jaegertracing/jaeger:2.22.0`). API i workery
eksportują ślady przez OTLP do Collectora, a ten do Jaegera.

- UI: <http://localhost:16686>, usługi `webhook-api` i `webhook-worker`.
- Jeden ślad zawiera span `POST /webhooks` i jako jego dzieci spany
  `webhook.process` — po jednym na każdą próbę.
- Atrybuty `webhook.process`: `attempt`, `idempotency_key`, `source`,
  `event_type`, `worker.id`, `outcome`; nigdy payload.
- Spany `/health`, `/ready` i `/metrics` są wyłączone.

API Jaegera v2 jest pod `/api/v3/…` (stare `/api/services` daje `404`):

```bash
curl -s localhost:16686/api/v3/services
# m.in. "webhook-api", "webhook-worker" (czasem także "jaeger")
```

**Ślad konkretnego zdarzenia.** Identyfikator śladu to znaki 4–35 kolumny
`traceparent` (format W3C `00-<trace_id>-<span_id>-01`). Link do UI daje
więc zapytanie ([jak je uruchomić](#sqlite-dla-operatora)):

```sql
SELECT 'http://localhost:16686/trace/' || substr(traceparent, 4, 32)
FROM webhook_events WHERE idempotency_key = 'evt-001';
```

### Logi

Strukturalny JSON na stdout (spany eksportera konsolowego, jeśli
włączony, na stderr).

- Każdy wpis ma `trace_id` i `span_id` (do wyszukania w Jaegerze).
- Wpisy o zdarzeniach mają także `event` oraz — zależnie od rodzaju —
  `idempotency_key`, `source`, `worker_id`, `attempt`.
- Wartości `event`: `accepted`, `duplicate`, `rate_limited`,
  `backpressure`, `db_busy`, `invalid`, `claimed`, `attempt`,
  `lease_recovered`, `stale_result`, `retention`.
- Payload i wynik nigdy nie trafiają do logów.
- Log dostępu uvicorna jest wyłączony (żądania opisują wpisy `event` i
  ślady).

```bash
docker compose logs --no-log-prefix api | grep '"idempotency_key": "evt-001"'
docker compose logs --no-log-prefix worker | grep '"event": "lease_recovered"'
```

### Ile workerów działa

API nie śledzi workerów (brak tabeli heartbeatów — świadoma decyzja), więc
liczby workerów nie widać w metrykach API. Źródła:

1. **`webhook_worker_up` w Collectorze** — każdy proces workera raportuje
   1, więc liczba serii = liczba workerów:
   ```bash
   curl -s localhost:8889/metrics | grep -c '^webhook_worker_up'
   # 3; po --scale worker=5: 5
   curl -s localhost:8889/metrics | grep '^webhook_worker_up'
   # webhook_worker_up{instance="05f377d57b85-7",job="webhook-worker",...} 1
   ```
   - Po zatrzymaniu workera (`stop`, `--scale` w dół, `SIGKILL`) jego
     seria znika po ~30 s (`metric_expiration`; zmierzone: 30 s).
   - Nowy kontener (`--scale`, ponowne utworzenie) może przez te ~30 s
     zawyżać licznik o 1. Restart w miejscu (np. po awarii procesu —
     Docker restartuje kontener) zachowuje ten sam `instance`: hostname
     się nie zmienia, a PID pod tini jest ten sam.
   - Nowy worker pojawia się po pierwszym eksporcie (≤ 5 s).
   - W trybie `EMBEDDED_WORKER` seria ma `job="webhook-api"`.
2. **`docker compose ps worker`** — liczba kontenerów workera w stanie
   `(healthy)`; healthcheck odpytuje `/health` workera co 5 s.
3. **`/health` konkretnego workera:**
   ```bash
   docker compose exec --index 2 worker python -c \
     "import urllib.request; print(urllib.request.urlopen('http://localhost:9100/health').read())"
   ```
4. **Pod obciążeniem** `webhook_queue_depth{status="processing"}` ≈
   liczba workerów × 20 (w teście `steady` i `burst`: 60 przy 3
   workerach).
5. **`webhook_queue_oldest_pending_age_seconds` rosnący** mimo „zdrowych”
   kontenerów = workery nie nadążają albo nie przetwarzają.
6. **Rozkład pracy między workery w bazie:** zapytanie po
   `json_extract(result_json, '$.processed_by')` w następnej sekcji.

## SQLite dla operatora

### Jak otworzyć bazę

**W devcontainerze** jest `sqlite3` CLI. Plik wolumenu należy do roota
demona docker-in-docker. Tryb `mode=ro` — tylko odczyt:

```bash
DB="$(docker volume inspect -f '{{ .Mountpoint }}' zadanie-webhook-receiver_webhook-data)/webhooks.db"
sudo sqlite3 -header -box "file:$DB?mode=ro" \
  "SELECT status, COUNT(*) FROM webhook_events GROUP BY status;"
```

**Na dowolnym hoście** (bez `sqlite3` na hoście; na macOS wolumen jest w
maszynie wirtualnej) — moduł `sqlite3` Pythona w kontenerze. Obsługuje
SQL, ale z poleceń z kropką tylko `.help` i `.quit`:

```bash
docker compose exec api python -m sqlite3 /data/webhooks.db \
  "SELECT status, COUNT(*) FROM webhook_events GROUP BY status"
```

### Zapytania

Czasy w bazie to INTEGER, ms od epoki — podgląd przez
`datetime(col/1000, 'unixepoch')`.

```sql
-- zdarzenia failed
SELECT idempotency_key, source, event_type, attempts, last_error,
       datetime(finished_at/1000, 'unixepoch') AS finished
FROM webhook_events WHERE status = 'failed' ORDER BY finished_at DESC;

-- zaległe: należne i niepodjęte (pending/retrying po terminie)
SELECT idempotency_key, status, attempts, last_error,
       datetime(run_at/1000, 'unixepoch') AS due,
       datetime(created_at/1000, 'unixepoch') AS created
FROM webhook_events
WHERE status IN ('pending', 'retrying')
  AND run_at <= CAST(unixepoch('subsec') * 1000 AS INTEGER)
ORDER BY run_at LIMIT 50;

-- wygasłe rezerwacje (worker padł, czeka na odzyskanie)
SELECT idempotency_key, attempts, datetime(run_at/1000, 'unixepoch') AS lease_expired
FROM webhook_events
WHERE status = 'processing'
  AND run_at <= CAST(unixepoch('subsec') * 1000 AS INTEGER);

-- rozkład pracy między workery
SELECT json_extract(result_json, '$.processed_by') AS worker, COUNT(*)
FROM webhook_events WHERE status = 'completed' GROUP BY worker;

-- link do śladu w Jaegerze
SELECT 'http://localhost:16686/trace/' || substr(traceparent, 4, 32)
FROM webhook_events WHERE idempotency_key = 'evt-001';
```

### Spójna kopia bazy

Do analizy poza produkcją — API kopii zapasowej SQLite (bezpieczne przy
działającym systemie):

```bash
sudo sqlite3 "file:$DB?mode=ro" ".backup /tmp/webhooks-copy.db"     # devcontainer
docker compose exec api python -c "import sqlite3; \
  sqlite3.connect('/data/webhooks.db').backup(sqlite3.connect('/tmp/copy.db'))"
docker compose cp api:/tmp/copy.db ./webhooks-copy.db              # dowolny host
```

### Uwaga: długie transakcje

Nie otwieraj na żywej bazie długich transakcji zapisujących (np. ręcznego
`BEGIN; UPDATE …` bez szybkiego `COMMIT`).

SQLite ma jednego pisarza naraz: taka transakcja blokuje zapis zdarzeń.
Po `busy_timeout` (5 s):

- API zaczyna odpowiadać `503`,
- workery logują `worker turn failed`.

## Testy, test obciążeniowy, benchmarki

### Testy i linter

Te same kroki co w CI:

```bash
uv sync --extra dev
uv run ruff check .
uv run ruff format --check .
uv run pytest            # unit + integration, pokrycie pakietu, próg 85%
```

Testy są deterministyczne (wstrzykiwany zegar, osobna baza na test).
Obejmują m.in.:

- pięć webhooków z treści zadania,
- duplikaty i kody z tabeli API,
- atomową rezerwację i kontrolę numeru próby,
- odzyskanie rezerwacji („restart”),
- retencję,
- propagację `traceparent`,
- plany zapytań (`EXPLAIN QUERY PLAN` bez `SCAN` tabeli).

CI (GitHub Actions) uruchamia ruff, pytest z progiem pokrycia 85% i
`docker build` obrazu.

### Test obciążeniowy

Generator asyncio + httpx:

- model otwarty (stałe tempo napływu, niezależne od czasu odpowiedzi),
- 5 wartości `X-Source`,
- uruchamiany jako kontener (profil compose `loadtest`), na świeżym
  stosie.

```bash
docker compose down -v && docker compose up -d --build --wait
docker compose run --rm -T loadtest steady > docs/results/steady.txt  # ~3 min
docker compose down -v && docker compose up -d --build --wait
docker compose run --rm -T loadtest burst > docs/results/burst.txt    # ~6–7 min
bash loadtest/scaling.sh                                              # N = 1, 2, 3; ~10 min
```

Wszystkie tempa, czasy i progi to flagi (`docker compose run --rm
loadtest steady --help`).

**`loadtest/scaling.sh`** działa po stronie hosta (kontener generatora
celowo nie ma dostępu do Dockera). Dla każdego N:

1. `docker compose down -v`,
2. `up --scale worker=N`,
3. zalewa kolejkę,
4. zapisuje `docs/results/scaling-N.json` i
   `docs/results/scaling-N-observations.txt`.

Na końcu zapisuje `docs/results/scaling-report.txt`.

Próbny przebieg bez nadpisywania wyników:

```bash
RESULTS_DIR=/tmp/scaling FLOOD_ARGS="--count 300 --warmup 2 --measure 10" bash loadtest/scaling.sh 1
```

### Benchmarki SQLite

- Uruchamiane ręcznie, poza CI — wyniki na współdzielonych maszynach
  GitHuba są zbyt zaszumione.
- Na nazwanym wolumenie, jak baza produkcyjna.
- Czas: ~10 min.

```bash
docker compose run --rm --no-deps -T -v bench-data:/data --entrypoint python \
  loadtest -W error -m benchmarks.sqlite_bench all --dir /data/bench --repeat 5 \
  > docs/results/benchmarks.txt
docker volume rm bench-data        # wolumen spoza projektu compose
```

`--quick` uruchamia krótki przebieg kontrolny (smoke test; małe rozmiary,
wyniki bez wartości pomiarowej).
