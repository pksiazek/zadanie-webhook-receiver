# Konfiguracja

[← README](../README.md)

Zmienne środowiskowe, porty i tryby uruchomienia. Skrócona instrukcja
uruchomienia jest w [README](../README.md#uruchomienie).

## Spis treści

- [Zmienne środowiskowe](#zmienne-środowiskowe)
- [OpenTelemetry](#opentelemetry)
- [Docker: wymagania i porty](#docker-wymagania-i-porty)
- [Tryb lokalny (uv)](#tryb-lokalny-uv)
- [Devcontainer](#devcontainer)

## Zmienne środowiskowe

Nazwa zmiennej = pole `Settings` w
[`common/config.py`](../src/webhook_receiver/common/config.py) wielkimi
literami.

Konfiguracja jest walidowana przy starcie:

- `LEASE_SECONDS > SIMULATED_MAX_SECONDS`,
- `SIMULATED_MIN_SECONDS ≤ SIMULATED_MAX_SECONDS`,
- `BACKOFF_MAX_SECONDS ≥ BACKOFF_BASE_SECONDS`.

| Zmienna | Domyślnie | Znaczenie |
|---|---|---|
| `DB_PATH` | `data/webhooks.db` (w obrazie `/data/webhooks.db`) | plik bazy |
| `API_PORT` | `8000` | port API |
| `EMBEDDED_WORKER` | `false` | worker w procesie API (development) |
| `MAX_ATTEMPTS` | `5` | liczba prób przed `failed` |
| `LEASE_SECONDS` | `30` | czas rezerwacji |
| `BACKOFF_BASE_SECONDS` / `BACKOFF_MAX_SECONDS` | `2` / `60` | odstęp ponowień |
| `WORKER_CONCURRENCY` | `20` | sloty na worker |
| `POLL_INTERVAL_SECONDS` | `0.5` | odpytywanie przy pustej kolejce |
| `SHUTDOWN_GRACE_SECONDS` | `10` | czas na dokończenie prób po SIGTERM |
| `SIMULATED_MIN_SECONDS` / `SIMULATED_MAX_SECONDS` | `2` / `5` | czas symulacji |
| `RATE_LIMIT_PER_SECOND` / `RATE_LIMIT_BURST` | `50` / `100` | limit per `X-Source` |
| `RATE_LIMIT_MAX_SOURCES` | `10000` | rozmiar LRU kubełków |
| `MAX_BACKLOG` | `100000` | próg backpressure |
| `BACKPRESSURE_RETRY_AFTER_SECONDS` | `5` | `Retry-After` dla `503` |
| `QUEUE_STATS_REFRESH_SECONDS` | `1` | odświeżanie cache statystyk kolejki |
| `MAX_BODY_BYTES` | `262144` | limit body |
| `RETENTION_INTERVAL_SECONDS` | `600` | co ile worker uruchamia retencję |
| `RETENTION_PAYLOAD_DAYS` | `7` | czyszczenie payloadu i wyniku `completed` |
| `RETENTION_COMPLETED_DAYS` / `RETENTION_FAILED_DAYS` | `30` / `30` | usuwanie wierszy |
| `RETENTION_BATCH_SIZE` | `1000` | wierszy na transakcję retencji |
| `WORKER_METRICS_PORT` | `9100` | serwer pomocniczy workera |
| `LOG_LEVEL` | `INFO` | poziom logów (niżej) |

`LOG_LEVEL` przyjmuje `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`
(wielkość liter dowolna).

## OpenTelemetry

| Zmienna | W compose | Znaczenie |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4318` | adres OTLP/HTTP |
| `OTEL_TRACES_EXPORTER` | — | eksporter śladów: `otlp`, `console` albo `none` |
| `OTEL_METRICS_EXPORTER` | `otlp` | `otlp` włącza wypychanie metryk przez OTLP |
| `OTEL_METRIC_EXPORT_INTERVAL` | `5000` | co ile ms wypychać metryki (domyślnie w SDK 60000) |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `3` | limit jednej wysyłki OTLP łącznie z ponowieniami |

Uwagi do tabeli:

- Samo ustawienie `OTEL_EXPORTER_OTLP_ENDPOINT` przełącza eksport śladów
  na OTLP.
- `OTEL_METRICS_EXPORTER` może być listą (np. `prometheus,otlp`). Bez
  `otlp` metryki są tylko na lokalnym `/metrics`.
- `OTEL_EXPORTER_OTLP_TIMEOUT` jest **w sekundach** (SDK Pythona;
  specyfikacja OTel podaje ms). Ogranicza też końcowe wypchnięcie przy
  zamykaniu.

Działanie domyślne:

- Lokalne `/metrics` (API, worker :9100) działają zawsze, niezależnie od
  `OTEL_METRICS_EXPORTER`.
- Bez Collectora (np. tryb lokalny) ślady idą domyślnie na konsolę
  (stderr), a metryki tylko na `/metrics`.
- `OTEL_SERVICE_NAME` **nie jest uwzględniany**. Nazwy usług
  (`webhook-api`, `webhook-worker`) są ustawione w kodzie
  (→ [TODO](../README.md#todo)).

## Docker: wymagania i porty

**Wymagania:** Docker Engine z Compose v2 (Linux, macOS z Docker
Desktop/OrbStack, Windows z WSL2).

- Obraz bazowy `python:3.14-slim-trixie` jest wieloarchitekturowy (amd64,
  arm64).
- Baza leży na nazwanym wolumenie.
- Jedyny bind mount to konfiguracja Collectora (`otel-collector.yaml`,
  tylko do odczytu).

```bash
docker compose up -d --build --wait   # api + 3 workery + Collector + Jaeger
docker compose ps                     # api i workery: (healthy)
curl -s localhost:8000/ready          # {"status":"ready"}
```

**Liczba workerów:** domyślnie `deploy.replicas: 3`, nadpisywana przez
`--scale worker=N`.

**Zatrzymanie:**

- `docker compose down` — dane zostają na wolumenie `webhook-data`,
- `docker compose down -v` — usuwa także dane.

| Usługa | Adres | Port hosta |
|---|---|---|
| API | `http://localhost:8000` | zmienna `API_PORT` |
| Jaeger UI | `http://localhost:16686` | zmienna `JAEGER_UI_PORT` |
| Collector, metryki wszystkich procesów | `http://localhost:8889/metrics` | zmienna `COLLECTOR_METRICS_PORT` |
| worker `/health`, `/metrics` | port 9100 tylko w sieci compose | celowo niepublikowany |

Uwagi do tabeli:

- Zmiana portu API, np. `API_PORT=8080 docker compose up -d`.
- Porty OTLP Collectora (4317, 4318) są dostępne tylko w sieci compose.
- Port workera jest niepublikowany celowo — `--scale` nie koliduje o port.

## Tryb lokalny (uv)

**Wymagania:** Python 3.14 i [uv](https://docs.astral.sh/uv/).

Tryb `EMBEDDED_WORKER=true` uruchamia worker w procesie API — wyłącznie
dla wygody developmentu (bez Dockera, jeden proces).

```bash
uv sync --extra dev
EMBEDDED_WORKER=true uv run python -m webhook_receiver api
```

- Baza: `data/webhooks.db` (zmienna `DB_PATH`), API na porcie 8000
  (`API_PORT`).
- Bez `OTEL_EXPORTER_OTLP_ENDPOINT` spany są wypisywane na stderr (logi
  JSON idą na stdout); `OTEL_TRACES_EXPORTER=none` je wyłącza.
- W tym trybie nie startuje serwer pomocniczy workera (9100), a metryki
  workera trafiają na `/metrics` API.
- Jeśli wbudowany worker ulegnie awarii, **API dalej przyjmuje webhooki**
  (świadoma decyzja: przyjmowanie nie zależy od przetwarzania):
  - awaria jest logowana od razu (`"event": "embedded_worker_crashed"`),
  - brak postępu widać po rosnącym
    `webhook_queue_oldest_pending_age_seconds`.
- Osobne procesy bez Dockera (ten sam `DB_PATH`):
  - `uv run python -m webhook_receiver api`,
  - w drugim terminalu `uv run python -m webhook_receiver worker`.

## Devcontainer

Repozytorium zawiera devcontainer
(`mcr.microsoft.com/devcontainers/python:3.14-trixie`) z funkcją
**docker-in-docker**: osobny demon Dockera wewnątrz kontenera, bez dostępu
do demona i plików hosta.

- W nim powstały wszystkie wyniki z [sekcji Wyniki](results.md).
- Polecenia `docker compose …` działają tak samo jak na „gołym” hoście.
- Porty 8000 i 16686 są przekazywane do hosta.
- Devcontainer ma też `sqlite3` CLI (patrz
  [SQLite dla operatora](operations.md#sqlite-dla-operatora)).
