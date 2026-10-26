# Konfiguracja

[← README](../README.md)

Zmienne środowiskowe, porty i tryby uruchomienia. Skrócona instrukcja uruchomienia jest w [README](../README.md#uruchomienie).

## Zmienne środowiskowe

Zmienne środowiskowe (nazwa = pole `Settings` w
[`common/config.py`](../src/webhook_receiver/common/config.py) wielkimi
literami). Konfiguracja jest walidowana przy starcie
(`LEASE_SECONDS > SIMULATED_MAX_SECONDS`, `SIMULATED_MIN_SECONDS ≤
SIMULATED_MAX_SECONDS`, `BACKOFF_MAX_SECONDS ≥ BACKOFF_BASE_SECONDS`).

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
| `LOG_LEVEL` | `INFO` | poziom logów |

## OpenTelemetry

OpenTelemetry: `OTEL_EXPORTER_OTLP_ENDPOINT` przełącza eksport śladów na
OTLP/HTTP (w compose `http://jaeger:4318`); `OTEL_TRACES_EXPORTER`
(`otlp`, `console`, `none`) wybiera eksporter jawnie. `OTEL_SERVICE_NAME`
**nie jest uwzględniany** — nazwy usług (`webhook-api`, `webhook-worker`)
są ustawione w kodzie (→ [TODO](../README.md#todo)).

## Docker: wymagania i porty

Wymagania: Docker Engine z Compose v2 (Linux, macOS z Docker
Desktop/OrbStack, Windows z WSL2). Obraz bazowy `python:3.14-slim-trixie`
jest wieloarchitekturowy (amd64, arm64); baza leży na nazwanym wolumenie,
bez bind mountów.

```bash
docker compose up -d --build --wait   # api + 3 workery + Jaeger
docker compose ps                     # api i workery: (healthy)
curl -s localhost:8000/ready          # {"status":"ready"}
```

Liczba workerów domyślnie `deploy.replicas: 3`, nadpisywana przez
`--scale worker=N`. Zatrzymanie: `docker compose down` (dane zostają na
wolumenie `webhook-data`) albo `docker compose down -v` (także dane).

| Usługa | Adres | Uwagi |
|---|---|---|
| API | `http://localhost:8000` | port hosta: zmienna `API_PORT` (np. `API_PORT=8080 docker compose up -d`) |
| Jaeger UI | `http://localhost:16686` | port hosta: zmienna `JAEGER_UI_PORT` |
| worker `/health`, `/metrics` | port 9100 tylko w sieci compose | celowo niepublikowany — `--scale` nie koliduje o port |

## Tryb lokalny (uv)

Wymagania: Python 3.14 i [uv](https://docs.astral.sh/uv/). Tryb
`EMBEDDED_WORKER=true` uruchamia worker w procesie API — wyłącznie dla
wygody developmentu (bez Dockera, jeden proces).

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
  (świadoma decyzja: przyjmowanie nie zależy od przetwarzania). Awaria jest
  logowana od razu (`"event": "embedded_worker_crashed"`), a brak postępu
  widać po rosnącym `webhook_queue_oldest_pending_age_seconds`.
- Osobne procesy bez Dockera: `uv run python -m webhook_receiver api` i w
  drugim terminalu `uv run python -m webhook_receiver worker` (ten sam
  `DB_PATH`).

## Devcontainer

Repozytorium zawiera devcontainer (`mcr.microsoft.com/devcontainers/python:3.14-trixie`)
z funkcją **docker-in-docker**: osobny demon Dockera wewnątrz kontenera,
bez dostępu do demona i plików hosta. W nim powstały wszystkie wyniki z
[sekcji Wyniki](results.md); polecenia `docker compose …` działają tak samo jak
na „gołym” hoście, porty 8000 i 16686 są przekazywane do hosta. Devcontainer
ma też `sqlite3` CLI (patrz [SQLite dla operatora](operations.md#sqlite-dla-operatora)).
