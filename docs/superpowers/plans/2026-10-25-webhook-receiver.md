# Webhook Receiver — plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prototyp serwisu HTTP przyjmującego webhooki z gwarancją
przetworzenia, idempotencją po `idempotency_key`, statusem zdarzenia,
retencją, obserwowalnością OTel, testem obciążeniowym i benchmarkami.

**Architecture:** Dwa procesy z jednego obrazu — `api` (FastAPI, jeden
proces uvicorn) zapisuje zdarzenia do SQLite (WAL), `worker` (asyncio, N
slotów) pobiera je atomowym claimem z lease i ogrodzeniem numerem próby.
SQLite jest jednocześnie magazynem i kolejką. Ślad OTel przechodzi przez
granicę procesów kolumną `traceparent`.

**Tech Stack:** Python 3.14, FastAPI, uvicorn, pydantic-settings, sqlite3
(stdlib), OpenTelemetry SDK + eksportery Prometheus/OTLP, prometheus-client,
pytest (+ pytest-asyncio, pytest-cov), httpx, ruff, Docker Compose v2,
Jaeger, GitHub Actions.

**Spec:** [docs/superpowers/specs/2026-10-25-webhook-receiver-design.md](../specs/2026-10-25-webhook-receiver-design.md)
(zaakceptowany). Sekcje spec cytowane jako §N. Przebieg decyzji:
[brainstorming](../specs/2026-10-25-webhook-receiver-brainstorming.md).

## Global Constraints

- Python **3.14**; obraz Docker `python:3.14-slim`; devcontainer
  `mcr.microsoft.com/devcontainers/python:3.14-trixie`.
- SQLite **≥ 3.37** — każdy proces sprawdza `sqlite3.sqlite_version` przy
  starcie i kończy się czytelnym błędem poniżej minimum.
- Połączenia SQLite: `autocommit=True` + jawne `BEGIN IMMEDIATE`/`COMMIT`/
  `ROLLBACK`; `PRAGMA busy_timeout=5000`, `synchronous=FULL`,
  `journal_mode=WAL` na każdym połączeniu; połączenie per wątek; wiersze z
  `RETURNING` pobierane przed `COMMIT`.
- Czasy w bazie: INTEGER, Unix epoch w **milisekundach**; w API ISO 8601 UTC
  (`2026-10-25T10:00:08Z`).
- Tabela `webhook_events` i indeksy dokładnie jak w §7 (z
  `ix_events_ready (run_at, status) WHERE status IN ('pending','retrying')`
  i `ix_events_leased (run_at) WHERE status = 'processing'`); zapytania
  claim/recover/stats powtarzają dosłownie warunki indeksów.
- API: **jeden** proces uvicorn (bez `--workers`).
- PEP 8 przez ruff: reguły `E, W, N, F, I, B, UP`, `line-length = 79`,
  `target-version = "py314"`; `ruff check` i `ruff format --check` zielone
  dla całego repo; `# noqa` tylko z komentarzem uzasadniającym.
- Testy: TDD, deterministyczne (wstrzykiwany zegar, brak `sleep` > 0,1 s),
  osobny plik bazy na test (`tmp_path`); `tests/unit` i `tests/integration`;
  pokrycie ≥ **85%** liczone tylko dla `src/webhook_receiver`.
- Każde zadanie kończy się zielonymi `ruff check .`, `ruff format --check .`
  i `pytest` — to warunek ukończenia zadania.
- Język: identyfikatory, komentarze i komunikaty w kodzie po angielsku;
  README po polsku [decyzja planu].
- Commity kończą się linią
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Payload nigdy nie trafia do atrybutów spanów ani logów.

## Review Focus

1. **Równoczesne `POST` z tym samym nowym kluczem** (retry nadawcy w tym
   samym momencie) — dokładnie jedna odpowiedź `202`, pozostałe `200
   duplicate`, jeden wiersz. Test: Task 3 (`test_concurrent_insert_same_key`)
   i Task 10 (`test_concurrent_posts_same_key`).
2. **Zniekształcone ciało `POST`** (nie-JSON, `payload` jako tablica/`null`,
   brak pól, klucz 256 znaków) — `422`, nigdy `500`. Test: Task 10
   (`test_post_invalid_bodies_return_422`).
3. **Klucze ze spacją, `%`, `/`, `?`, `#`, UTF-8 i dokładnie 255 znaków** —
   zapis i `GET` przez `status_url` zwracają ten sam klucz. Test: Task 10
   (`test_get_roundtrip_special_keys`).
4. **SIGTERM w trakcie przetwarzania** — worker nie pobiera nowej pracy,
   kończy trwające zadania w `SHUTDOWN_GRACE`, niedokończone wracają przez
   wygaśnięcie lease. Test: Task 11 (`test_stop_finishes_in_flight`).
5. **Pierwszy start na pustym wolumenie / bez katalogu bazy** — katalog
   tworzony, schemat inicjalizowany równolegle przez kilka procesów bez
   błędu. Test: Task 2 (`test_connect_creates_parent_dir`,
   `test_init_schema_concurrent`).

## Struktura plików

```
.devcontainer/devcontainer.json          # obraz 3.14-trixie + docker-in-docker
pyproject.toml                           # zależności, ruff, pytest, coverage
src/webhook_receiver/
  __init__.py
  __main__.py                            # CLI: api | worker
  config.py                              # Settings (env)
  clock.py                               # Clock, SystemClock
  models.py                              # Status, Event, QueueStats, …
  db.py                                  # connect, immediate, schema, pool
  repository.py                          # EventRepository (całe SQL)
  backoff.py                             # backoff_ms
  simulation.py                          # simulate, SimulatedFailure
  ratelimit.py                           # RateLimiter
  queue_stats.py                         # QueueStatsCache
  telemetry.py                           # tracing, metryki, traceparent
  logging_setup.py                       # logi JSON z trace_id
  api/__init__.py
  api/app.py                             # create_app, trasy
  api/schemas.py                         # WebhookIn, StatusOut
  api/body_limit.py                      # BodySizeLimitMiddleware
  worker.py                              # Worker
  worker_http.py                         # /health, /metrics na 9100
loadtest/run.py, loadtest/stats.py       # generator i raport
scripts/scaling.sh                       # orkiestracja scenariusza scaling
benchmarks/sqlite_bench.py               # 4 benchmarki
tests/fakes.py, tests/conftest.py
tests/unit/…, tests/integration/…, tests/loadtest/…
Dockerfile, docker-compose.yml, .dockerignore
.github/workflows/ci.yml
README.md
```

---

### Task 0: Devcontainer — Python 3.14 i docker-in-docker

**Files:**
- Modify: `.devcontainer/devcontainer.json`

**Interfaces:**
- Produces: środowisko z `python3.14`, SQLite 3.46.1, `docker` i
  `docker compose` (wymagane od Task 13).

- [ ] **Step 1: Zmień obraz i dodaj feature**

`"image": "mcr.microsoft.com/devcontainers/python:3.14-trixie"`; w
`features` dodaj `"ghcr.io/devcontainers/features/docker-in-docker:2": {}`.
Ustaw `"python.defaultInterpreterPath": "/usr/local/bin/python"` bez zmian.
Dodaj `16686` do `forwardPorts` (UI Jaegera).

- [ ] **Step 2: Commit i push**

```bash
git add .devcontainer/devcontainer.json
git commit -m "chore: devcontainer on Python 3.14 trixie with docker-in-docker"
git push
```

- [ ] **Step 3: STOP — przebudowa przez użytkownika**

Poproś użytkownika o „Rebuild Container” w VS Code. Po przebudowie
zweryfikuj:

Run: `python --version && python -c "import sqlite3;print(sqlite3.sqlite_version)" && docker info --format '{{.ServerVersion}}' && docker compose version`
Expected: `Python 3.14.x`, `3.46.1`, wersja serwera Dockera, `Docker Compose version v2.x`.

---

### Task 1: Szkielet projektu, konfiguracja, zegar, CI (lint + testy)

**Files:**
- Create: `pyproject.toml`, `src/webhook_receiver/__init__.py`,
  `src/webhook_receiver/config.py`, `src/webhook_receiver/clock.py`,
  `tests/__init__.py`, `tests/fakes.py`, `tests/conftest.py`,
  `tests/unit/test_config.py`, `.github/workflows/ci.yml`

**Interfaces:**
- Produces:
  - `Settings(BaseSettings)` — pola (zmienna środowiskowa = nazwa pola
    wielkimi literami, bez prefiksu), wartości domyślne z spec:
    `db_path: Path = Path("data/webhooks.db")`, `api_port: int = 8000`,
    `max_attempts: int = 5`, `lease_seconds: float = 30`,
    `backoff_base_seconds: float = 2`, `backoff_max_seconds: float = 60`,
    `worker_concurrency: int = 20`, `poll_interval_seconds: float = 0.5`,
    `shutdown_grace_seconds: float = 10`,
    `simulated_min_seconds: float = 2`, `simulated_max_seconds: float = 5`,
    `rate_limit_per_second: float = 50`, `rate_limit_burst: int = 100`,
    `rate_limit_max_sources: int = 10_000`, `max_backlog: int = 100_000`,
    `backpressure_retry_after_seconds: int = 5`,
    `queue_stats_refresh_seconds: float = 1`,
    `retention_interval_seconds: float = 600`,
    `retention_payload_days: float = 7`,
    `retention_completed_days: float = 30`,
    `retention_failed_days: float = 30`,
    `retention_batch_size: int = 1000`, `max_body_bytes: int = 262_144`,
    `worker_metrics_port: int = 9100`, `embedded_worker: bool = False`,
    `log_level: str = "INFO"`.
  - Walidator modelu: `lease_seconds > simulated_max_seconds` oraz
    `simulated_min_seconds <= simulated_max_seconds`, inaczej
    `ValueError`.
  - `class Clock(Protocol): def now_ms(self) -> int`;
    `class SystemClock` (`time.time_ns() // 1_000_000`).
  - `tests/fakes.py`: `class FakeClock` z `__init__(self, start_ms: int =
    1_700_000_000_000)`, `now_ms()`, `advance(self, ms: int) -> None`.

- [ ] **Step 1: `pyproject.toml`**

Build backend `hatchling`, pakiet w `src/`. Zależności przypięte `==` do
najnowszych wydań sprawdzonych `pip index versions <pakiet>`:
`fastapi`, `uvicorn`, `pydantic-settings`, `opentelemetry-api`,
`opentelemetry-sdk`, `opentelemetry-exporter-prometheus`,
`opentelemetry-exporter-otlp-proto-http`,
`opentelemetry-instrumentation-fastapi` (wersja `0.XbY` odpowiadająca
`opentelemetry-sdk 1.X`), `prometheus-client`. Extra `loadtest`: `httpx`.
Extra `dev`: `pytest`, `pytest-asyncio`, `pytest-cov`, `httpx`, `ruff`.
`[tool.ruff]` wg Global Constraints. `[tool.pytest.ini_options]`:
`testpaths = ["tests"]`, `asyncio_mode = "auto"`,
`addopts = "--cov=webhook_receiver --cov-report=term-missing"`.
`[tool.coverage.report] fail_under = 85`.

- [ ] **Step 2: Napisz test konfiguracji (failing)**

```python
def test_defaults_match_spec():
    s = Settings()
    assert s.max_attempts == 5 and s.lease_seconds == 30
    assert s.worker_concurrency == 20 and s.max_backlog == 100_000
    assert s.max_body_bytes == 262_144

def test_env_override(monkeypatch):
    monkeypatch.setenv("MAX_ATTEMPTS", "3")
    assert Settings().max_attempts == 3

def test_lease_must_exceed_simulation():
    with pytest.raises(ValueError):
        Settings(lease_seconds=5, simulated_max_seconds=5)
```

- [ ] **Step 3: Uruchom — FAIL**

Run: `pip install -e ".[dev,loadtest]" && pytest tests/unit/test_config.py -v`
Expected: FAIL (`ImportError: Settings`).

- [ ] **Step 4: Zaimplementuj `config.py`, `clock.py`, `tests/fakes.py`**

- [ ] **Step 5: Uruchom — PASS** (`pytest tests/unit/test_config.py -v`)

- [ ] **Step 6: `.github/workflows/ci.yml`**

Trigger `push` i `pull_request`; job `test` na `ubuntu-latest`,
`actions/setup-python` z `python-version: "3.14"`; kroki:
`pip install -e ".[dev,loadtest]"`, `ruff check .`, `ruff format --check .`,
`pytest`. (Krok `docker build` dochodzi w Task 13.) Próg 85% obowiązuje
od pierwszego commita — każde zadanie dodaje kod razem z testami, więc
pokrycie nie powinno spaść poniżej progu; jeśli spadnie, zadanie nie jest
ukończone.

- [ ] **Step 7: Lint + commit + push**

Run: `ruff check . && ruff format --check . && pytest`
```bash
git add -A && git commit -m "feat: project skeleton, settings, clock, CI" && git push
```

---

### Task 2: Warstwa bazy — połączenie, transakcje, schemat

**Files:**
- Create: `src/webhook_receiver/db.py`, `src/webhook_receiver/models.py`,
  `tests/unit/test_db.py`

**Interfaces:**
- Consumes: —
- Produces (`db.py`):
  - `MIN_SQLITE_VERSION: tuple[int, int, int] = (3, 37, 0)`
  - `check_sqlite_version(version: str = sqlite3.sqlite_version) -> None`
    — `RuntimeError("SQLite >= 3.37.0 required, found X")`.
  - `connect(path: Path | str) -> sqlite3.Connection` — tworzy katalog
    nadrzędny, `autocommit=True`, `row_factory=sqlite3.Row`, pragmy z Global
    Constraints; `journal_mode=WAL` ponawiane do 5 razy, jeśli zwrot ≠
    `wal`.
  - `@contextmanager immediate(conn) -> Iterator[sqlite3.Connection]` —
    `BEGIN IMMEDIATE`, `COMMIT`, przy wyjątku `ROLLBACK` i re-raise.
  - `SCHEMA_SQL: str` — DDL z §7 (`CREATE … IF NOT EXISTS`).
  - `init_schema(conn) -> None` — `SCHEMA_SQL` w `immediate`.
  - `class ConnectionPool` — `__init__(self, path: Path | str)`,
    `get(self) -> sqlite3.Connection` (per wątek, `threading.local`),
    `close_all(self) -> None`.
- Produces (`models.py`):
  - `class Status(StrEnum)`: `PENDING="pending"`, `PROCESSING`,
    `RETRYING`, `COMPLETED`, `FAILED`.
  - `@dataclass(frozen=True) class Event`: `id: int`,
    `idempotency_key: str`, `source: str`, `event_type: str`,
    `payload: dict | None`, `status: Status`, `attempts: int`,
    `run_at: int`, `last_error: str | None`, `result: dict | None`,
    `traceparent: str | None`, `created_at: int`,
    `finished_at: int | None`; property `payload_purged: bool`
    (`finished_at is not None and payload is None`).
  - `@dataclass(frozen=True) class QueueStats`: `pending: int`,
    `retrying: int`, `processing: int`, `oldest_due_age_ms: int`;
    property `backlog` = `pending + retrying`.
  - `@dataclass(frozen=True) class RecoveryResult`: `retried: int`,
    `failed: int`.
  - `@dataclass(frozen=True) class RetentionPolicy`: `payload_ms: int`,
    `completed_ms: int`, `failed_ms: int`, `batch_size: int`.
  - `@dataclass(frozen=True) class PurgeResult`: `payloads_cleared: int`,
    `completed_deleted: int`, `failed_deleted: int`.

- [ ] **Step 1: Napisz testy (failing)**

```python
def test_check_sqlite_version_rejects_old():
    with pytest.raises(RuntimeError, match="3.37.0"):
        check_sqlite_version("3.36.9")
    check_sqlite_version("3.46.1")

def test_connect_sets_pragmas(tmp_path):
    conn = connect(tmp_path / "a.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000

def test_connect_creates_parent_dir(tmp_path):
    connect(tmp_path / "nested" / "dir" / "a.db")
    assert (tmp_path / "nested" / "dir" / "a.db").exists()

def test_immediate_rolls_back_on_error(tmp_path): ...
    # INSERT w immediate, raise → wiersza nie ma

def test_schema_is_strict(tmp_path):
    # INSERT tekstu do run_at → sqlite3.IntegrityError lub
    # sqlite3.DataError ("cannot store TEXT value in INTEGER column")

def test_init_schema_concurrent(tmp_path):
    # 4 wątki, każdy z własnym connect(), równocześnie init_schema
    # → brak wyjątków, tabela istnieje

def test_pool_returns_connection_per_thread(tmp_path):
    # pool.get() w wątku głównym is pool.get(); w innym wątku — inny obiekt
```

- [ ] **Step 2: Uruchom — FAIL** (`pytest tests/unit/test_db.py -v`)

- [ ] **Step 3: Zaimplementuj `db.py` i `models.py`**

- [ ] **Step 4: Uruchom — PASS**

- [ ] **Step 5: Lint + commit** (`feat: sqlite connection, transactions, schema`)

---

### Task 3: Repozytorium — przyjęcie i odczyt

**Files:**
- Create: `src/webhook_receiver/repository.py`,
  `tests/unit/test_repository_intake.py`

**Interfaces:**
- Consumes: `ConnectionPool`, `immediate`, `Event`, `Status`, `Clock`.
- Produces:
  - `class EventRepository` — `__init__(self, pool: ConnectionPool,
    clock: Clock)`.
  - `insert_or_get(self, key: str, source: str, event_type: str,
    payload: dict, traceparent: str | None) -> tuple[Event, bool]` —
    `True` = utworzone. Jedna transakcja `immediate`:
    `INSERT … ON CONFLICT(idempotency_key) DO NOTHING RETURNING *`; brak
    wiersza → `SELECT` istniejącego. Nowy: `status='pending'`,
    `attempts=0`, `run_at=created_at=now`. Payload zapisywany
    `json.dumps(payload, separators=(",", ":"))`.
  - `get(self, key: str) -> Event | None`.
  - `_row_to_event(row: sqlite3.Row) -> Event` (prywatna, wspólna dla
    dalszych metod).

- [ ] **Step 1: Napisz testy (failing)**

```python
def test_insert_new_event(repo, clock):
    ev, created = repo.insert_or_get("evt-001", "shop", "order.created",
                                     {"order_id": "ORD-2024-1234"}, None)
    assert created and ev.status is Status.PENDING
    assert ev.attempts == 0 and ev.run_at == ev.created_at == clock.now_ms()

def test_duplicate_returns_existing_regardless_of_payload(repo):
    repo.insert_or_get("evt-1", "shop", "a", {"x": 1}, None)
    ev, created = repo.insert_or_get("evt-1", "other", "b", {"x": 2}, None)
    assert not created and ev.source == "shop" and ev.payload == {"x": 1}

def test_get_unknown_returns_none(repo):
    assert repo.get("nope") is None

def test_traceparent_persisted(repo): ...

def test_concurrent_insert_same_key(tmp_path):
    # 8 wątków, każdy własny EventRepository na tym samym pliku,
    # Barrier → insert_or_get("same", …); dokładnie jeden created=True,
    # COUNT(*) == 1
```

Fixture `repo` (w `tests/conftest.py`): `FakeClock`, `ConnectionPool(tmp_path
/ "t.db")`, `init_schema`, `EventRepository`.

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: event intake and lookup`)

---

### Task 4: Repozytorium — claim, odzyskiwanie, wynik z ogrodzeniem

**Files:**
- Modify: `src/webhook_receiver/repository.py`
- Create: `tests/unit/test_repository_lifecycle.py`

**Interfaces:**
- Produces (metody `EventRepository`):
  - `claim(self, limit: int, lease_ms: int) -> list[Event]` — §8:
    `UPDATE webhook_events SET status='processing', attempts=attempts+1,
    run_at=:now+:lease WHERE id IN (SELECT id FROM webhook_events WHERE
    status IN ('pending','retrying') AND run_at <= :now ORDER BY run_at
    LIMIT :limit) RETURNING *`; zwraca zdarzenia po claimie (z nowym
    `attempts`).
  - `recover_expired(self, max_attempts: int) -> RecoveryResult` — dwie
    instrukcje w jednej transakcji, warunek `status = 'processing' AND
    run_at <= :now`: `attempts < max` → `retrying`, `run_at=now`,
    `last_error='lease expired'`; `attempts >= max` → `failed`,
    `finished_at=now`, `last_error='lease expired'`.
  - `complete(self, event_id: int, attempt: int, result: dict) -> bool` —
    `WHERE id=? AND status='processing' AND attempts=?`; ustawia
    `status='completed'`, `result_json`, `finished_at=now`,
    `last_error=NULL`; `False` = odrzucony (nieaktualna próba).
  - `record_failure(self, event_id: int, attempt: int, error: str,
    retry_at: int | None) -> bool` — ten sam warunek ogrodzenia;
    `retry_at` → `retrying`, `run_at=retry_at`; `None` → `failed`,
    `finished_at=now`.

- [ ] **Step 1: Napisz testy (failing)**

```python
def test_claim_takes_due_in_run_at_order_and_increments_attempts(repo, clock)
    # 3 zdarzenia z rosnącym created; claim(2, 30_000) → dwa najstarsze,
    # status PROCESSING, attempts == 1, run_at == now + 30_000

def test_claim_skips_not_yet_due_retrying(repo, clock)
def test_two_claims_never_share_rows(tmp_path)
    # 2 wątki × claim(50) na 100 zdarzeniach → rozłączne zbiory id, suma 100
def test_complete_with_current_attempt(repo)
def test_complete_rejected_for_stale_attempt(repo, clock)
    # claim → advance(31_000) → recover_expired(5) → claim (attempts=2)
    # → complete(id, attempt=1, …) is False; complete(id, 2, …) is True
def test_record_failure_schedules_retry(repo, clock)
def test_record_failure_without_retry_marks_failed(repo, clock)
def test_recover_expired_moves_to_retrying_with_lease_expired(repo, clock)
    # status RETRYING, run_at == now, last_error == "lease expired"
def test_recover_expired_at_max_attempts_fails(repo, clock)
def test_queue_queries_use_partial_indexes(repo)
    # EXPLAIN QUERY PLAN dla SQL claimu i recover: żaden wiersz planu nie
    # zaczyna się od "SCAN webhook_events"; claim używa ix_events_ready,
    # recover ix_events_leased
```

Do testu planów wystaw SQL jako stałe modułu: `CLAIM_SQL`,
`RECOVER_RETRY_SQL`, `RECOVER_FAIL_SQL` (używane i przez metody, i przez
test).

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: claim, lease recovery and fenced completion`)

---

### Task 5: Repozytorium — statystyki kolejki i retencja

**Files:**
- Modify: `src/webhook_receiver/repository.py`
- Create: `tests/unit/test_repository_stats_retention.py`

**Interfaces:**
- Produces:
  - `queue_stats(self) -> QueueStats` — liczniki: `SELECT status,
    COUNT(*) … WHERE status IN ('pending','retrying') GROUP BY status` i
    `… WHERE status = 'processing'`; `oldest_due_age_ms = now −
    min(MIN(run_at) z ready z run_at<=now, MIN(run_at) z processing z
    run_at<=now)`, `0` gdy brak (§12).
  - `purge(self, policy: RetentionPolicy) -> PurgeResult` — porcjami po
    `policy.batch_size`, każda porcja we własnej transakcji, aż do 0
    zmienionych; przenośnie `WHERE id IN (SELECT id … LIMIT ?)`:
    (1) `completed` z `finished_at < now − payload_ms` i
    `payload_json IS NOT NULL` → `payload_json=NULL, result_json=NULL`;
    (2) `completed` z `finished_at < now − completed_ms` → `DELETE`;
    (3) `failed` z `finished_at < now − failed_ms` → `DELETE`.
  - Stałe `STATS_READY_SQL`, `STATS_PROCESSING_SQL`, `OLDEST_READY_SQL`,
    `OLDEST_LEASED_SQL`, `PURGE_PAYLOAD_SQL`, `PURGE_COMPLETED_SQL`,
    `PURGE_FAILED_SQL`.

- [ ] **Step 1: Napisz testy (failing)**

```python
def test_queue_stats_counts_by_status(repo)
def test_oldest_due_age_ignores_future_retries(repo, clock)
    # retrying z run_at w przyszłości → oldest_due_age_ms == 0
def test_oldest_due_age_includes_expired_leases(repo, clock)
def test_purge_clears_payload_after_7_days(repo, clock)
    # completed, advance(7 dni + 1 ms) → payload None, payload_purged True,
    # wiersz istnieje
def test_purge_deletes_completed_after_30_days(repo, clock)
def test_purge_keeps_failed_payload_until_deleted(repo, clock)
    # failed po 8 dniach ma payload; po 30 dniach + 1 ms usunięty
def test_purge_never_touches_unfinished(repo, clock)
def test_purge_works_in_batches(repo, clock)   # batch_size=2, 5 wierszy
def test_stats_and_purge_queries_use_indexes(repo)   # bez "SCAN webhook_events"
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: queue stats and two-tier retention`)

---

### Task 6: Backoff i symulacja przetwarzania

**Files:**
- Create: `src/webhook_receiver/backoff.py`,
  `src/webhook_receiver/simulation.py`, `tests/unit/test_backoff.py`,
  `tests/unit/test_simulation.py`

**Interfaces:**
- Produces:
  - `backoff_ms(attempt: int, base_ms: int, max_ms: int, rng: Random) ->
    int` — `min(base_ms * 2 ** (attempt - 1), max_ms) * rng.uniform(0.8,
    1.2)`, zaokrąglone do int.
  - `class SimulatedFailure(Exception)`.
  - `async def simulate(payload: dict, attempt: int, min_seconds: float,
    max_seconds: float, rng: Random, sleep: Callable[[float],
    Awaitable[None]] = asyncio.sleep) -> dict` — śpi
    `rng.uniform(min, max)`, potem: `payload.get("simulate") ==
    "fail_once"` i `attempt == 1` → `SimulatedFailure("simulated failure
    (fail_once)")`; `"fail_always"` → `SimulatedFailure("simulated
    failure (fail_always)")`; inaczej zwraca `{"duration_ms": int}`.

- [ ] **Step 1: Testy (failing)**

```python
def test_backoff_grows_exponentially_within_jitter():
    rng = Random(0)
    assert 1600 <= backoff_ms(1, 2000, 60_000, rng) <= 2400
    assert 3200 <= backoff_ms(2, 2000, 60_000, rng) <= 4800
def test_backoff_is_capped():
    assert backoff_ms(20, 2000, 60_000, Random(0)) <= 72_000

async def test_simulate_fail_once_only_first_attempt():
    sleeps = []
    async def fake_sleep(s): sleeps.append(s)
    with pytest.raises(SimulatedFailure, match="fail_once"):
        await simulate({"simulate": "fail_once"}, 1, 2, 5, Random(0), fake_sleep)
    assert "duration_ms" in await simulate({"simulate": "fail_once"}, 2, 2, 5,
                                           Random(0), fake_sleep)
    assert all(2 <= s <= 5 for s in sleeps)
async def test_simulate_fail_always(): ...
async def test_simulate_success_without_trigger(): ...
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: backoff and processing simulation`)

---

### Task 7: Rate limiter

**Files:**
- Create: `src/webhook_receiver/ratelimit.py`, `tests/unit/test_ratelimit.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class RateDecision`: `allowed: bool`,
    `retry_after_seconds: int`.
  - `class RateLimiter` — `__init__(self, rate_per_second: float, burst:
    int, clock: Clock, max_sources: int)`; `check(self, source: str) ->
    RateDecision`. Token bucket per źródło w `OrderedDict` (LRU:
    przy przekroczeniu `max_sources` usuwany najdawniej używany),
    `threading.Lock` (endpointy działają w puli wątków).
    `retry_after_seconds = max(1, ceil((1 - tokens) / rate))` przy
    odmowie, `0` przy zgodzie.

- [ ] **Step 1: Testy (failing)**

```python
def test_allows_burst_then_rejects(clock):
    rl = RateLimiter(50, 100, clock, 10)
    assert all(rl.check("a").allowed for _ in range(100))
    d = rl.check("a")
    assert not d.allowed and d.retry_after_seconds >= 1
def test_refills_over_time(clock): ...   # advance(1000) → 50 kolejnych OK
def test_sources_are_independent(clock): ...
def test_lru_bounds_memory(clock):
    rl = RateLimiter(1, 1, clock, 2)
    for s in "abc": rl.check(s)
    assert len(rl._buckets) == 2
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: per-source token bucket rate limiter`)

---

### Task 8: Telemetria — tracing, traceparent, metryki, logi JSON

**Files:**
- Create: `src/webhook_receiver/telemetry.py`,
  `src/webhook_receiver/logging_setup.py`,
  `tests/unit/test_telemetry.py`, `tests/unit/test_logging.py`
- Modify: `tests/conftest.py` (sesyjna fixture telemetrii)

**Interfaces:**
- Produces (`telemetry.py`):
  - `setup_telemetry(service_name: str, span_exporter: SpanExporter |
    None = None) -> None` — **raz na proces** (kolejne wywołania bez
    efektu): `TracerProvider` z `Resource(service.name,
    service.instance.id=hostname-pid)`; exporter: podany (testy) →
    `SimpleSpanProcessor`; inaczej wg `OTEL_TRACES_EXPORTER`
    (`none` → brak), `OTEL_EXPORTER_OTLP_ENDPOINT` ustawione → OTLP HTTP
    w `BatchSpanProcessor`, w przeciwnym razie `ConsoleSpanExporter(out=
    sys.stderr)`. `MeterProvider` z `PrometheusMetricReader`.
  - `get_tracer() -> Tracer`, `get_meter() -> Meter`.
  - `current_traceparent() -> str | None` — W3C `traceparent` z bieżącego
    kontekstu (`TraceContextTextMapPropagator.inject`).
  - `context_from_traceparent(traceparent: str | None) -> Context`.
  - `metrics_payload() -> tuple[bytes, str]` — `generate_latest()` i
    content type z `prometheus_client`.
  - `class ApiMetrics` — `__init__(self, stats: Callable[[],
    QueueStats])`; `received(self, source: str, outcome: str) -> None`
    (licznik `webhooks_received`, atrybuty `source`, `outcome` ∈
    `accepted|duplicate|rate_limited|backpressure|invalid`);
    rejestruje `ObservableGauge` `webhook_queue_depth` (atrybut
    `status`) i `webhook_queue_oldest_pending_age` (jednostka `s`),
    czytające wyłącznie `stats()`.
  - `class WorkerMetrics` — `processing_duration(self, seconds: float,
    outcome: str)` (histogram `webhook_processing_duration`, jednostka
    `s`), `attempt(self, outcome: str)` (licznik `webhook_attempts`,
    `success|retry|failed`), `lease_recoveries(self, count: int)`
    (licznik `webhook_lease_recoveries`), `stale_result(self)` (licznik
    `webhook_stale_results`).
- Produces (`logging_setup.py`):
  - `setup_logging(level: str) -> None` — handler na **stdout**,
    `JsonFormatter`: pola `ts`, `level`, `logger`, `message`, `trace_id`,
    `span_id` (z bieżącego spana, hex, puste gdy brak) oraz pola z
    `extra` (`idempotency_key`, `source`, `event`, …) —
    `json.dumps(..., ensure_ascii=False)`.

- [ ] **Step 1: Testy (failing)**

```python
def test_traceparent_roundtrip(span_exporter):
    with get_tracer().start_as_current_span("post") as parent:
        tp = current_traceparent()
    assert tp.startswith("00-")
    with get_tracer().start_as_current_span(
            "process", context=context_from_traceparent(tp)) as child:
        pass
    assert child.parent.span_id == parent.get_span_context().span_id
    assert child.context.trace_id == parent.get_span_context().trace_id

def test_metric_names_on_prometheus_endpoint():
    ApiMetrics(lambda: QueueStats(3, 1, 2, 1500)).received("shop", "accepted")
    WorkerMetrics().processing_duration(2.5, "success")
    body, _ = metrics_payload()
    text = body.decode()
    assert 'webhooks_received_total{' in text
    assert "webhooks_received_total_total" not in text
    assert "webhook_processing_duration_seconds_bucket" in text
    assert 'webhook_queue_depth{status="pending"} 3.0' in text

def test_json_log_has_trace_ids(capsys, span_exporter):
    # w spanie: logger.info("x", extra={"idempotency_key": "evt-1"})
    # → jedna linia JSON z trace_id (32 hex), idempotency_key
```

Fixture sesyjna `span_exporter` w `tests/conftest.py`:
`InMemorySpanExporter`, `setup_telemetry("test", exporter)`, `clear()`
przed każdym testem używającym jej. Dokładny format nazw metryk zależy od
wersji eksportera — jeśli asercja nazw nie przechodzi, dostosuj nazwy
instrumentów (nie asercje), aby `/metrics` dawał nazwy z §12.

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: OpenTelemetry tracing, metrics and JSON logs`)

---

### Task 9: Cache statystyk kolejki i middleware limitu ciała

**Files:**
- Create: `src/webhook_receiver/queue_stats.py`,
  `src/webhook_receiver/api/__init__.py`,
  `src/webhook_receiver/api/body_limit.py`,
  `tests/unit/test_queue_stats.py`, `tests/unit/test_body_limit.py`

**Interfaces:**
- Produces:
  - `class QueueStatsCache` — `__init__(self, repo: EventRepository,
    refresh_seconds: float)`; `refresh(self) -> None` (jedno zapytanie
    `repo.queue_stats()`); `get(self) -> QueueStats` (ostatnia wartość,
    bez zapytań; przed pierwszym refresh — zera); `start(self) -> None`
    (wątek daemon odświeżający co `refresh_seconds`); `stop(self) ->
    None`.
  - `class BodySizeLimitMiddleware` (czyste ASGI) — `__init__(self, app,
    max_bytes: int)`; `Content-Length > max_bytes` → `413` z JSON
    `{"detail": "request body too large"}` bez czytania ciała; brak
    nagłówka → liczy bajty w `receive` i po przekroczeniu zwraca `413`.

- [ ] **Step 1: Testy (failing)**

```python
def test_cache_returns_zeros_before_refresh(repo)
def test_cache_get_does_not_query(repo, monkeypatch)
    # po refresh(): podmień repo.queue_stats na raise → get() działa
def test_body_limit_rejects_content_length():
    # mała aplikacja Starlette + middleware(max_bytes=10), POST 11 B → 413
def test_body_limit_rejects_chunked_stream()
def test_body_limit_allows_small_body()
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: queue stats cache and body size limit`)

---

### Task 10: API HTTP

**Files:**
- Create: `src/webhook_receiver/api/schemas.py`,
  `src/webhook_receiver/api/app.py`, `tests/unit/test_api.py`

**Interfaces:**
- Consumes: `Settings`, `Clock`, `ConnectionPool`, `init_schema`,
  `check_sqlite_version`, `EventRepository`, `RateLimiter`,
  `QueueStatsCache`, `BodySizeLimitMiddleware`, `ApiMetrics`,
  `current_traceparent`, `setup_telemetry`, `setup_logging`.
- Produces:
  - `schemas.WebhookIn(BaseModel)`: `idempotency_key: str` (`min_length=1,
    max_length=255`), `event_type: str` (`min_length=1`), `payload:
    dict[str, Any]`; `model_config = ConfigDict(extra="ignore")`.
  - `schemas.StatusOut(BaseModel)` — pola i mapowanie z §6 (przykład
    `GET`), `duplicate: bool | None = None` (pomijane w JSON gdy `None`).
  - `create_app(settings: Settings, *, clock: Clock | None = None) ->
    FastAPI` — lifespan: `check_sqlite_version`, `ConnectionPool`,
    `init_schema`, `QueueStatsCache.start()`, przy
    `settings.embedded_worker` start `Worker` (Task 11) w tle; zamknięcie
    odwrotnie. `FastAPIInstrumentor.instrument_app(app,
    excluded_urls="health,ready,metrics")`. Dla testów `app.state` trzyma
    `repo`, `stats`, `limiter`.

Trasy (kolejność kontroli w `POST` z §6):

- `POST /webhooks` — `async def` z `request: Request`; ręcznie: brak/pusty
  lub > 64 znaków `X-Source` → `400`; ciało `WebhookIn.model_validate_json`
  (błąd → `422`, `{"detail": <errors>}`); `limiter.check` → `429` +
  `Retry-After`; jeśli `stats.get().backlog >= max_backlog`: istniejący
  klucz (`repo.get` w `run_in_threadpool`) → `200` duplikat, inaczej `503`
  + `Retry-After: backpressure_retry_after_seconds`; w przeciwnym razie
  `insert_or_get` w `run_in_threadpool` z `current_traceparent()` →
  `202` `{"idempotency_key", "status": "pending", "status_url"}` +
  `Location` (oba `/webhooks/` + `quote(key, safe="")`) albo `200`
  `StatusOut` z `duplicate=true`. Każdy wynik → `metrics.received`.
  Log `event=accepted|duplicate|…`.
- `GET /webhooks/{idempotency_key:path}` — pusty klucz lub brak → `404`;
  inaczej `StatusOut`.
- `GET /health` → `200 {"status": "ok"}`.
- `GET /ready` → `200 {"status": "ready"}` gdy `SELECT 1` i istnienie
  tabeli OK; inaczej `503 {"status": "not ready", "failed": [...]}`.
- `GET /metrics` → `metrics_payload()`.

- [ ] **Step 1: Testy (failing)**

Fixture `client`: `create_app(Settings(db_path=tmp_path/"a.db",
max_backlog=…), clock=FakeClock())` w `TestClient` (context manager).
Pięć webhooków z gistu jako stała `GIST_WEBHOOKS` w `tests/fakes.py`
(dosłownie `evt-001`…`evt-005`).

```python
def test_post_new_returns_202_with_location(client):
    r = client.post("/webhooks", json=GIST_WEBHOOKS[0], headers=SRC)
    assert r.status_code == 202
    assert r.headers["location"] == "/webhooks/evt-001"
    assert r.json() == {"idempotency_key": "evt-001", "status": "pending",
                        "status_url": "/webhooks/evt-001"}
def test_all_gist_examples_accepted(client)
def test_duplicate_returns_200_with_status(client)
    # drugi POST (także z innym payloadem i innym X-Source) → 200,
    # duplicate True, status "pending", source pierwszego zgłoszenia
def test_missing_or_bad_x_source_is_400_before_validation(client)
    # brak nagłówka + niepoprawne ciało → 400 (nie 422); "" i 65 znaków → 400
def test_post_invalid_bodies_return_422(client)
    # "not json", {"payload": []}, {"payload": None}, brak event_type,
    # idempotency_key "" i 256 znaków → 422; 255 znaków → 202
def test_body_over_256_kib_is_413(client)
def test_rate_limit_returns_429_with_retry_after(client_small_limits)
def test_backpressure_503_for_new_but_200_for_duplicate(client_backlog_1)
    # max_backlog=1: POST A (202), stats.refresh(); POST B → 503 +
    # Retry-After "5"; POST A ponownie → 200 duplicate
def test_get_roundtrip_special_keys(client)
    # klucze: "evt/1", "a b", "100%", "q?x#y", "zażółć", "x"*255:
    # POST → GET r.json()["status_url"] → 200 i ten sam idempotency_key
def test_get_unknown_and_empty_key_404(client)
def test_get_maps_fields(client, repo_access)
    # po claim + record_failure(retry_at) → status "retrying",
    # next_attempt_at ISO, max_attempts 5, attempts 1
def test_ready_and_health(client)
def test_metrics_exposes_received_counter(client)
def test_post_stores_traceparent_of_request_span(client, span_exporter)
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: `test_concurrent_posts_same_key`** — `ThreadPoolExecutor`
  20 równoległych `POST` z tym samym kluczem przez `TestClient` →
  dokładnie jeden `202`, reszta `200`. Uruchom, PASS.

- [ ] **Step 6: Lint + commit** (`feat: HTTP API with ordered checks and backpressure`)

---

### Task 11: Worker, serwer pomocniczy, CLI

**Files:**
- Create: `src/webhook_receiver/worker.py`,
  `src/webhook_receiver/worker_http.py`,
  `src/webhook_receiver/__main__.py`, `tests/unit/test_worker.py`,
  `tests/unit/test_worker_http.py`, `tests/unit/test_main.py`

**Interfaces:**
- Consumes: `EventRepository`, `Settings`, `Clock`, `simulate`,
  `SimulatedFailure`, `backoff_ms`, `WorkerMetrics`, `get_tracer`,
  `context_from_traceparent`, `RetentionPolicy`.
- Produces:
  - `class Worker` — `__init__(self, repo: EventRepository, settings:
    Settings, clock: Clock, metrics: WorkerMetrics, *, worker_id: str,
    rng: Random | None = None, sleep: Callable[[float], Awaitable[None]]
    = asyncio.sleep)`.
  - `async def run(self) -> None` — pętla do `stop()`: (1) co obrót
    `repo.recover_expired(max_attempts)` (w `asyncio.to_thread`; licznik
    `lease_recoveries`); (2) `free = concurrency − len(in_flight)`; jeśli
    `free > 0`: `claim(free, lease_ms)` i start zadania `process` dla
    każdego; (3) czekanie: jeśli ostatni claim zwrócił 0 lub brak wolnych
    slotów → `asyncio.wait(in_flight | {stop_event, poll_timer},
    return_when=FIRST_COMPLETED)`, gdzie `poll_timer` = `sleep(poll)`
    tylko gdy ostatni claim zwrócił 0; dzięki temu zwolnienie slotu od
    razu dobiera pracę (§9); (4) retencja co `retention_interval_seconds`
    (wg `clock`). Po `stop()`: brak nowych claimów, `asyncio.wait(in_flight,
    timeout=shutdown_grace_seconds)`, potem anulowanie pozostałych (ich
    lease wygaśnie).
  - `def stop(self) -> None`.
  - `@property def healthy(self) -> bool` — pętla działa i ostatni obrót
    < `10 × poll_interval_seconds` temu.
  - `async def process(self, event: Event) -> None` — span
    `webhook.process` z `context=context_from_traceparent(...)`, atrybuty
    `attempt`, `idempotency_key`, `source`, `event_type`, `worker.id`;
    `simulate(...)`; sukces → `complete(event.id, event.attempts,
    {"processed_by": worker_id, "duration_ms": …, "attempt": n})`; błąd →
    `record_failure` z `retry_at = now + backoff_ms(...)` gdy `attempts <
    max_attempts`, inaczej `None`; `False` z repo → `metrics.stale_result()`
    + log `event=stale_result`; metryki `attempt` i `processing_duration`.
  - `async def run_retention_once(self) -> PurgeResult`.
  - `worker_http.start_worker_http_server(port: int, worker: Worker) ->
    ThreadingHTTPServer` — `GET /health` → `200`/`503` wg
    `worker.healthy`; `GET /metrics` → `metrics_payload()`; inne → `404`;
    wątek daemon.
  - `__main__.main(argv: list[str] | None = None) -> int` — `api`:
    `setup_logging`, `setup_telemetry("webhook-api")`,
    `uvicorn.run(create_app(Settings()), host="0.0.0.0",
    port=settings.api_port, workers=1, log_config=None)`; `worker`:
    `check_sqlite_version`, pool, `init_schema`,
    `setup_telemetry("webhook-worker")`, serwer pomocniczy, `asyncio.run`
    z obsługą `SIGTERM`/`SIGINT` → `worker.stop()`. Nieznana komenda →
    kod 2.

- [ ] **Step 1: Testy (failing)**

Ustawienia testowe: `simulated_min/max_seconds=0.01/0.02`,
`lease_seconds=1`, `backoff_base_seconds=0.01`, `backoff_max_seconds=0.02`,
`poll_interval_seconds=0.01`, `worker_concurrency=4`. Zegar: `SystemClock`
(czasy są milisekundowe; testy trwają ułamki sekundy).

```python
async def test_processes_event_to_completed(worker_env)
async def test_fail_once_retries_then_completes(worker_env)
    # statusy przez GET: końcowo COMPLETED, attempts == 2
async def test_fail_always_ends_failed_after_max_attempts(worker_env)
    # max_attempts=3 → FAILED, attempts == 3, last_error zawiera "fail_always"
async def test_refills_slot_immediately(worker_env)
    # concurrency=1, poll_interval=5 s, 3 zdarzenia → wszystkie COMPLETED
    # w < 1 s (dowód, że dobieranie nie czeka na poll)
async def test_recovers_expired_lease_from_dead_worker(worker_env)
    # ręczny claim (symulacja padniętego workera), czekanie > lease →
    # nowy Worker kończy zdarzenie, attempts == 2
async def test_stale_completion_is_rejected_and_counted(worker_env)
async def test_stop_finishes_in_flight(worker_env)
    # simulated 0.3 s, start, po 0.05 s stop() → run() kończy się,
    # zdarzenie COMPLETED, brak nowych claimów po stop
async def test_process_span_is_child_of_post_span(worker_env, span_exporter)
async def test_retention_runs_on_interval(worker_env)

def test_worker_http_health_and_metrics(...)   # port 0, urllib
def test_main_unknown_command_returns_2()
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Lint + commit** (`feat: async worker with slot refill, fencing and retention`)

---

### Task 12: Testy integracyjne end-to-end

**Files:**
- Create: `tests/integration/__init__.py`,
  `tests/integration/test_end_to_end.py`

**Interfaces:**
- Consumes: `create_app` z `embedded_worker=True`, `Worker`, `main`.

- [ ] **Step 1: Testy (failing lub od razu zielone — wtedy zweryfikuj, że
  realnie przechodzą przez worker)**

```python
def test_gist_examples_complete_end_to_end(tmp_path)
    # TestClient(create_app(embedded_worker=True, szybka symulacja)),
    # 5 webhooków z gistu → polling GET do "completed" (timeout 5 s)
def test_duplicate_during_processing_not_reprocessed(tmp_path)
    # wynik (processed_by, attempt) niezmieniony po duplikacie
def test_restart_api_and_worker_preserves_events(tmp_path)
    # app1 bez workera: 3× POST → zamknięcie; app2 z workerem na tym samym
    # pliku → wszystkie completed
def test_separate_worker_process_shares_database(tmp_path)
    # subprocess: python -m webhook_receiver worker z env DB_PATH,
    # szybka symulacja, WORKER_METRICS_PORT=0; API w TestClient bez
    # workera; zdarzenie dochodzi do completed; SIGTERM → kod wyjścia 0
```

- [ ] **Step 2: Uruchom całość**

Run: `pytest -v`
Expected: PASS, pokrycie ≥ 85%.

- [ ] **Step 3: Lint + commit** (`test: end-to-end and restart scenarios`)

---

### Task 13: Docker, Compose, CI build

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `python -m webhook_receiver api|worker`, `python -m
  loadtest.run` (Task 14 — usługa `loadtest` może wskazywać na moduł
  dodany później).

- [ ] **Step 1: `Dockerfile`**

`FROM python:3.14-slim`; `pip install --no-cache-dir .[loadtest]`; kopiuje
`src/`, `loadtest/`, `benchmarks/`, `pyproject.toml`; użytkownik `app`
(UID 10001); `mkdir -p /data && chown app:app /data`; `ENV
DB_PATH=/data/webhooks.db PYTHONUNBUFFERED=1`; `USER app`; `EXPOSE 8000
9100`; `ENTRYPOINT ["python", "-m", "webhook_receiver"]`; `CMD ["api"]`.

- [ ] **Step 2: `docker-compose.yml`**

- `api`: build `.`, `command: ["api"]`, porty `"${API_PORT:-8000}:8000"`,
  wolumen `webhook-data:/data`, env `OTEL_EXPORTER_OTLP_ENDPOINT=
  http://jaeger:4318`, `OTEL_SERVICE_NAME=webhook-api`, healthcheck
  `["CMD", "python", "-c", "import urllib.request,sys;
  sys.exit(0 if urllib.request.urlopen('http://localhost:8000/ready',
  timeout=2).status == 200 else 1)"]`.
- `worker`: ten sam obraz, `command: ["worker"]`, `deploy.replicas: 3`,
  env `WORKER_CONCURRENCY=20`, wolumen, healthcheck jak wyżej na
  `localhost:9100/health`, `stop_grace_period: 20s`, bez `ports`.
- `jaeger`: `jaegertracing/all-in-one` z przypiętym tagiem (najnowszy
  stabilny), env `COLLECTOR_OTLP_ENABLED=true`, port
  `"${JAEGER_UI_PORT:-16686}:16686"`.
- `loadtest`: ten sam obraz, `profiles: ["loadtest"]`, `entrypoint:
  ["python", "-m", "loadtest.run"]`, env `BASE_URL=http://api:8000`.
- `volumes: webhook-data: {}`.

- [ ] **Step 3: Zweryfikuj w devcontainerze**

Run: `docker compose up -d --build && sleep 20 && docker compose ps`
Expected: `api` i 3× `worker` w stanie `healthy`, `jaeger` `running`.

Run: `curl -s -X POST localhost:8000/webhooks -H 'X-Source: shop' -H 'Content-Type: application/json' -d '{"idempotency_key":"evt-001","event_type":"order.created","payload":{"order_id":"ORD-2024-1234"}}' && sleep 6 && curl -s localhost:8000/webhooks/evt-001`
Expected: `202`, potem `"status":"completed"`; ślad w Jaegerze
(`localhost:16686`, usługa `webhook-api`) z dziećmi `webhook.process`.

Run: `docker compose stop worker` → `docker compose ps` pokazuje workery
`exited (0)` w < 20 s (graceful shutdown).

- [ ] **Step 4: Krok CI**

W `ci.yml` dodaj job `docker` (`needs: test`): `docker build -t
webhook-receiver .`.

- [ ] **Step 5: Commit + push; sprawdź zielony workflow**

Run: `gh run watch --exit-status`
Expected: oba joby zielone.

---

### Task 14: Test obciążeniowy i skrypt `scaling`

**Files:**
- Create: `loadtest/__init__.py`, `loadtest/stats.py`, `loadtest/run.py`,
  `scripts/scaling.sh`, `tests/loadtest/test_stats.py`

**Interfaces:**
- Produces:
  - `stats.percentile(values: list[float], p: float) -> float` (metoda
    nearest-rank).
  - `stats.parse_queue_depth(metrics_text: str) -> dict[str, float]` —
    wartości `webhook_queue_depth{status=…}`.
  - `stats.linearity(throughputs: dict[int, float]) -> dict[int, float]`
    — `t[n] / (n * t[1])`.
  - `run.main(argv)` — podkomendy:
    `steady` (`--rate 1000 --minutes 3 --drain-timeout 120`),
    `burst` (fazy z §13: 3000/min × 2 min; 100/min do kolejki ≤ 100,
    limit 600 s; 0 do kolejki = 0 łącznie z `processing`, limit 120 s;
    twardy limit całości 900 s),
    `flood` (`--count 3000 --rate 200 --warmup 10 --measure 120` →
    JSON `{"workers": N, "throughput_per_min": …}`; ponawia `429` po
    `Retry-After`),
    `scaling-report <plik.json>…` (tabela N / throughput / % liniowości,
    PASS gdy każdy ≥ 0.85).
    Wspólne: `--base-url` (domyślnie `$BASE_URL` lub
    `http://localhost:8000`), `--sources 5`, model otwarty (wysyłka wg
    harmonogramu, niezależnie od odpowiedzi), próbkowanie `/metrics` co
    1 s; raport: p50/p95/p99/max per faza, kody odpowiedzi, przebieg
    kolejki, czasy faz, PASS/FAIL (p99 < 100 ms, zero nie-2xx, limity);
    na końcu `steady`/`burst` — `GET` wszystkich wysłanych kluczy, każdy
    `completed`. Kod wyjścia ≠ 0 przy FAIL.
  - `scripts/scaling.sh` — dla N w 1 2 3: `docker compose down -v`,
    `docker compose up -d --build --scale worker=$N`, czekanie na
    healthy, `docker compose run --rm loadtest flood … > results/scaling-$N.json`
    (z polem `workers=N`), na końcu `docker compose run --rm -v
    ./results:/results loadtest scaling-report /results/scaling-*.json`.
    `set -euo pipefail`.

- [ ] **Step 1: Testy (failing)**

```python
def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4], 50) == 2
    assert percentile(list(range(1, 101)), 99) == 99
def test_parse_queue_depth():
    text = 'webhook_queue_depth{status="pending"} 12.0\n'
    assert parse_queue_depth(text)["pending"] == 12.0
def test_linearity():
    assert linearity({1: 340, 2: 680, 3: 900})[3] == pytest.approx(0.882, 0.01)
```

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Uruchom scenariusze (compose 3×20, ślady do Jaegera)**

Run: `docker compose up -d --build && docker compose run --rm loadtest steady && docker compose run --rm loadtest burst && bash scripts/scaling.sh`
Expected: wszystkie PASS. Zapisz raporty w `results/` (commitowane) —
źródło liczb do README. Przy FAIL: nie zmieniaj progów; zdiagnozuj
(`superpowers:systematic-debugging`) i opisz wynik.

- [ ] **Step 6: Lint + commit** (`feat: load test scenarios and scaling orchestration`)

---

### Task 15: Benchmarki SQLite

**Files:**
- Create: `benchmarks/__init__.py`, `benchmarks/sqlite_bench.py`,
  `tests/loadtest/test_bench_smoke.py`

**Interfaces:**
- Consumes: `connect`, `init_schema`, `ConnectionPool`, `EventRepository`,
  `RetentionPolicy`, `SystemClock`, `stats.percentile`.
- Produces: `sqlite_bench.main(argv)` — podkomendy (każda na pliku bazy w
  podanym katalogu `--dir`, `--repeat N`, raport mediana i p95 w ms):
  `sync` (1000 × `insert_or_get` przy `synchronous=FULL` i `NORMAL`),
  `claim` (`claim(20)` przy 0 / 10 000 / 100 000 zdarzeniach ready),
  `contention` (przepustowość tx/s przy 1/2/4/8 procesach
  `multiprocessing`, każdy `insert_or_get` przez 10 s),
  `retention` (czas jednej porcji 1000 przy `purge`), `all`.

- [ ] **Step 1: Test dymny (failing)**

```python
def test_benchmarks_smoke(tmp_path):
    assert main(["all", "--dir", str(tmp_path), "--repeat", "1",
                 "--quick"]) == 0
```

`--quick` zmniejsza rozmiary (np. 100 / 1 000 wierszy, 1 s), żeby test
trwał < 10 s.

- [ ] **Step 2: FAIL** → **Step 3: implementacja** → **Step 4: PASS**

- [ ] **Step 5: Uruchom pełne benchmarki**

Run: `docker compose run --rm --entrypoint python loadtest -m benchmarks.sqlite_bench all --dir /data/bench --repeat 5 | tee results/benchmarks.txt`
Expected: tabela 4 benchmarków; `claim` przy 100 000 nie rośnie
proporcjonalnie do rozmiaru (indeks).

- [ ] **Step 6: Lint + commit** (`feat: SQLite benchmarks`)

---

### Task 16: README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: wyniki z `results/`, spec §4, §7, §11, §12, §13, §13a, §15,
  §15a, §16, §17.

- [ ] **Step 1: Napisz README (po polsku)** — sekcje i obowiązkowe treści:
  badge CI; opis i architektura (diagram z §5); gwarancje „dokładnie raz”
  z restartem API (§4); uruchomienie: lokalnie (`pip install -e .[dev]`,
  `EMBEDDED_WORKER=true python -m webhook_receiver api`), Docker na
  „gołym” hoście (`docker compose up -d --build`, skalowanie `--scale
  worker=N`); `curl` na pięciu webhookach z gistu (`evt-001…005`) +
  duplikat + `fail_once`/`fail_always`; API (tabela kodów, kolejność
  kontroli); statusy (diagram §8); obserwowalność (metryki, Jaeger, logi,
  **jak sprawdzić liczbę workerów**: `docker compose ps`); przykładowe
  zapytania `sqlite3` (lista `failed`, zaległe, `datetime(col/1000,
  'unixepoch')`); tradeoffy — wszystkie punkty z listy §15 (readiness
  niezależne od workera, `synchronous=FULL` z liczbą z benchmarku 1,
  schemat, SQLite współdzielony przez kontenery, semantyka, retencja z
  GDPR i niemalejącym plikiem, jeden proces uvicorn, duplikat bez
  porównania treści, klucze `.`/`..`); założenia skalowalności (§16) z
  wynikami `steady`/`burst`/`scaling` i benchmarków (z opisem sprzętu);
  sekcja `# TODO` (lista z §17 + punkt o formacie `idempotency_key`).

- [ ] **Step 2: Weryfikacja**

Każde polecenie z README wykonane raz na czystym `docker compose down -v`;
wyniki zgodne z opisem.

- [ ] **Step 3: Commit + push** (`docs: README with architecture, tradeoffs and results`)

---

### Task 17 (opcjonalny): OpenTelemetry Collector

Decyzja „realizujemy / TODO” zapada po Task 16 (spec §12). Jeśli TAK:

**Files:**
- Create: `otel-collector.yaml`
- Modify: `docker-compose.yml`, `src/webhook_receiver/telemetry.py`,
  `README.md`

- [ ] **Step 1:** Collector (`otel/opentelemetry-collector-contrib`,
  przypięty tag): receiver OTLP, exporter `prometheus` (port 8889) i
  `otlp` do Jaegera; API i workery wysyłają metryki OTLP
  (`OTEL_METRICS_EXPORTER=otlp`); worker raportuje gauge
  `webhook_worker_up` = 1 z `service.instance.id`.
- [ ] **Step 2:** Test jednostkowy: przy `OTEL_METRICS_EXPORTER=otlp`
  `setup_telemetry` konfiguruje `PeriodicExportingMetricReader`.
- [ ] **Step 3:** Weryfikacja: `curl localhost:8889/metrics | grep -c
  webhook_worker_up` == 3 przy 3 workerach.
- [ ] **Step 4:** README — sekcja „ile workerów działa” zaktualizowana.
  Commit.

Jeśli NIE: dopisz Collector do `# TODO` w README i zakończ.
