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
- Ciche połknięcie konfliktu (ten sam klucz, inna treść).
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
  zapis i decyzja „nowe/duplikat/konflikt” w jednej transakcji.
- **Wykonanie co najmniej raz:** jeśli worker padnie w trakcie, rezerwacja
  (lease) wygasa i zdarzenie jest podejmowane ponownie.
- **Efekt dokładnie raz:** zapis wyniku jest warunkowy (tylko właściciel
  aktualnej rezerwacji, tylko ze stanu `processing`), więc spóźniony worker
  z wygasłą rezerwacją nie nadpisze wyniku. Prawdziwy efekt uboczny musiałby
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
                                                       │ heartbeat, retencja
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
| duplikat, identyczna treść | `200 OK` | aktualny status (jak w `GET`), pole `duplicate: true` |
| ten sam klucz, inna treść | `409 Conflict` [ustalone] | opis konfliktu, aktualny status oryginału |
| brak `X-Source` | `400` | opis błędu |
| przekroczony limit nadawcy | `429` + `Retry-After` [ustalone] | opis |
| przekroczony próg zaległości (backpressure) | `503` + `Retry-After` [decyzja projektanta] | opis |

„Ta sama treść” = równy hash SHA-256 kanonicznego JSON-a (posortowane klucze,
bez białych znaków) z pól `event_type` + `payload` [decyzja projektanta].

`idempotency_key` jest **globalnie unikalny** [ustalone]. `X-Source`
służy wyłącznie do rate limitingu i obserwowalności (metryki, logi,
atrybuty spanów) — nie jest częścią tożsamości zdarzenia ani hasha. W
rekordzie zapisujemy źródło pierwszego zgłoszenia; ponowienie z innego
źródła z identyczną treścią to zwykły duplikat (`200`) [decyzja
projektanta].

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
  "updated_at": "...",
  "finished_at": null,
  "payload_purged": false
}
```

`finished_at` odpowiada kolumnie o tej samej nazwie (moment wejścia w
`completed` lub `failed`); `payload_purged` = `payload_purged_at IS NOT
NULL`. Nieznany klucz → `404`.

### Endpointy operacyjne

- `GET /health` — liveness: proces odpowiada.
- `GET /ready` — readiness API: baza dostępna (zapytanie) i schemat
  zainicjalizowany; inaczej `503` z listą niespełnionych warunków.
  **Brak workera nie wpływa na gotowość API** — przyjmowanie ma działać
  niezależnie od przetwarzania (to cel architektury B). Stan workerów jest
  raportowany informacyjnie w ciele odpowiedzi (`workers_alive`, wiek
  najświeższego heartbeatu) oraz metryką [ustalone — wymaga jawnego opisu
  w README, patrz §15].
- `GET /metrics` — metryki w formacie Prometheus (eksporter OpenTelemetry),
  **w tym gauge'e kolejki** (`webhook_queue_depth{status}`,
  `webhook_queue_oldest_pending_age_seconds`, `webhook_workers_alive`)
  liczone z tego samego cache co backpressure. To jedno miejsce, z którego
  dyżurny i test obciążeniowy czytają stan kolejki, niezależnie od liczby
  workerów [decyzja projektanta, po recenzji].

## 7. Model danych (SQLite)

```sql
CREATE TABLE webhook_events (
    id               INTEGER PRIMARY KEY,
    source           TEXT    NOT NULL,   -- X-Source pierwszego zgłoszenia
    idempotency_key  TEXT    NOT NULL UNIQUE,
    event_type       TEXT    NOT NULL,
    payload_json     TEXT,               -- NULL po wyczyszczeniu (retencja)
    payload_hash     TEXT    NOT NULL,   -- zostaje do wykrywania konfliktów
    status           TEXT    NOT NULL CHECK (status IN
                       ('pending','processing','retrying','completed','failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    next_attempt_at  TEXT    NOT NULL,   -- kiedy wolno podjąć
    lease_owner      TEXT,               -- id workera + token rezerwacji
    lease_expires_at TEXT,
    last_error       TEXT,
    result_json      TEXT,
    traceparent      TEXT,               -- W3C trace context z żądania POST
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    finished_at      TEXT,               -- completed lub failed
    payload_purged_at TEXT
);

-- pobieranie pracy: tylko wiersze gotowe do podjęcia
CREATE INDEX ix_events_ready ON webhook_events (next_attempt_at)
    WHERE status IN ('pending', 'retrying');
-- odzyskiwanie porzuconych rezerwacji
CREATE INDEX ix_events_lease ON webhook_events (lease_expires_at)
    WHERE status = 'processing';
-- retencja
CREATE INDEX ix_events_finished ON webhook_events (status, finished_at)
    WHERE finished_at IS NOT NULL;

CREATE TABLE worker_heartbeats (
    worker_id   TEXT PRIMARY KEY,
    last_seen_at TEXT NOT NULL,
    started_at  TEXT NOT NULL
);
```

Czas: ISO 8601 UTC [dla wykonawcy: format/typ, byle porównywalny
leksykograficznie lub liczbowo w SQL].

Ustawienia połączenia: `journal_mode=WAL`, `busy_timeout=5000`,
`foreign_keys=ON` oraz **`synchronous=FULL`** [ustalone — decyzja
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
       lease wygasła,     │                   │ └─błąd, próby < max──► retrying ──(next_attempt_at)──claim──► processing
       próby < max ───────┘                   │
                                              └─błąd lub lease wygasła, próby = max──► failed
```

| Przejście | Kiedy dokładnie |
|---|---|
| → `pending` | w transakcji `INSERT` nowego zdarzenia, przed odpowiedzią `202`; `next_attempt_at = now` |
| `pending`/`retrying` → `processing` | worker atomowo rezerwuje: jeden `UPDATE … WHERE id IN (SELECT … ORDER BY next_attempt_at LIMIT k) RETURNING *`; ustawia `lease_owner`, `lease_expires_at = now + LEASE`, `attempts += 1` |
| `processing` → `completed` | symulacja zakończona sukcesem; `UPDATE … WHERE id = ? AND status = 'processing' AND lease_owner = ?` zapisuje wynik, status i `finished_at` w jednej instrukcji |
| `processing` → `retrying` | symulacja rzuciła błąd i `attempts < max_attempts`; `next_attempt_at = now + backoff(attempts)`; `last_error` zapisany |
| `processing` → `failed` | błąd przy `attempts = max_attempts`; stan końcowy |
| `processing` → `pending` | rezerwacja wygasła (worker padł / restart) i `attempts < max_attempts`; wykonywane przez worker przy pobieraniu pracy |
| `processing` → `failed` (wygasła) | rezerwacja wygasła przy `attempts = max_attempts`; `last_error = "lease expired"` |

Parametry domyślne (konfigurowalne) [decyzja projektanta]:
`MAX_ATTEMPTS=5`, `LEASE_SECONDS=30` (> maks. czasu symulacji 5 s),
backoff `min(BACKOFF_BASE · 2^(attempts−1), BACKOFF_MAX)` z jitterem ±20%,
`BACKOFF_BASE=2 s`, `BACKOFF_MAX=60 s`. Baza i limit backoffu są
konfigurowalne, żeby testy i demo `fail_always` nie trwały ~50 s.

Duplikat w dowolnym stanie dostaje aktualny status.

## 9. Worker

- Pętla asyncio z pulą `WORKER_CONCURRENCY` slotów (domyślnie 64 —
  przy 17 zdarzeniach/s × średnio 3,5 s ≈ 60 równoległych) [decyzja
  projektanta].
- Pobiera pracę porcjami (tyle, ile wolnych slotów); gdy brak pracy — śpi
  `POLL_INTERVAL` (0,5 s); gdy pracy jest dużo — dobiera od razu.
- Operacje SQLite wykonywane poza pętlą zdarzeń (`asyncio.to_thread` lub
  dedykowany wątek) [dla wykonawcy].
- Heartbeat do `worker_heartbeats` co 5 s.
- Graceful shutdown (SIGTERM): przestaje pobierać, czeka na trwające zadania
  do `SHUTDOWN_GRACE` (10 s); niedokończone odzyskają się po wygaśnięciu
  rezerwacji. W compose `stop_grace_period: 20s` (> `SHUTDOWN_GRACE`), żeby
  Docker nie zabił workera SIGKILL-em w trakcie kończenia.
- Zadanie retencji uruchamiane periodycznie w workerze (co 10 min) — patrz
  §11. Przy wielu workerach każdy może je uruchomić; operacja jest
  idempotentna, więc to bezpieczne.
- Pomocniczy serwer HTTP na `WORKER_METRICS_PORT` (domyślnie 9100) z
  `/metrics` (metryki procesu workera: czasy przetwarzania, wyniki prób) i
  `/health` — dostępny w sieci compose, **niepublikowany na hoście**, żeby
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
| `payload_json` i `result_json` zdarzeń `completed` | 7 dni od `finished_at` | ustawiane na `NULL`, `payload_purged_at` ustawione |
| cały wiersz `completed` (klucz + hash + status) | 30 dni od `finished_at` | wiersz usuwany |
| cały wiersz `failed` (z payloadem — do diagnozy) | 30 dni od `finished_at` | wiersz usuwany |
| heartbeaty nieaktywnych workerów | 1 dzień | usuwane |

Zdarzenia niezakończone nigdy nie są usuwane.

Konsekwencje (opisane w README): okno retencji klucza = okno ochrony przed
duplikatami. Duplikat po wyczyszczeniu payloadu nadal jest rozpoznawany
(hash zostaje), dostaje status bez wyniku (`payload_purged: true`). Po
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
  - `webhooks_received_total{source, outcome=accepted|duplicate|conflict|rate_limited|backpressure|invalid}`
  - `webhook_processing_duration_seconds` (histogram), `{outcome}`
  - `webhook_attempts_total{outcome=success|retry|failed}`
  - `webhook_queue_depth{status}` (gauge odczytywany z bazy)
  - `webhook_queue_oldest_pending_age_seconds` (gauge — najważniejszy sygnał
    „kolejka nie nadąża”)
  - `webhook_lease_recoveries_total`
  - `webhook_workers_alive` (gauge z tabeli heartbeatów)
  - Uwaga implementacyjna: eksporter Prometheus z OTel sam dokleja `_total`
    do liczników i jednostkę do histogramów — instrumenty nazywamy bez
    sufiksów (`webhooks_received`, `webhook_processing_duration` z jednostką
    `s`), żeby na `/metrics` wyszły nazwy jak wyżej. Pakiety
    instrumentacji (`0.xxbN`) przypięte do pasującej wersji SDK.
  - Gauge'e kolejki na `/metrics` API (patrz §6), metryki przetwarzania na
    `/metrics` workera.
- **Logi:** strukturalny JSON na stdout, w każdym wpisie `trace_id`,
  `span_id`, `idempotency_key`, `source`; zdarzenia: przyjęcie, duplikat,
  konflikt, claim, wynik próby, odzyskanie rezerwacji, retencja.
- **Eksport:** domyślnie konsola (span exporter do stdout) — działa bez
  infrastruktury [ustalone]. Ustawienie `OTEL_EXPORTER_OTLP_ENDPOINT`
  przełącza na OTLP. `docker compose` zawiera Jaeger all-in-one (UI
  `localhost:16686`), o ile to tanie; jeśli okaże się kosztowne — zostaje
  konsola, a Jaeger trafia do TODO [ustalone].

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

Środowisko: `docker compose` (api + 1 worker + Jaeger) w devcontainerze z
docker-in-docker; podczas pomiaru ślady eksportowane do Jaegera (OTLP), nie
do konsoli, bo wypisywanie spanów na stdout zawyża p99 [ustalone].

Oczekiwana przepustowość przetwarzania 1 workera: 64 sloty / 3,5 s ≈
18/s ≈ 1100/min.

**Scenariusz `steady`** [ustalone]: 1000/min przez 3 min.
Kryteria: p99 opóźnienia `POST` < 100 ms, zero odpowiedzi innych niż 2xx.

**Scenariusz `burst`** [ustalone]:

| Faza | Ruch | Koniec fazy | Oczekiwane | Limit (porażka po przekroczeniu) |
|---|---|---|---|---|
| 1 | 3000/min (6000 żądań) | po 2 min | kolejka rośnie do ~3800; p99 `POST` < 100 ms, zero nie-2xx | stałe 2 min |
| 2 | 100/min | kolejka (`pending`+`retrying`) ≤ ~100 | ~3,5–4 min | 10 min |
| 3 | 0 | kolejka = 0 (łącznie z `processing`) | kilka sekund | 2 min |
| całość | | | ~6 min | 15 min (twardy) |

Na końcu każdego scenariusza: weryfikacja, że **wszystkie wysłane klucze
mają status `completed`**.

Raport: p50/p95/p99 i maks. opóźnienia `POST` per faza, rozkład kodów
odpowiedzi, przebieg głębokości kolejki w czasie, czasy trwania faz,
wynik kryteriów (PASS/FAIL). Wyniki z przebiegu (z opisem sprzętu) trafiają
do README; dodatkowo przebieg `burst` z `--scale worker=2` pokazujący
~dwukrotnie szybsze schodzenie kolejki [decyzja projektanta].

## 14. Testy

pytest; TDD w trakcie implementacji [ustalone]. Kluczowe obszary:

- repozytorium: zapis nowego / duplikat / konflikt; atomowy claim (dwóch
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

- Kod (pakiet Pythona, `pyproject.toml`, ruff do formatowania/lintu).
- `Dockerfile` (jeden obraz, dwie komendy: `api` i `worker`).
- `docker-compose.yml`: `api`, `worker`, `jaeger`; nazwany wolumen z plikiem
  bazy; healthcheck API na `/ready`; `stop_grace_period: 20s` dla workera.
- Zmiana devcontainera: feature `docker-in-docker`.
- README: architektura, gwarancje (§4), tradeoffy, uruchomienie (lokalnie i
  w Dockerze), przykładowe wywołania `curl`, wyniki testu obciążeniowego,
  założenia skalowalności, sekcja **TODO**. Sekcja tradeoffów musi jawnie
  opisywać co najmniej [ustalone]:
  - **Readiness API niezależne od workera** — co oznacza dla operatora
    (`/ready` zielone przy niedziałającym workerze; jak wtedy wykryć
    problem: `webhook_workers_alive`, `webhook_queue_oldest_pending_age_seconds`,
    pole `workers_alive` w `/ready`) i dlaczego tak wybrano.
  - **`synchronous=FULL`** — dlaczego nie `NORMAL`, z pomiarem kosztu.
  - Semantykę „dokładnie raz” (§4), dwuprogową retencję (§11), jeden
    proces uvicorn (§3), granicę jednego hosta (§16).
- Ten dokument i zapis brainstormingu w `docs/superpowers/specs/`.

## 16. Założenia skalowalności (do README)

- 1000/min ≈ 17/s; przy średnio 3,5 s przetwarzania potrzeba ~60
  równoległych slotów → jeden worker z 64 slotami; więcej przez `--scale`.
- Zapis do SQLite w WAL: jeden pisarz naraz; przy ~17 INSERT/s +
  porównywalnej liczbie UPDATE workerów jest duży zapas — weryfikujemy
  pomiarem, łącznie z kosztem `synchronous=FULL`.
- Granica: jeden host. Dalej: PostgreSQL (`SELECT … FOR UPDATE SKIP LOCKED`)
  lub broker z outboxem (→ TODO).

## 17. Pozostawione wykonawcy

- Struktura pakietów i nazwy modułów.
- Biblioteka do konfiguracji (np. `pydantic-settings`) i do logów JSON.
- Sposób wykonywania operacji SQLite poza pętlą zdarzeń.
- Dokładny format czasu w bazie.
- Szczegóły skryptu testu obciążeniowego i formatu raportu.
- Treść sekcji TODO w README (minimum: HMAC, współdzielony rate limit,
  pełny stos obserwowalności, PostgreSQL/broker, narzędzie migracji,
  endpoint do ręcznego ponowienia `failed`, dead-letter/alerty).
  Obowiązkowo także punkt [ustalone]: **doprecyzować z nadawcami format
  `idempotency_key`** (zestaw znaków, długość, struktura) i na tej podstawie
  dobrać najlepsze rozwiązanie — obecne (dowolny string + kodowanie
  procentowe w ścieżce), ograniczony zestaw znaków albo klucz poza ścieżką
  (parametr zapytania / wewnętrzny identyfikator).
