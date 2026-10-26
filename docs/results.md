# Wyniki testu obciążeniowego i benchmarków

[← README](../README.md)

Pełne wyniki z katalogu [`results/`](results/) z interpretacją. Podsumowanie jest w [README](../README.md#wyniki).

## Środowisko

Wszystkie pomiary ([`results/`](results/),
[`results/environment.txt`](results/environment.txt)): Apple Silicon,
10 rdzeni, 11 GiB RAM, maszyna wirtualna Linux OrbStack na macOS
(aarch64), devcontainer z docker-in-docker, Docker 29.9.2, Compose 2.40.3,
obraz `python:3.14-slim-trixie` (Python 3.14.9, SQLite 3.46.1), wolumen
nazwany na btrfs. Ślady eksportowane do Jaegera podczas pomiarów.
Topologia: api × 1, worker × 3 × `WORKER_CONCURRENCY=20` (`steady`,
`burst`); worker × N = 1, 2, 3 (`scaling`). Liczby dotyczą tej maszyny —
na innym sprzęcie, zwłaszcza innym dysku, będą inne.

## `steady` — 1000/min przez 3 min ([`results/steady.txt`](results/steady.txt))

| Faza | Czas | Żądania | p50 | p95 | p99 | max | Kody |
|---|---|---|---|---|---|---|---|
| przyjmowanie | 179,9 s | 3000 | 8,4 ms | 13,1 ms | 19,3 ms | 50,1 ms | `202`: 3000 |
| schodzenie kolejki | 7,1 s (limit 120 s) | — | | | | | |

Szczyt kolejki (`pending`+`retrying`): 9; `processing` w fazie przyjmowania
55–60 (3 × 20 slotów). Kompletność: wysłano 3000, `completed` 3000.
Kryteria (p99 < 100 ms, zero odpowiedzi innych niż 2xx, limity faz, każdy
klucz `completed`): **PASS**.

## `burst` — 3000/min, potem 100/min, potem cisza ([`results/burst.txt`](results/burst.txt))

| Faza | Czas (limit) | Żądania | p50 | p95 | p99 | max | Kody |
|---|---|---|---|---|---|---|---|
| 1: 3000/min | 120,0 s (120 s) | 6000 | 6,8 ms | 10,9 ms | 17,0 ms | 47,2 ms | `202`: 6000 |
| 2: 100/min do kolejki ≤ 100 | 249,0 s (600 s) | 416 | 9,7 ms | 14,4 ms | 20,0 ms | 30,7 ms | `202`: 416 |
| 3: 0/min do kolejki = 0 | 12,0 s (120 s) | 0 | | | | | |

Całość 381,0 s (limit 900 s). Kolejka rosła do **3904** (t = 121 s), a
potem spokojnie schodziła — netto ok. 150–175 zdarzeń na ~10 s mimo
napływu 100/min — przy stałych 60 zdarzeniach w `processing`; przez cały czas opóźnienie `POST` się nie
zmieniło (p99 wszystkich żądań 17,1 ms). Kompletność: 6416 / 6416
`completed`. **PASS**. To jest główny argument za osobnymi procesami `api` i `worker`:
przyjmowanie nie zależy od tempa przetwarzania.

Opóźnienie planowania generatora (o ile później od planu wysłał żądanie)
wynosiło maks. 12,7 ms w `burst` i 7,1 ms w `steady` — zadane tempo było
utrzymane.

## `scaling` — przepustowość przetwarzania dla N workerów ([`results/scaling-report.txt`](results/scaling-report.txt))

Na czystej bazie: 3000 zdarzeń wysłanych tempem 200/s z 5 źródeł, potem
pomiar przez 120 s (okno 25–145 s) tempa kończenia zdarzeń.

| Workery (× 20 slotów) | Zdarzeń/min | Liniowo (N × wynik 1) | Liniowość | p99 `POST` | Wynik |
|---|---|---|---|---|---|
| 1 | 342,3 | 342,3 | 100,0% | 17,2 ms | PASS |
| 2 | 683,0 | 684,6 | 99,8% | 18,4 ms | PASS |
| 3 | 1025,8 | 1026,9 | 99,9% | 21,0 ms | PASS |

Kryterium ≥ 85% liniowości: **PASS** dla każdego N. Wyniki zgadzają się z
wyliczeniem 20 slotów / 3,5 s ≈ 340/min na worker. W logach workerów przy
każdym N: 0 × `database is locked`, 0 × `worker turn failed`, 0 ×
odzyskanych rezerwacji; CPU workera ok. 1–11% (`docker stats` w trakcie).
Przepustowość ogranicza tu liczba slotów, nie SQLite — patrz benchmark
rywalizacji poniżej.

## Benchmarki SQLite ([`results/benchmarks.txt`](results/benchmarks.txt))

Koszt pojedynczych operacji w izolacji, kod produkcyjny (`connect()`,
`EventRepository`), plik na dysku, 5 powtórzeń. W wierszach z n = 5 p95 =
maksimum.

**1. `synchronous=FULL` vs `NORMAL`** — transakcja zapisu zdarzenia
(`insert_or_get`):

| Tryb | Tempo | mediana | p95 | p99 |
|---|---|---|---|---|
| FULL | bez przerw (1967 tx/s seryjnie) | 0,442 ms | 0,892 ms | 1,532 ms |
| FULL | 100 tx/s | **1,998 ms** | 2,899 ms | 5,537 ms |
| NORMAL | bez przerw (22073 tx/s seryjnie) | 0,023 ms | 0,048 ms | 0,065 ms |
| NORMAL | 100 tx/s | 0,195 ms | 0,291 ms | 0,423 ms |

Realistyczny koszt to wiersz z tempem 100 tx/s: **ok. 2 ms na zapis** z
fsync (bez przerw zapisy korzystają z „rozgrzanego” urządzenia, stąd
0,44 ms). `NORMAL` jest o rząd wielkości tańszy, a mimo to zostaje `FULL`
— uzasadnienie w [kompromisach](tradeoffs.md#synchronousfull-zamiast-normal). Zastrzeżenie: w maszynie
wirtualnej OrbStack na macOS fsync może nie wymuszać zapisu na nośnik tak
jak `F_FULLFSYNC`, więc liczby dla `FULL` mogą być optymistyczne.

**2. Rezerwacja przy rosnącej kolejce** — `claim(20)`:

| Kolejka należnych | claim(20) mediana / p95 | samo zapytanie (bez `COMMIT`) | `COMMIT` mediana |
|---|---|---|---|
| 0 | 0,009 / 0,011 ms | 0,005 ms | 0,001 ms |
| 10 000 | 0,477 / 0,639 ms | 0,045 ms | 0,375 ms |
| 100 000 | 0,466 / 0,619 ms | 0,046 ms | 0,365 ms |

Koszt rezerwacji **nie zależy od długości kolejki** do 100 000 (= domyślny
`MAX_BACKLOG`): samo zapytanie to ok. 0,05 ms, reszta to commit z `FULL`.
Plan: `SEARCH … USING COVERING INDEX ix_events_ready`. `recover_expired`
kosztuje 0,009 ms (p95 0,012 ms), ale tylko gdy żadna rezerwacja nie wygasła
(60 ważnych rezerwacji, nic do zapisu); odzyskanie, które coś zapisuje,
płaci dodatkowo za commit.

**3. Rywalizacja o zapis** — 1/2/4/8 procesów piszących bez przerw przez
10 s:

| Procesy | Łącznie tx/s (mediana / maks.) | Na proces: mediana (min–maks.) | Najdłuższa transakcja | `SQLITE_BUSY` (5 przebiegów) |
|---|---|---|---|---|
| 1 | 2314,2 / 2396,7 | 2314 (2209–2397) | 18 ms | 0 |
| 2 | 2305,3 / 2416,4 | 1103 (148–1613) | 5082 ms | 4 |
| 4 | 2279,9 / 2373,9 | 450 (0–1759) | 5141 ms | 9 |
| 8 | 2219,5 / 2237,4 | 68 (0–2037) | 5221 ms | 26 |

Wnioski:

- **Pułap ok. 2200–2300 tx/s łącznie, niezależnie od liczby procesów** —
  SQLite ma jednego pisarza naraz, więcej procesów nie dodaje
  przepustowości zapisu.
- **Blokada nie jest sprawiedliwa:** przy nasyceniu część procesów
  „głoduje” (min. 0 tx/s), czeka pełny `busy_timeout` (najdłuższa
  transakcja ok. 5,1–5,2 s) i dostaje `SQLITE_BUSY`. Stąd decyzja: `POST`
  mapuje `SQLITE_BUSY`/`SQLITE_LOCKED` na `503` + `Retry-After`, a nie
  `500`; worker przeżywa taki błąd (log, odzyskanie rezerwacji).
  `busy_timeout` celowo nie został podniesiony — wydłużyłby tylko ogon
  opóźnień.
- To pomiar przy nasyceniu. Ogon opóźnień rośnie dużo wcześniej niż przy
  pułapie, więc użyteczny pułap przy p99 < 100 ms jest znacznie niższy niż
  2200 tx/s (nie mierzyliśmy go osobno).
- Test obciążeniowy to ≤ 3 transakcje zapisu na zdarzenie (zapis,
  rezerwacja — do 20 zdarzeń naraz, zakończenie), czyli przy ~17
  zdarzeniach/s **≤ ~50 tx/s, ok. 2% pułapu** (szacunek). To tłumaczy liniowe
  skalowanie i zero błędów blokady w `scaling`.

**4. Retencja** — 10 000 kwalifikujących się + 100 000 świeżych wierszy:

| Operacja | n | mediana | p95 |
|---|---|---|---|
| czyszczenie payloadu, porcja 1000 | 50 | 1,815 ms | 1,989 ms |
| usuwanie, porcja 1000 | 50 | 2,011 ms | 6,526 ms |
| `purge()` 10 000 wierszy (20 porcji) | 5 | 45,326 ms | 48,326 ms |

Porcja 1000 trzyma blokadę zapisu ok. 2 ms, a każda porcja to osobna
transakcja, więc zapis zdarzeń przeplata się z retencją. `RETENTION_BATCH_SIZE`
zostaje 1000 (także gdy trzy workery uruchomią retencję jedna po drugiej).
Wszystkie zapytania retencji, rezerwacji i odzyskiwania używają indeksów
(`EXPLAIN QUERY PLAN` w pliku wyników).

**Czyszczenie payloadu przy wielu już wyczyszczonych wierszach** — 10 000
kwalifikujących się + 100 000 wierszy `completed` w wieku 7–30 dni bez
payloadu (taki stan ma tabela po tygodniu ruchu):

| Porcja | przed (`ix_events_finished`) | po (`ix_events_purgeable`) |
|---|---|---|
| porcja 1000, mediana / p95 | 7,817 / 12,528 ms | 1,801 / 14,978 ms |
| ostatnia, pusta porcja | 6,561 ms | 0,010 ms |

- Przed poprawką każda porcja przechodziła przez wszystkie wyczyszczone
  wiersze — koszt rósł liniowo z ich liczbą (ok. 7 ms na 100 000; przy
  docelowym ruchu ~33 mln wierszy to ok. 2 s blokady zapisu na porcję).
- Indeks `ix_events_purgeable` zawiera tylko wiersze z payloadem, więc
  mediana porcji jest taka sama jak bez wyczyszczonych wierszy (ok. 1,8 ms),
  a pusta porcja kosztuje 0,01 ms.
- p95 14,978 ms po poprawce to jedna wolna porcja na przebieg (5 z 50
  próbek), nie przeszukiwanie. Spowalnia ją automatyczny checkpoint WAL po
  przygotowaniu danych (masowe czyszczenie 100 000 wierszy); po ręcznym
  `PRAGMA wal_checkpoint` odstająca porcja znika.
- Kolumna „przed” pochodzi z osobnego przebiegu (`retention --repeat 5`,
  ten sam benchmark na kodzie sprzed poprawki); nie ma jej w pliku wyników.
