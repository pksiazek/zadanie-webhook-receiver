# Webhook Receiver — zapis przebiegu brainstormingu

- **Data:** 2026-10-24 / 2026-10-25
- **Uczestnicy:** autor zadania (Piotr Książek), Claude (Opus 5.5) z procesem
  `superpowers:brainstorming`
- **Wynik:** [2026-10-25-webhook-receiver-design.md](2026-10-25-webhook-receiver-design.md)

Zapis odtwarza kolejne kroki rozmowy: co zostało zauważone, o co zapytano,
jaka padła odpowiedź i jaka decyzja z niej wynikła.

## 0. Rozpoznanie

- Źródło: gist z zadaniem rekrutacyjnym „Webhook Receiver Service” (gist
  zawiera też drugie zadanie — „Document Processing Service” — **poza
  zakresem**).
- Katalog roboczy pusty poza konfiguracją devcontainera: Python 3.12,
  `sqlite3`, port 8000, ruff. Projekt budowany od zera.

## 1. Zrozumienie zadania i pierwsze obserwacje

Odczytanie zadania: HTTP API przyjmujące webhooki JSON z `idempotency_key`,
zapis do SQLite, przetwarzanie asynchroniczne (symulacja `sleep(2–5 s)`),
brak utraty po restarcie, duplikaty zwracają istniejący status, endpoint
statusu, retencja, README + Dockerfile + TODO. Oceniający patrzą na sposób
myślenia.

Zgłoszone obserwacje:

1. „Dokładnie raz” to w praktyce **przyjęcie dokładnie raz** (`UNIQUE`) +
   **przetwarzanie co najmniej raz** z idempotentnym zapisem wyniku —
   uczciwe nazwanie tego w README to atut.
2. SQLite ma jednego pisarza naraz; przy ~17 webhookach/s z WAL to nie
   problem, ale warto to policzyć w README.
3. Otwarta kwestia: ten sam klucz, inny payload.

**Pytanie:** zwarty prototyp w 2–4 h czy pełny zakres z bonusami?

**Odpowiedź / decyzje:**

- Pełny zakres **z punktami bonusowymi**.
- Repozytorium na **GitHubie** (do utworzenia).
- **Pełny proces brainstormingu z zapisem do pliku** (ten dokument).
- Implementacja w **pełnej dyscyplinie superpowers**.

## 2. Narzędzia

**Pytanie:** preferencje co do frameworka i tego, czy worker to osobny
proces?

**Odpowiedź / decyzje:**

- Repozytorium **publiczne**, nazwa **`zadanie-webhook-receiver`**.
- **FastAPI** — akceptowane.
- Osobny proces workera — **do ustalenia** przy podejściach.

## 3. Jak pokazać retry

Obserwacja: symulacja `sleep` nigdy nie zawodzi, więc retry nie da się
zademonstrować bez wymuszania błędów. Opcje: losowy odsetek błędów vs
deterministyczny wyzwalacz w payloadzie.

**Pytanie:** jak oceniający ma zobaczyć działanie retry?

**Decyzja:** **deterministyczny wyzwalacz** — ułatwia testy.

Doprecyzowanie (zaakceptowane): dwa warianty —
`"simulate": "fail_once"` (pierwsza próba pada, retry się udaje) i
`"simulate": "fail_always"` (po wyczerpaniu prób stan `failed`).

## 4. Ten sam klucz, inny payload

Opcje: `409 Conflict` (głośny sygnał, wymaga hasha payloadu) vs zwrot
istniejącego statusu (dosłownie wg zadania, ale ciche gubienie zdarzenia).
Rekomendacja: 409.

**Decyzja:** **409 — chcemy dać znać** nadawcy.

## 5. Retencja

Obserwacja: okno retencji = okno ochrony przed duplikatami; usunięcie
rekordu sprawia, że spóźniony retry zostanie przetworzony ponownie.
Propozycja: dwa progi — payload i wynik krócej (dane ciężkie, potencjalnie
osobowe), klucz + hash + status dłużej; `failed` dłużej niż `completed`.

**Pytanie:** jak długo nadawcy realnie ponawiają webhooki?

**Decyzja:** **akceptacja propozycji** — konfigurowalne wartości domyślne
(payload 7 dni, klucz 30 dni), opisane jako założenie w README.

## 6. Podsumowanie zachowania i statusy

Przedstawione podsumowanie: przyjęcie (`202` nowe, istniejący status dla
duplikatu, `409` dla konfliktu), przetwarzanie async z wyzwalaczami,
retencja dwuprogowa; propozycje statusów, odzyskiwania po restarcie (lease)
i zawartości odpowiedzi statusu.

**Pytanie:** czego brakuje? Oraz: po czym identyfikować nadawcę do rate
limitingu (nagłówek `X-Source`, klucz API, IP)?

**Odpowiedź / decyzje:**

- Prośba o **dokładniejszy opis statusów — kiedy są przyjmowane**.
- Rate limiting: **`X-Source` jest OK**.

Doprecyzowanie statusów (przedstawione z diagramem):

- `pending` — w tej samej transakcji co zapis, przed `202`.
- `processing` — atomowa rezerwacja w jednym `UPDATE` z terminem wygaśnięcia.
- `completed` — wynik i status w jednej transakcji.
- `retrying` — błąd, zostały próby; `next_attempt_at` z backoffem
  wykładniczym.
- `failed` — błąd przy ostatniej próbie (domyślnie 5), stan końcowy.
- `processing → pending` — gdy rezerwacja wygaśnie (padnięcie/restart);
  próba się liczy.
- Propozycja: brak `X-Source` → `400`, przekroczony limit → `429` +
  `Retry-After`.

## 7. Obserwowalność

Propozycja: `/metrics` (Prometheus), logi JSON z kluczem i źródłem,
`/health` + `/ready`.

**Pytanie użytkownika:** czy do Pythona są biblioteki OpenTelemetry i czy
łatwo je podpiąć?

Odpowiedź: tak — `opentelemetry-api/sdk`, instrumentacja FastAPI (jedna
linia), instrumentacja `sqlite3`, eksportery OTLP i Prometheus. Wymaga
własnej pracy: propagacja kontekstu przez granicę async — zapis
`traceparent` w wierszu zdarzenia i odtworzenie go w workerze.

**Decyzje:**

- OpenTelemetry jako **główny mechanizm**.
- **`traceparent` — koniecznie.**
- Jeśli pełny stos to dużo pracy: eksport do konsoli, a postawienie
  pełnego oprogramowania do oglądania wyników → TODO.

Doprecyzowanie (przedstawione): domyślnie konsola; standardowa zmienna
`OTEL_EXPORTER_OTLP_ENDPOINT` przełącza na OTLP; Jaeger all-in-one w
`docker compose` (tani, ~10 linii); pełny stos Grafana/Tempo/Prometheus →
TODO.

## 8. Test obciążeniowy

Obserwacja: 1000/min ≈ 17/s; przy 2–5 s przetwarzania potrzeba ~60
równoległych slotów, inaczej kolejka rośnie — to wpływa na architekturę
workera.

**Pytanie:** co ma udowodnić test obciążeniowy?

**Decyzje:**

1. Że przyjmowanie **wytrzymuje 1000/min z niskim opóźnieniem**.
2. Że **powyżej przepustowości kolejkuje, a potem spokojnie schodzi**.

## 9. Podejścia architektoniczne

Wspólne dla wszystkich: SQLite jako magazyn **i** kolejka — wiersz `pending`
jest zadaniem, brak dual-write.

- **A.** Jeden proces (FastAPI + pula asyncio) — najprostsze, ale API i
  przetwarzanie dzielą pętlę zdarzeń i los; brak niezależnego skalowania.
- **B.** Osobne procesy `api` i `worker`, wspólny kod i plik SQLite,
  komunikacja wyłącznie przez bazę (claim z lease), heartbeat workera dla
  `/ready` — izoluje przyjmowanie od przetwarzania, pozwala skalować
  workery, daje prawdziwą granicę procesów dla `traceparent`; koszt: więcej
  elementów, opóźnienie pollingu, konkurencja o zapis, jeden host.
- **C.** Zewnętrzny broker (Redis/Celery) — odrzucone (zadanie narzuca
  SQLite, dual-write/outbox, dodatkowa infrastruktura); do TODO.

Rekomendacja: B (+ opcjonalny przełącznik do uruchomienia workera w procesie
API na potrzeby developmentu).

**Decyzja:** **B**.

## 10. Rozmiar i dalsze kroki

- Rozmiar: **projekt** → pełny dokument projektowy.
- Recenzja dokumentu (builder check) przez świeżego subagenta —
  **na życzenie użytkownika model Fable, effort high**.
- GitHub CLI nie jest zalogowane w środowisku — utworzenie zdalnego repo
  wymaga `gh auth login`.

## 11. Builder check

Recenzent (świeży subagent, model Fable, effort high) przeczytał dokument,
gist i devcontainer; empirycznie sprawdził, że SQLite WAL z 3 procesami
piszącymi i `synchronous=FULL` działa na tym systemie plików (~1300 tx/s).
Sesja recenzenta została raz przerwana (koniec sesji Claude Code) i
wznowiona z zachowanego zapisu.

### Pytania do użytkownika

1. **Docker nie istnieje w devcontainerze** — nie da się zweryfikować
   Dockerfile/compose/testu obciążeniowego. Omówione opcje:
   - `docker-outside-of-docker` — gniazdo demona hosta (OrbStack); w
     praktyce uprawnienia roota na hoście: dostęp do wszystkich kontenerów,
     obrazów, wolumenów i (przez OrbStack) plików macOS; porty na hoście
     (`host.docker.internal`).
   - `docker-in-docker` — osobny demon w kontenerze (wymaga
     `--privileged`); brak dostępu do zasobów hosta, wszystko potrzebne
     działa w środku; koszt: osobny cache obrazów.

   Rekomendacja zmieniona na docker-in-docker. **Decyzja: docker-in-docker.**

2. **Kryteria testu obciążeniowego.** Propozycja: p99 `POST` < 100 ms przy
   1000/min, zero nie-2xx, ślady do Jaegera podczas pomiaru.
   Użytkownik zaproponował bardziej złożony scenariusz burst:
   (1) 2 min po 3000, (2) potem 100 do zejścia kolejki do ~100,
   (3) czekanie na zero; poprosił o limity czasowe.
   Wyjaśniono dwuznaczność fazy 1 (3000 łącznie vs 3000/min) z wyliczeniem
   — przy 1 workerze (~1100/min) wariant 3000/min daje kolejkę ~3800 i
   czytelny spadek ~4 min. Zaproponowano limity: faza 2 ≤ 10 min,
   faza 3 ≤ 2 min, całość ≤ 15 min.
   **Decyzja: faza 1 = 3000/min przez 2 min; progi i limity zaakceptowane.**

### Rozstrzygnięte przez projektanta (oznaczone w dokumencie)

- `/ready` API nie zależy od workera (zgodnie z izolacją przyjmowania);
  stan workerów informacyjnie w ciele i metryce.
- Gauge'e kolejki wystawione na `/metrics` API — źródło dla testu
  obciążeniowego i dyżurnego; port metryk workera niepublikowany na hoście
  (konflikt z `--scale`).
- Konfigurowalna baza/limit backoffu (szybkie testy `fail_always`).
- Ograniczony zestaw znaków `idempotency_key` (klucz w ścieżce URL).
- Ujednolicone nazwy pól (`finished_at`, `payload_purged`).
- `stop_grace_period: 20s` > `SHUTDOWN_GRACE`.
- Nazwy instrumentów OTel bez sufiksów (eksporter Prometheus je dokleja).
- Jeden proces uvicorn dla API (stan limitera w pamięci).
- Jeden span `webhook.process` na próbę, dziecko spana żądania.

## 12. Przegląd decyzji projektanta przez użytkownika

1. Readiness API niezależne od workera — **OK, ale musi być jasno opisane
   w dokumentacji** → wymóg jawnej sekcji w README (§15).
2. Pytanie „o co chodzi z `--scale`?” — wyjaśnienie: `docker compose up
   --scale worker=N` uruchamia N kontenerów workera; stały port hosta dla
   metryk workera kolidowałby między replikami, stąd metryki workera tylko w
   sieci compose, a stan kolejki na `/metrics` API.
3. Unikalność klucza w obrębie nadawcy — **odrzucone: `idempotency_key` jest
   globalny.** Pomysł wziął się z ekstrapolacji „kilku systemów
   zewnętrznych” + rate limitingu per `X-Source`; zadanie tego nie mówi.
   `X-Source` służy tylko do rate limitingu i obserwowalności; `GET` go nie
   wymaga; duplikat z innego źródła o tej samej treści to zwykły duplikat
   (decyzja projektanta).
4. Ograniczony zestaw znaków klucza — **odrzucone („nie podoba mi się”).**
   Uzasadnienie ograniczenia (klucz w ścieżce URL) rozwiązane inaczej:
   dowolny string ≤ 255 znaków, kodowanie procentowe w ścieżce `GET`,
   `Location`/`status_url` zwracane zakodowane.
5. `synchronous=FULL` — **OK, dopisać decyzję do dokumentacji** → pełne
   uzasadnienie w §7 i wymóg w README.
6. Domyślne parametry — OK.
7. Jeden proces uvicorn — OK.

Powrót do formatu klucza: przedstawiono trzy warianty (dowolny string +
kodowanie procentowe; ograniczony zestaw znaków — ryzyko odrzucania
legalnych webhooków; klucz poza ścieżką). **Decyzja: zostaje obecny
wariant; w TODO punkt o doprecyzowaniu formatu `idempotency_key` i doborze
najlepszego rozwiązania.**

## 13. Przegląd schematu bazy

Pytania użytkownika: (1) wady i zalety osobnych `id` i `idempotency_key`,
(2) dlaczego wszystkie kolumny mają typ TEXT, (3) „wiele kolumn wydaje się
niepotrzebnych — przyjrzyj się dokładnie”.

Analiza (przedstawiona jako plan, zaakceptowana):

1. **`id` zostaje** jako wewnętrzny uchwyt: w SQLite `INTEGER PRIMARY KEY`
   to alias ukrytego `rowid` (bez dodatkowej kolumny i indeksu); indeks
   `UNIQUE` na kluczu istniałby także przy kluczu jako PK; `WITHOUT ROWID`
   odradzane dla dużych wierszy, a indeksy częściowe trzymałyby cały klucz.
2. **Typy:** czasy → `INTEGER` (epoch ms), `payload_hash` → `BLOB`,
   `status` zostaje TEXT + CHECK (czytelność), JSON zostaje TEXT, tabele
   `STRICT`. DDL zweryfikowany w SQLite 3.40.1 (STRICT odrzuca tekst w
   kolumnie liczbowej; zapytanie claimu używa indeksu częściowego).
3. **Kolumny:** usunięte `max_attempts`, `updated_at`,
   `payload_purged_at`, `lease_owner` (ogrodzenie numerem próby
   `attempts`); `next_attempt_at` + `lease_expires_at` scalone w `run_at`
   (wzorzec visibility timeout) — jeden indeks zamiast dwóch. 20 → 14
   kolumn.

Dodatkowe pytanie użytkownika: „przez co jest używana tabela
`worker_heartbeats`?” — odpowiedź: zapisuje każdy worker co 5 s, czyta
tylko API (`workers_alive` w `/ready`, metryka `webhook_workers_alive`);
po rozdzieleniu readiness od workera tylko informacyjna; jedyna wartość —
odróżnienie bezczynnego workera od martwego przy pustej kolejce.
**Decyzja: usunąć tabelę.** Awarię workera wykrywa healthcheck Dockera na
jego `/health`, brak postępu — `webhook_queue_oldest_pending_age_seconds`;
ograniczenie opisane w README.

## 14. Test obciążeniowy — skalowalność

Propozycja użytkownika: przeprojektować test tak, by były **3 workery po 20
„wątków”** — test ma udowadniać skalowalność.

Doprecyzowanie: worker używa równoległych slotów asyncio, nie wątków
(przetwarzanie to `sleep`); efekt dla testu ten sam. Wyliczenie: 3×20 =
60 slotów ≈ 1030/min (prawie jak dotychczasowe 1×64); `steady` ma ~3%
zapasu, `burst` — kolejka ~3900, spadek ~4 min, w limitach.

Uwaga: sam przebieg na 3×20 pokazuje, że konfiguracja wystarcza, a nie że
system się skaluje. Zaproponowano scenariusz `scaling`: N = 1, 2, 3 workery
po 20 slotów, zalana kolejka, pomiar tempa kończenia przez 2 min;
oczekiwane ≈ 340/690/1030 na min; kryterium ≥ 85% liniowości.

**Decyzja: domyślna konfiguracja compose i scenariusze `steady`/`burst`
na 3×20; scenariusz `scaling` zaakceptowany.**

## 15. Linter i CI

Uwaga użytkownika: o PEP 8 jest tylko krótka wzmianka — warto dodać twarde
wymaganie lintera.

Propozycja (zaakceptowana): ruff z regułami `E`, `W`, `N`, `F`, `I`, `B`,
`UP`; długość linii 79 (dosłownie PEP 8); `ruff check` + `ruff format
--check` bez błędów dla kodu, testów i skryptu obciążeniowego; zielony
linter warunkiem ukończenia każdego zadania.

Pytanie: gdzie egzekwować — tylko w procesie wytwarzania czy także w
GitHub Actions? **Decyzja: dodajemy też GitHub Actions** (lint + pytest +
`docker build`; test obciążeniowy poza CI).

## 16. Duplikaty — ta sama czy różna treść?

Pytanie użytkownika: czy z zadania wynika, że ten sam `idempotency_key` z
tą samą / inną treścią trzeba obsługiwać różnie?

Odpowiedź: nie — zadanie mówi tylko, że duplikat rozpoznajemy po
`idempotency_key` i zwracamy istniejący status. Rozróżnienie treści
(`409`) było dodatkiem projektanta z sekcji 4; jego koszt: `payload_hash`,
kanonizacja JSON, dodatkowa ścieżka API i testy. Przedstawione opcje:
zostawić `409` (rekomendacja), dosłownie wg zadania, wariant pośredni
(`200` + metryka niezgodności).

**Decyzja: dosłownie wg zadania** — duplikat = ten sam klucz, zawsze `200`
z istniejącym statusem. Usunięte: `409`, `payload_hash`, kanonizacja,
outcome `conflict`. Kompromis opisany w README; wykrywanie niezgodności
treści → TODO. (Zastępuje decyzję z sekcji 4.)

## 17. Monitoring — ile workerów działa?

Pytanie użytkownika: więcej o monitoringu i metrykach, w szczególności jak
sprawdzić, czy i ile workerów działa.

Przedstawiono źródła (API `/metrics`, worker `/metrics` na 9100 tylko w
sieci compose, Jaeger, logi, `/health`/`/ready`) i lukę: po usunięciu
heartbeatów liczby workerów nie widać w żadnej metryce — tylko
`docker compose ps` (healthcheck) i pośrednio
`queue_depth{processing}` pod obciążeniem. Opcje: OTel Collector w compose
(rekomendacja), przywrócenie heartbeatów, zostawić jak jest.

**Decyzja: OpenTelemetry Collector jako rozszerzenie albo TODO — do
decyzji w trakcie implementacji** (ostatnie, opcjonalne zadanie planu).

## 18. Testy jednostkowe jako twarde wymaganie

Pytanie użytkownika: czy jest zapis o konieczności testów jednostkowych?

Odpowiedź: częściowo (zakres bonusowy, TDD, lista obszarów, pytest w CI),
ale bez twardego wymagania na wzór lintera. Zaproponowano: testy
jednostkowe obowiązkowe dla każdego modułu z logiką, TDD, deterministyczne
(wstrzykiwany zegar), podział unit/integration, zielone testy warunkiem
ukończenia zadania i blokadą CI, próg pokrycia 85% (`pytest-cov`).

**Decyzja: zaakceptowane, próg 85%.**

## 19. Benchmarki

Pytanie użytkownika: czy mamy benchmarki, czy tylko testy obciążeniowe?

Odpowiedź: tylko testy obciążeniowe (zadanie: „testy obciążeniowe **lub**
benchmarki” — formalnie wystarcza). Benchmarki mierzą koszt pojedynczych
operacji i zamieniają założenia w liczby. Zaproponowano cztery:
`synchronous=FULL` vs `NORMAL`; claim przy kolejce 0/10k/100k; rywalizacja
o zapis przy 1/2/4/8 procesach; koszt porcji retencji. Ręcznie, poza CI,
wyniki w README.

**Decyzja: wszystkie cztery.**

## 20. SQLite współdzielony przez kilka kontenerów

Użytkownik przekazał treść nowego punktu do sekcji tradeoffów README (§15):
„SQLite współdzielony przez kilka kontenerów: ograniczenia”.

Zawartość: dlaczego to działa (wspólne jądro, blokady POSIX `fcntl`,
`-shm` przez `mmap`); warunki poprawności: tylko jeden host (bez NFS),
nazwany wolumen zamiast bind mounta, prawo zapisu do katalogu bazy (ten
sam obraz i UID), jeden pisarz naraz (sufit z benchmarku nr 3),
`busy_timeout` łagodzi chwilową rywalizację, poprawność logiczna
niezależna od liczby kontenerów (atomowy claim, ogrodzenie `attempts`);
stan weryfikacji: pomiar recenzenta w jednym kontenerze, wariant
wielokontenerowy potwierdzą scenariusz `scaling` i benchmark nr 3.

**Decyzja: dopisane do §15 jako obowiązkowy punkt README [ustalone].**
