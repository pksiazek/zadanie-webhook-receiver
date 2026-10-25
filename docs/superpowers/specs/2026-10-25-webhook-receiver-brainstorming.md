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

_(uzupełniane po recenzji)_
