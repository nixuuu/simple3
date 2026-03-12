# Pisanie wydajnego kodu w Rust – checklist do code review

## Ownership i borrowing
- unikaj niepotrzebnych `.clone()`, `.to_owned()`, `.to_string()`
- przekazuj referencje `&T` / `&mut T` tam, gdzie to możliwe
- korzystaj z `Cow<'_, T>` zamiast klonowania w sytuacjach „może potrzebuję własności, może nie"
- nie trzymaj dużych struktur przez ownership, gdy wystarczy referencja
- używaj `Arc<T>` w kodzie wielowątkowym, `Rc<T>` tylko w single-threaded
- rozważ `Box<dyn Trait>` vs generics — dynamic dispatch zmniejsza rozmiar binarki, ale kosztuje wydajność

## Wybór struktur danych
- używaj `Vec` do sekwencyjnego dostępu i operacji push/pop z końca
- używaj `VecDeque` do operacji z obu końców
- używaj `HashMap` do szybkiego wyszukiwania po kluczu (średnio O(1))
- używaj `BTreeMap` / `BTreeSet` gdy potrzebny jest porządek lub zakresy
- unikaj wyszukiwania liniowego (`find`, `position`) na dużych kolekcjach
- dla małych kolekcji (do ~20 elementów) `Vec` + liniowe wyszukiwanie bywa szybsze niż `HashMap`

## Iteratory vs pętle ręczne
- preferuj chain iteratorów: `map` → `filter` → `collect` / `for_each` / `fold`
- unikaj pętli `for i in 0..len` z indeksowaniem, gdy da się to zrobić iteratorem
- nie twórz `.collect::<Vec<_>>()` tylko po to, żeby potem iterować po wektorze
- używaj `Iterator::size_hint()` i `ExactSizeIterator` gdy implementujesz własne iteratory

## Równoległe przetwarzanie (rayon)
- używaj `par_iter()` / `par_iter_mut()` / `into_par_iter()` dla CPU-bound operacji na dużych kolekcjach
- zamiana `.iter()` na `.par_iter()` ma sens gdy:
  - kolekcja ma > 1000 elementów lub
  - pojedyncza iteracja trwa > 1μs
- nie używaj rayon dla operacji I/O-bound — tam lepszy async (tokio, async-std)
- unikaj `par_iter()` gdy:
  - operacja wymaga zachowania kolejności i używasz `for_each` (użyj `par_bridge()` + `collect()` jeśli kolejność ważna)
  - kolekcja jest mała (overhead thread pool przewyższa zysk)
  - wewnątrz iteracji są alokacje lub locki (kontencja zniweluje zysk)
- preferuj `par_chunks()` / `par_chunks_mut()` dla przetwarzania dużych danych w partiach
- dla redukcji używaj `par_iter().reduce()` lub `par_iter().sum()` zamiast ręcznego zbierania wyników
- ustaw rozmiar thread pool jawnie przez `rayon::ThreadPoolBuilder` gdy domyślny (liczba CPU) nie jest optymalny
- profiluj przed i po — nie wszystkie pętle zyskują na równoległości

## Alokacje i zarządzanie pamięcią
- minimalizuj alokacje wewnątrz hot paths (fragmentów kodu wykonywanych wielokrotnie w pętlach lub na krytycznej ścieżce)
- używaj `&str` zamiast `String` w argumentach funkcji (gdy nie modyfikujesz)
- preferuj `String::with_capacity()` + `push_str` zamiast wielokrotnego `+=`
- unikaj `format!` w pętlach — buduj `String` ręcznie lub użyj `write!` do istniejącego bufora
- dla małych wektorów rozważ `smallvec`, `tinyvec`
- dla małych stringów rozważ `smol_str`, `compact_str`, `smartstring`

## Async
- używaj `tokio::spawn` dla zadań async, `spawn_blocking` dla blokującego CPU/IO
- unikaj `.await` w pętlach gdy da się użyć `join_all`, `try_join_all` lub `FuturesUnordered`
- nie trzymaj `MutexGuard` przez `.await` — użyj `tokio::sync::Mutex` lub przeorganizuj kod
- preferuj `select!` z `biased` gdy kolejność sprawdzania ma znaczenie

## Bezpieczeństwo i panika
- używaj `unwrap()` / `expect()` tylko w testach i `main`
- w bibliotekach i kodzie produkcyjnym zwracaj `Result` / `Option`
- unikaj `panic!` na hot paths (koszt unwind jest wysoki)
- bloki `unsafe` trzymaj małe, dobrze udokumentowane i uzasadnione
- oznaczaj funkcje zwracające wartość, która nie powinna być ignorowana, atrybutem `#[must_use]`

## Obsługa błędów
- w bibliotekach definiuj własne typy błędów (`thiserror`)
- w aplikacjach używaj `anyhow` dla uproszczonej propagacji błędów
- unikaj `Box<dyn Error>` jako typu zwracanego w publicznym API
- nie nadużywaj `.unwrap_or_default()` — jawnie obsługuj przypadki brzegowe

## SQLx i baza danych
- używaj makr `sqlx::query!()`, `sqlx::query_as!()`, `sqlx::query_scalar!()` zamiast runtime `query()` / `query_as()`
- compile-time verification wymaga ustawionej zmiennej `DATABASE_URL` podczas kompilacji
- dla CI bez dostępu do bazy używaj offline mode:
  - generuj cache: `cargo sqlx prepare`
  - commituj katalog `.sqlx/` do repozytorium
  - w CI ustaw `SQLX_OFFLINE=true`
- migracje trzymaj w `migrations/` i uruchamiaj przez `sqlx::migrate!()` lub CLI
- unikaj `sqlx::query_unchecked!()` — jeśli musisz go użyć, dodaj komentarz z uzasadnieniem
- dla dynamicznych zapytań (np. filtry budowane w runtime) używaj query buildera (`sea-query`, `diesel` dsl) lub `QueryBuilder` z sqlx
- nie interpoluj wartości do SQL stringów — zawsze używaj bind parameters (`$1`, `$2` w Postgres)

## Długość plików i organizacja kodu
- trzymaj pliki z typami + podstawowymi metodami w przedziale 300–700 linii
- pliki z implementacjami traitów, serde, sqlx: do 800–1000 linii
- pliki testów jednostkowych: do 1200–1500 linii (jeśli czytelne)
- pliki `main.rs` / `lib.rs` w korzeniu: maksymalnie 400–600 linii
- gdy plik przekracza 800 linii i nadal rośnie → wyciągaj prywatne moduły
- gdy w pliku jest więcej niż 1–2 publiczne typy → rozbij
- gdy scrollujesz więcej niż 2–3 razy, żeby znaleźć metodę → rozbij
- preferuj strukturę vertical slice (grupowanie po funkcjonalności, np. `orders/`, `users/`) zamiast podziału warstwowego (`models/`, `services/`, `controllers/`)
- wyciągaj implementacje traitów, serde, sqlx do osobnych plików (np. `order/impls.rs`, `order/db.rs`)

## Wydajność praktyczna
- wykonuj profilowanie przed optymalizacją:
  - `cargo flamegraph`, `samply` — CPU profiling
  - `heaptrack` — analiza alokacji
  - `cargo-show-asm` — inspekcja wygenerowanego kodu
- trzymaj hot paths wolne od blokującego I/O i alokacji
- unikaj niepotrzebnych kopii przy pracy z dużymi danymi (np. `bytes::Bytes`, `&[u8]`)
- stosuj `#[inline]` / `#[inline(always)]` tylko po rzeczywistych pomiarach — kompilator zwykle wie lepiej

## Czytelność i utrzymywalność
- jawnie podawaj lifetimes tylko tam, gdzie kompilator ich nie wywnioskuje
- używaj nazw zmiennych i funkcji opisujących intencję, a nie typ
- trzymaj funkcje w granicach 30–50 linii
- usuwaj martwy kod (unused variables, imports, funkcje)

## Testy wydajnościowe
- pisz benchmarki dla krytycznych fragmentów (`criterion`, `divan`)
- porównuj wyniki przed i po zmianach
- pokrywaj testami typowe i graniczne przypadki użycia
- uruchamiaj benchmarki na tej samej maszynie / w CI z pinowanym CPU

## Zależności i kompilacja
- ogranicz liczbę zależności do minimum
- wyłączaj niepotrzebne features w `Cargo.toml` (`default-features = false`)
- w profilu release dla finalnych binarek produkcyjnych:
  ```toml
  [profile.release]
  opt-level = 3
  lto = "fat"
  codegen-units = 1
  ```
  (znacząco wydłuża kompilację — nie używaj w dev buildach)
- usuwaj debug info z binarki (`strip = true` w profilu lub `strip` po buildzie)

## Clippy i rustfmt
- kod musi przechodzić `cargo clippy -- -D warnings`
- nie używaj `#![allow(...)]` ani `#[allow(...)]` bez konkretnego, trwałego uzasadnienia
- zabronione uzasadnienia: „ktoś kiedyś poprawi", „bez tego nie buduje się", „tymczasowo"
- każde ignorowanie clippy wymaga komentarza z rzeczywistym powodem, np.:
  ```rust
  #[allow(clippy::too_many_arguments)]
  // Uzasadnienie: builder pattern nie pasuje do tego API,
  // argumenty są ze sobą logicznie powiązane i zawsze przekazywane razem
  ```
- formatuj zgodnie z `rustfmt`
- usuwaj wszystkie unused imports i zmienne
