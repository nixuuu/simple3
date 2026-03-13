# Writing efficient Rust code – code review checklist

## Ownership and borrowing
- avoid unnecessary `.clone()`, `.to_owned()`, `.to_string()`
- pass references `&T` / `&mut T` wherever possible
- use `Cow<'_, T>` instead of cloning in "might need ownership, might not" situations
- don't hold large structs by ownership when a reference suffices
- use `Arc<T>` in multithreaded code, `Rc<T>` only in single-threaded
- consider `Box<dyn Trait>` vs generics — dynamic dispatch reduces binary size but costs performance

## Data structure selection
- use `Vec` for sequential access and push/pop from the end
- use `VecDeque` for operations from both ends
- use `HashMap` for fast key lookup (average O(1))
- use `BTreeMap` / `BTreeSet` when ordering or range queries are needed
- avoid linear search (`find`, `position`) on large collections
- for small collections (up to ~20 elements) `Vec` + linear search can be faster than `HashMap`

## Iterators vs manual loops
- prefer iterator chains: `map` → `filter` → `collect` / `for_each` / `fold`
- avoid `for i in 0..len` with indexing when an iterator can do the job
- don't `.collect::<Vec<_>>()` just to iterate over the vector afterwards
- use `Iterator::size_hint()` and `ExactSizeIterator` when implementing custom iterators

## Parallel processing (rayon)
- use `par_iter()` / `par_iter_mut()` / `into_par_iter()` for CPU-bound operations on large collections
- switching `.iter()` to `.par_iter()` makes sense when:
  - the collection has > 1000 elements, or
  - a single iteration takes > 1μs
- don't use rayon for I/O-bound operations — use async instead (tokio, async-std)
- avoid `par_iter()` when:
  - the operation requires ordering and you're using `for_each` (use `par_bridge()` + `collect()` if order matters)
  - the collection is small (thread pool overhead exceeds the gain)
  - iterations contain allocations or locks (contention will negate the gain)
- prefer `par_chunks()` / `par_chunks_mut()` for batch processing of large data
- for reductions use `par_iter().reduce()` or `par_iter().sum()` instead of manually collecting results
- set thread pool size explicitly via `rayon::ThreadPoolBuilder` when the default (CPU count) is suboptimal
- profile before and after — not all loops benefit from parallelism

## Allocations and memory management
- minimize allocations inside hot paths (code executed repeatedly in loops or on the critical path)
- use `&str` instead of `String` in function arguments (when not modifying)
- prefer `String::with_capacity()` + `push_str` over repeated `+=`
- avoid `format!` in loops — build the `String` manually or use `write!` to an existing buffer
- for small vectors consider `smallvec`, `tinyvec`
- for small strings consider `smol_str`, `compact_str`, `smartstring`

## Async
- use `tokio::spawn` for async tasks, `spawn_blocking` for blocking CPU/IO
- avoid `.await` in loops when `join_all`, `try_join_all`, or `FuturesUnordered` can be used
- don't hold a `MutexGuard` across `.await` — use `tokio::sync::Mutex` or restructure the code
- prefer `select!` with `biased` when check order matters

## Safety and panics
- use `unwrap()` / `expect()` only in tests and `main`
- in libraries and production code return `Result` / `Option`
- avoid `panic!` on hot paths (unwind cost is high)
- keep `unsafe` blocks small, well-documented, and justified
- mark functions returning values that should not be ignored with `#[must_use]`

## Error handling
- in libraries define custom error types (`thiserror`)
- in applications use `anyhow` for simplified error propagation
- avoid `Box<dyn Error>` as a return type in public APIs
- don't overuse `.unwrap_or_default()` — explicitly handle edge cases

## SQLx and databases
- use macros `sqlx::query!()`, `sqlx::query_as!()`, `sqlx::query_scalar!()` instead of runtime `query()` / `query_as()`
- compile-time verification requires `DATABASE_URL` set during compilation
- for CI without database access use offline mode:
  - generate cache: `cargo sqlx prepare`
  - commit the `.sqlx/` directory to the repository
  - in CI set `SQLX_OFFLINE=true`
- keep migrations in `migrations/` and run via `sqlx::migrate!()` or CLI
- avoid `sqlx::query_unchecked!()` — if you must use it, add a comment with justification
- for dynamic queries (e.g. filters built at runtime) use a query builder (`sea-query`, `diesel` dsl) or `QueryBuilder` from sqlx
- never interpolate values into SQL strings — always use bind parameters (`$1`, `$2` in Postgres)

## File length and code organization
- keep files with types + basic methods in the 300–700 line range
- files with trait implementations, serde, sqlx: up to 800–1000 lines
- unit test files: up to 1200–1500 lines (if readable)
- `main.rs` / `lib.rs` root files: 400–600 lines maximum
- when a file exceeds 800 lines and keeps growing → extract private modules
- when a file has more than 1–2 public types → split it
- when you scroll more than 2–3 times to find a method → split it
- prefer vertical slice structure (grouping by feature, e.g. `orders/`, `users/`) over layered organization (`models/`, `services/`, `controllers/`)
- extract trait implementations, serde, sqlx into separate files (e.g. `order/impls.rs`, `order/db.rs`)

## Practical performance
- profile before optimizing:
  - `cargo flamegraph`, `samply` — CPU profiling
  - `heaptrack` — allocation analysis
  - `cargo-show-asm` — generated code inspection
- keep hot paths free of blocking I/O and allocations
- avoid unnecessary copies when working with large data (e.g. `bytes::Bytes`, `&[u8]`)
- use `#[inline]` / `#[inline(always)]` only after actual measurements — the compiler usually knows better

## Readability and maintainability
- explicitly specify lifetimes only where the compiler cannot infer them
- use variable and function names that describe intent, not type
- keep functions within 30–50 lines
- remove dead code (unused variables, imports, functions)

## Performance testing
- write benchmarks for critical sections (`criterion`, `divan`)
- compare results before and after changes
- cover both typical and edge cases with tests
- run benchmarks on the same machine / in CI with pinned CPU

## Dependencies and compilation
- keep the number of dependencies to a minimum
- disable unnecessary features in `Cargo.toml` (`default-features = false`)
- in release profile for final production binaries:
  ```toml
  [profile.release]
  opt-level = 3
  lto = "fat"
  codegen-units = 1
  ```
  (significantly increases compile time — don't use in dev builds)
- strip debug info from the binary (`strip = true` in profile or `strip` after build)

## Clippy and rustfmt
- code must pass `cargo clippy -- -D warnings`
- don't use `#![allow(...)]` or `#[allow(...)]` without a specific, lasting justification
- forbidden justifications: "someone will fix it later", "it won't build without this", "temporarily"
- every clippy suppression requires a comment with a real reason, e.g.:
  ```rust
  #[allow(clippy::too_many_arguments)]
  // Justification: builder pattern doesn't fit this API,
  // arguments are logically related and always passed together
  ```
- format according to `rustfmt`
- remove all unused imports and variables
