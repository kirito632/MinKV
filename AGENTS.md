# MinKV Agent Guide

## Scope

These instructions apply to the entire repository. Prefer the executable build
configuration and nearby code over older reports or completion-summary files.
`CMakeLists.txt` currently sets C++17, even if another local instruction mentions
C++20.

For Git or contribution work, also follow `.instructions/git-rules.md` and
`.instructions/contribution-sop.md` as applicable.

## Repository Map

- `src/core/`: `ShardedCache`, LRU storage, the public `MinKV` facade, and core
  cache behavior.
- `src/base/`: background services and low-level utilities such as expiration,
  append-only files, thread pools, and async logging.
- `src/persistence/`: WAL, group commit, checkpointing, recovery, and LSN logic.
- `src/vector/`: vector values, indexes, Top-K helpers, and SIMD operations.
- `src/graph/`: graph serialization and the Graph-on-KV/GraphRAG layer.
- `src/server/`: HTTP/RESP adapters. `httplib.h` is vendored here.
- `tests/` and `src/tests/`: standalone assert-based tests and benchmarks.
- `mcp_server_go/`: a separate Go 1.21 MCP bridge using the standard library.
- `docs/`: design notes and benchmark reports; these are not the source of truth
  for current behavior.

## Build

The supported build path is CMake. The default build type is `Release` and all
C++ targets are compiled for the host CPU with AVX2/FMA enabled.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --target <target> -j2
```

Use `Release` for benchmarks:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target comprehensive_benchmark -j2
```

Configuration requires RapidCheck. The graph HTTP target also requires
`nlohmann_json`; CMake fetches it when no system package is found, so a fresh
configure may need network access. Do not remove `-march=native`, `-mavx2`, or
`-mfma` casually: published performance numbers and vector code assume them.
Conversely, do not claim portable non-AVX2 support unless it is implemented and
tested.

Avoid building `all` for a narrow change. It includes servers, benchmarks, and
graph property tests. Build the smallest relevant target instead.

## Tests

There is no registered CTest suite. Tests are executables in `build/bin`, so run
the relevant binary directly after building its target:

```bash
cmake --build build --target test_vector_integration -j2
./build/bin/test_vector_integration
```

Useful target mapping:

- cache construction/sharding: `test_sharded_cache_constructor`
- expiration failures: `expiration_exception_test`
- WAL/group commit: `group_commit_test`, `wal_performance_benchmark`
- async logging: `async_logger_test`
- vector behavior: `test_vector_integration`
- graph serialization/store: `test_graph_serializer`,
  `test_graph_store_phase1`, `test_graph_store_phase2`
- GraphRAG integration: `test_mcp_simulation`, `test_graphrag_accuracy`

Tests use `assert`; configure a `Debug` build so the assertions remain active.
A failed assertion terminates the binary with a nonzero status. No GoogleTest/Catch2
framework is present. Add new test executables to `CMakeLists.txt`, link
`pthread` when they exercise concurrent code, and avoid sleeps when observable
synchronization is available.

Some persistence tests create relative `.log` files or test-data directories.
Before running one, inspect its paths. Remove only artifacts created by that run;
never delete pre-existing user data or repository fixtures.

Benchmarks are validation only for performance work. Record the build type,
compiler flags, CPU, thread count, shard count, and workload when changing a
published performance result.

For the Go MCP module:

```bash
cd mcp_server_go
go test ./...
go build ./...
```

Run `gofmt` only on changed Go files.

## C++ Conventions

- Match `.clang-format`: Google-derived style, 2-space indentation, attached
  braces, 90-column limit, and left-aligned pointers/references.
- Format only touched C++ files or changed regions; avoid repository-wide churn.
- Keep the project on C++17 unless the task explicitly changes the build
  contract.
- Reuse the existing `minkv`, `minkv::db`, and `minkv::graph` namespaces and
  nearby naming conventions. Do not introduce a parallel abstraction for an
  existing cache, serializer, thread, or persistence helper.
- Keep public headers self-contained. Template definitions that callers must
  instantiate belong in headers.
- Add comments for invariants and non-obvious concurrency or binary-format
  decisions, not line-by-line narration.
- Keep changes narrow. Do not update generated reports, benchmark CSV/PNG files,
  archives, or deployment scripts unless the task requires them.

## Correctness Invariants

- `ShardedCache` is thread-safe and owns background expiration and persistence
  lifecycles. Preserve lock ordering and ensure worker threads are stopped and
  joined before destroying state they access.
- `ttl_ms == 0` means no expiration. Preserve this contract across cache,
  serialization, recovery, and server adapters.
- Persistent mutations enqueue their WAL record before changing in-memory state.
  Keep LSNs monotonic, checkpoint cuts consistent, recovery idempotent, and old
  WAL cleanup strictly after a successful covering snapshot.
- Do not hold a cache/shard lock across filesystem I/O, waits, or arbitrary user
  work unless the existing API explicitly requires it. `update_in_place` is the
  notable callback-under-lock API, so document and test any change to it.
- Preserve on-disk and graph serialization compatibility unless a migration is
  part of the task. Graph nodes, edges, adjacency lists, and embeddings share the
  underlying string KV store.
- Vector operations must validate dimensions and keep scalar/SIMD results
  equivalent within an appropriate floating-point tolerance.

## Change Hygiene

- The worktree may already contain unrelated edits and untracked artifacts.
  Inspect `git status` and the relevant diff, then preserve all changes you did
  not make.
- When public behavior or commands change, update both `README.md` and
  `README_zh-CN.md` where applicable.
- Do not commit, push, delete files, or run destructive Git commands unless the
  user explicitly requests it. Show the diff before a requested commit.
- Never place credentials, API keys, local data, binaries, or benchmark output in
  a commit.

## Sandbox Failure Handling

If a command required for the task fails because of sandbox, namespace, or
permission setup, retry it immediately with escalation. Examples include
`bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted` and similar
sandbox startup failures.

1. Attempt the command normally first.
2. If the failure is environment-related, rerun it with `require_escalated`.
3. Use a narrow `prefix_rule` when appropriate.
4. Issue the escalation request directly instead of asking for a separate
   confirmation first.
