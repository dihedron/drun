# AGENTS.md - Project Guidelines

## Build, Test, and Lint Commands
* **Run Tests:** `go test -v ./...`
* **Run Specific Test:** `go test -v ./internal/auth -run TestTokenExchange`
* **Build Binary:** `make`
* **Release:** `make release-[major|minor|patch]` depending on whether we are increasing the major, minor or revision level in semantic versioning (e.g. v1.2.3)
* **Other arguments:** run `make help` to get a list of available command line switches.

## Spec-Driven Development (SDD) Workflow
1. **Never code blind:** All new features or non-trivial bug fixes must have an approved specification file in the `.specs/` directory before writing implementation code.
2. **Plan first:** Always use Claude's `/plan` mode to review requirements against the codebase before editing files.
3. **Threat Model:** Whenever developing a complex solution, check that there is a Threat Model in the spec, ask for one otherwise, and take it into account when designing the solution itself and writing the code.
4. **Test-Driven Implementation:** Write unit or integration tests matching the acceptance criteria in the spec *before* writing the implementation code.
5. **Compliance Audit:** Before finishing a task, verify that the implementation matches the API contracts, data models, and edge cases defined in the active spec.
6. **No Secrets in Commits:** Before committing anything, check with all available local tools (e.g. gitleaks, trufflehog) that no secrets are in the sources being committed. 
7. **No vulnerable dependencies in Commits:** Before committing, run govulncheck and check that there are no vulnerable dependencies in `go.mod`.
8. **No unneeded backward compatibility:** Backward compatibility is not necessary (and should be avoided if it complicates code, introduces warts or hinders comprehension of the design and the code) until a product's major version (e.g. v1.0.0) is released, and even then it may be worth evaluating costs and benefits of keeping the product backward-compatible versus bumping the product's major version. Always ask for the user's choice unless the decision is trivial.
9. **Fuzzy Testing and Benchmarking:** Propose fuzzy testing and benchmarking when reasonable.
10. **Lab Testing:** Whenever applicable, propose to set up a lab environment to cover complex integration scenarios; don't shy away from proposing more coverage scenarios, including lengthy stress tests and chaos monkey to check for correctness under random conditions (e.g. race conditions, malfunctions), whenever reasonable.
11. **Up-to-date Documentation:** Always keep documentation up to date and aligned with the code and the product behaviour.
12. **Publish Documentation:** Always produce and attach to the release a Setup Guide, an Operator's Guide and an Architecture and Design Guide, in PDF format from Markdown sources; when diagrams and tables do not fit a page in portrait mode, use the landscape orientation for that page. Additional documents may be produced on a per-project basis.
13. **Sane Commits:** before every commit, run `go test ./...` (and `make lint`), unless the commit changes no code and no test input (documentation only). Check that everything is still sane.
14. **Overnight Runs:** Any run that takes hours (soak and chaos runs, full lab rebuilds, release-run campaigns) is written up as a plan and run in an overnight, unattended session, never started in the middle of a working day: propose it, say how long it takes, and report the outcome in the morning.
15. **Hourly Checks:** Any soak, campaign or other lengthy test (more than about an hour) is checked every hour while it runs (a scheduled check, not an occasional glance): is it still running, are failures or errors growing, is the lab healthy (nodes, Raft quorum, the lab's PostgreSQL, disk). On a real product failure, evaluate whether to stop, fix and restart on the fixed code; failures of the lab or tooling are fixed in the lab. Report each check in a line.

## Coding Style & Architecture Standards
* **Language/Framework:** Go (Golang); propose additional libraries where applicable.
* **Error Handling:** Never swallow errors. Wrap errors using `fmt.Errorf("context: %w", err)` and map them to gRPC status codes cleanly at the transport layer. Log errors where they occur using `log/slog`.
* **Concurrency:** Use explicit context propagation (`context.Context`) across all database calls, network requests, and background worker threads.
* **Logging:** Use structured logging (with `log/slog`) with contextual keys (`node_id`, `runnable`, `epoch`, `term`). Never log raw secrets, tokens, or PII.

## Project Structure Overview
* `cmd/` - Application entrypoints and main binaries: `drun` (the node and its CLI) and the example runnables (`drun-runnable-*`); for details on how the application command lines are structured, see the paragraph on command line arguments
* `internal/` - Private application and library code (the Raft node and its FSM, the supervisor, the admin API, the configuration).
* `pkg/` - Public library code: the build metadata injected at link time (`pkg/metadata`) and the runnable SDK (`pkg/runnable`: the interface, the go-plugin loader and server, the conformance kit), which third-party runnables import.
* `.specs/` - Feature design documents and architectural specifications; start from `drun_specification.md`, and see `drun-backlog.md` for the work agreed for later.
* `test/` - Test tooling: the LXD lab (`test/lab`) and its fault injection.
* `docs/` - The Setup, Operator's and Architecture and Design guides, in Markdown (rule 12).
* `packaging/` - Install scripts, the service unit and the public packaging key.
* `*.mk` - The build: `go.mk`, `help.mk` and `release.mk` are identical in ligo, netcheck and veho (keep them in sync with those copies); `Makefile` and `custom.mk` hold this project's settings and targets. Run `make help`.

## Project Notes
* **Consensus:** `hashicorp/raft`, pinned; every behaviour the design relies on is an `EXT-nn` assumption in `.specs/drun_specification.md` §12, checked by the spike's regression tests; recheck them on every version bump.
* **The FSM does very little:** it holds the fencing epoch and the current incarnations, plus each runnable's store, a SQLite database (`mattn/go-sqlite3`: the only cgo in the node, built statically against musl) written only by fenced batches (`.specs/drun-store.md`). `Apply` is deterministic: no clock, no randomness, no I/O but the store files; the leader puts timestamps in the commands, and the store refuses non-deterministic SQL. Every batch is applied exactly once, tracked by the last applied index inside the database itself.
* **Schema migrations belong to the runnable:** it migrates its own store at the start of `Run`, one version per batch (`pkg/runnable/migrate`).
* **Fencing:** a runnable is started only after its incarnation's epoch is committed; every checkpoint carries the epoch and the FSM refuses a stale one (INV-3). Stateful runnables pass the epoch to their external state store; the overlap window between two leaders is reduced, never assumed away.
* **Plugins:** `hashicorp/go-plugin` over gRPC with AutoMTLS; one fresh process per incarnation, killed when leadership is lost; only pinned binaries (SHA-256 in the configuration) are executed.

## Command line
Each application supports command line switches.
The command line arguments parsing is based on `github.com/jessevdk/go-flags`. Where applicable, the command line follows the simple convention:
```bash
$> my-application <object> <verb>
```
where the <object> is the type of resource to act upon and <verb> is the action. This is similar to how docker works (e.g. docker image list). The one verb that displays an object is `show`. The exceptions are the process commands (`serve`, `version`, `completion`). 
All applications should provide a version switch, with a one-liner compact mode and a verbose mode providing more detailed information at least about the application, its version, license, author, build and packaging environments.