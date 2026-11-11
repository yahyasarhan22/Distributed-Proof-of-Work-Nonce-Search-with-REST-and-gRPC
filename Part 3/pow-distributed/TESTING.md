# Executed verification — 2026-11-11

## GitHub publication recheck

Before publication, both Part 2 and Part 3 passed fresh `go build ./...`,
`go test -count=1 -v ./...`, `go vet ./...`,
`go test -race -count=1 ./...`, and `go mod verify` runs. Both dashboard
JavaScript files also passed `node --check`.

The launchers and count/configuration process checks passed in a separate source
copy on HTTP 28080 and gRPC 52051 onward, preserving an existing eight-worker
session. The copy used the local Go dependency cache and changed only test
ports/cache locations. Four independent workers produced a proof verified with
.NET SHA-256; duplicate launch prevention, selective stopping, cleanup, and
1/2/4/8-worker registration passed.

One crash-demo run exposed a timing limitation in the demonstration script:
the killed worker's range was requeued, but another worker found a valid proof
before reassignment. The script reported missing reassignment evidence. This
is documented in the README; no coordinator/worker source was changed.
The browser interaction suite was not rerun for this documentation update;
its earlier evidence is recorded below.

## Earlier implementation verification

Environment: Windows/amd64, Go `go1.27.4`, portable GCC targeting
`x86_64-w64-mingw32`, Chromium with Playwright. The project requires Go 1.26+.
The runtime dashboard uses no Node packages. Local tests used HTTP 18080,
coordinator gRPC 51051 and worker ports 51052 onward because Windows refused
binding HTTP 8080. The application default remains 8080.

## Final command results

All commands below actually executed successfully:

| Command | Actual result |
|---|---|
| `go mod tidy` | Exit 0 |
| `go build ./...` | Exit 0 |
| `go test -count=1 -v ./...` | Exit 0; 18 top-level tests passed, plus 2 subtests |
| `go test -race ./...` | Exit 0; no races reported |
| `go vet ./...` | Exit 0; no findings |
| `scripts/generate-proto.ps1`, followed by `go build ./...` | Exit 0 |
| `node --check internal/coordinator/web/app.js` | Exit 0 |
| `node --check tests/dashboard.cjs` | Exit 0 |
| `tests/launchers.ps1` | Passed real four-process proof, unique PIDs, duplicate rejection, selective stopping and full cleanup |
| `tests/configuration.ps1` | Passed actual 1, 2, 4 and 8 worker registration/stop cycles, alternate ports and chunk configuration |
| `tests/failure-process.ps1 -BaseUrl http://127.0.0.1:18080` | Passed forced active-worker termination, offline detection, exact range reassignment and independent proof verification |
| `node tests/dashboard.cjs` | Passed against the final running coordinator and four real worker processes |

The Go tests use ephemeral TCP listeners, real protobuf/gRPC clients and
servers, HTTP test servers and controlled failing gRPC handlers. This permits
deterministic failure/cancellation scenarios without probabilistic long mining
runs. Separate Windows tests then exercise actual worker process termination.
Timeout/cancellation tests synchronize through channels and bounded polling.

The race suite also passed three consecutive runs earlier in validation.
After review fixes, the assignment-deadline and busy-handoff regressions passed
three further race-instrumented repetitions, followed by the full final suite.

## Observed real-process evidence

The final normal four-process launcher test computed:

```text
data = hello
difficulty = 4
nonce = 3004193
hash = 0000e8cdbde6b8bd4676e6def02e3fde1fb24b214d3c185f5f7c2bcd1d045ea0
```

PowerShell independently recomputed this hash using .NET SHA-256. This is an
observed result, not a constant used by the application. Another run can find
a different valid nonce.

The final crash test observed worker-2 holding `[1000000,2000000)` for job
`482fc8fd201f7728fb1472263f1f9c01`. It killed that worker, observed it offline,
and found these coordinator log transitions for the **same** range:

```text
range assigned worker=worker-2 assignment=9d7f7f92d9ae7cf940a8dd993c564fb6
range requeued worker=worker-2 reason="Search RPC: ... Unavailable ... forcibly closed ..."
range assigned worker=worker-3 assignment=87164ee26cbbcf3b24c48ce538541f51
```

That job completed with a real, independently verified proof:

```text
data = fault-demo
difficulty = 6
nonce = 6703361
hash = 000000b6d5017402ec6219a7580bc215b65d6582273faccb1efcfa152b1bccfe
```

These are functionality checks, not scalability measurements. No Part 4
benchmark table or worker-count performance experiment was performed.

## Dashboard verification

The browser test used the real coordinator API and actual worker processes.
It checked submission, SHA verification with Node's independent `crypto`
implementation, native EventSource responses/nonzero progress, displayed worker
assignments, cancellation, reopening completed history and terminal SSE closure.
It also verified safe rendering of literal HTML-like input and form recovery
from an injected HTTP 400 response.

A transparent local test proxy severed an established SSE connection. The UI
displayed its disconnected state, native EventSource reconnected, and the same
job remained operable. This proxy is test-only and is not part of the runtime.

Desktop (1440px) and mobile (390px) screenshots were captured in
`docs/screenshots`. The final test confirmed no page-level mobile horizontal
overflow, full-hash wrapping, reduced-motion behavior and no browser JavaScript
exceptions. The history table scrolls inside its own region on small screens.
The mechanical design detector used its degraded fallback because optional
parser packages were unavailable; contrast was not automatically certified.

## Review fixes and practical limits

A read-only reviewer identified a worker handoff race: a cancelled client RPC
can return before the remote Search handler clears its busy flag. The added
regression demonstrably failed before the fix. ResourceExhausted now requeues
the range with a 100ms cooldown while preserving the worker session. Its
regression and the full race suite pass. Shutdown was also changed to hold one
launcher lock across both worker and coordinator stopping; subsequent process
tests passed. Targeted review found no further substantive issue in those fixes.

All test-launched coordinator/worker processes were stopped afterward. Final
PID metadata arrays are empty. No Part 2 source or tools were modified.

The coordinator deliberately uses in-memory state. No persistence, TLS or
authentication was added. Attempt counts exclude work lost before a progress
report. The finite nonce domain and counter saturation behavior are documented
in README and ARCHITECTURE. Default-port 8080 operation could not be verified
on this Windows environment; the same HTTP server passed at port 18080.
