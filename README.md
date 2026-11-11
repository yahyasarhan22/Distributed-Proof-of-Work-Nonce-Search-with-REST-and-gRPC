# Distributed Systems Assignment 1: gRPC and Distributed Proof of Work

ENCS5324 Assignment 1 — **Part 2** and **Part 3**, implemented in Go.
Each part is a separate Go module and runs independently.

| Part | Application | Run instructions |
| --- | --- | --- |
| 2 | Live log viewer: unary and server-streaming gRPC | [Part 2 README](Part%202/logviewer/README.md) |
| 3 | Distributed SHA-256 nonce search: REST coordinator, gRPC workers, live browser dashboard | [Part 3 README](Part%203/pow-distributed/README.md) |

The assignment specification is [A1.pdf](A1.pdf). The commands below start from
the repository root unless stated otherwise. Paths are quoted because the part
folder names contain spaces.

## Prerequisites and download

- Install **Go 1.26 or newer** and make sure `go version` works in a new terminal.
- Install Git to clone, or use GitHub's **Code → Download ZIP** and extract it.
- Internet access is required for the first dependency download. Both
  applications then run locally, with no database, account, API key, or Docker.
- Generated protobuf Go files are included. **You do not need protoc to run or
  test either part.** Node.js is also unnecessary for the dashboard.

```sh
git clone https://github.com/yahyasarhan22/Distributed-Proof-of-Work-Nonce-Search-with-REST-and-gRPC.git
cd Distributed-Proof-of-Work-Nonce-Search-with-REST-and-gRPC
go version
```

**Run the parts one at a time:** both use port `50051` by default. Stop Part 2's
server with Ctrl+C before starting Part 3. Part 3 also allows alternate ports.

## Part 2 — live log viewer

In **terminal 1**, from the repository root:

```sh
cd "Part 2/logviewer"
go mod download
go run ./server
```

Keep it open. Expect `Log Server listening on localhost:50051`.

In **terminal 2**, also starting from the repository root:

```sh
cd "Part 2/logviewer"
go run ./client
```

The client prints `Log Server` / `RUNNING` from the unary RPC, then five log
messages over approximately four seconds from the streaming RPC. The levels
include INFO, WARNING, and ERROR, ending with `Stream completed successfully.`
The events are simulated; the database error is a sample message.

Optional: `go run ./client -service payments` changes the simulated service name.
Stop the server with **Ctrl+C** when finished.

## Part 3 — distributed nonce search

The coordinator accepts jobs over REST and assigns distinct nonce ranges to
independent worker processes over gRPC. Workers search for
`SHA256(data + decimal_string(nonce))` with the requested number of leading
**hexadecimal zero digits**. The coordinator verifies the winning proof and
cancels the remaining assignments. Failed workers' ranges are reassigned.

### Windows PowerShell: start four workers

From the repository root:

```powershell
cd 'Part 3/pow-distributed'
go mod download
powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/start-coordinator.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/start-workers.ps1 -Count 4
```

Open **http://localhost:8080/**. Confirm four workers are online, enter `hello`
with difficulty `4`, and submit the job. Expect `FOUND`, a nonce, and a SHA-256
hash beginning with `0000`. The valid nonce can differ between runs. The
dashboard also shows live progress, job history, worker assignments, and a
cancel button. No frontend build or additional web server is needed.

Stop the background processes from the same project directory:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File ./scripts/stop-system.ps1
```

If port 8080 is unavailable, stop the system, start the coordinator with
`-HttpPort 18080`, start the workers as above, and open http://localhost:18080/.
If port 50051 is occupied, see the alternate-port example in the
[Part 3 README](Part%203/pow-distributed/README.md).

### Any OS: run foreground processes

On Windows, Linux, or macOS, use a separate terminal for each command below.
**First run `cd "Part 3/pow-distributed"` from the repository root in every
terminal.** These commands are an alternative to the Windows launchers.

| Terminal | Command |
| --- | --- |
| 1 | `go run ./cmd/coordinator` |
| 2 | `go run ./cmd/worker -id worker-1 -port 50052` |
| 3 | `go run ./cmd/worker -id worker-2 -port 50053` |
| 4 | `go run ./cmd/worker -id worker-3 -port 50054` |
| 5 | `go run ./cmd/worker -id worker-4 -port 50055` |

Open http://localhost:8080/ as above. Press **Ctrl+C in each terminal** to stop
foreground processes; the Windows stop script manages only launcher processes.

### Optional REST demonstration (PowerShell)

With the coordinator and workers running:

```powershell
$base = 'http://localhost:8080'
$job = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"hello","difficulty":4}'
curl.exe -N "$base/jobs/$($job.job_id)/events"
Invoke-RestMethod "$base/jobs/$($job.job_id)" | Format-List
```

The SSE stream ends after the final `found` event. The result includes `job_id`,
`status`, `nonce`, `hash`, `nonces_checked`, and `elapsed_seconds`.
Use the dashboard to cancel a higher-difficulty job while it is running.
For worker-crash/reassignment, concurrent jobs, independent hash verification,
and API error demonstrations, see the [Part 3 guide](Part%203/pow-distributed/README.md#manual-demonstration-guide).

## Build and test

Run the following **inside each module directory separately**
(`Part 2/logviewer` and `Part 3/pow-distributed`):

```sh
go build ./...
go test -count=1 ./...
go vet ./...
```

There is no root-level Go module. Tests launch their own local servers and
clean them up. Part 2 checks unary results, streamed message timing, normal
completion, cancellation, and deadlines. Part 3 checks hashing, REST/SSE,
real gRPC searches, concurrent jobs, cancellation, worker failures and retries.

Optional race check: `go test -race -count=1 ./...` requires a supported C
compiler (MinGW-w64 on Windows). Additional Windows process tests and earlier
dashboard test evidence are described in
[Part 3 TESTING.md](Part%203/pow-distributed/TESTING.md).

## Repository contents

```text
.
├── A1.pdf
├── README.md
├── Part 2/
│   └── logviewer/             # Independent Go module
│       ├── client/
│       ├── server/
│       └── proto/             # Schema and generated bindings
└── Part 3/
    └── pow-distributed/       # Independent Go module
        ├── cmd/               # Coordinator and worker entry points
        ├── internal/          # Scheduling, hashing, workers, embedded UI
        ├── proto/             # Schema and generated bindings
        ├── scripts/           # Windows launch, stop, and generation tools
        ├── tests/             # Process and optional browser checks
        └── docs/screenshots/  # Dashboard captures
```

Downloaded SDKs/compilers, caches, executable files, runtime logs, and process
records are excluded from Git. Install Go locally; the launch scripts build
their own executables. Existing `benchmark-*.csv` files are retained as raw
data; this repository's run guide covers Parts 2 and 3, without a Part 4
performance analysis. State is held in memory, so restarting the coordinator
clears jobs and worker registrations.
