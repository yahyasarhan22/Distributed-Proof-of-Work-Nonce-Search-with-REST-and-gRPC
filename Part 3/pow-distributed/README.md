# Distributed proof-of-work nonce search

Part 3 of ENCS5324 Assignment 1, plus the optional HTML dashboard. One Go
coordinator serves REST, SSE and the dashboard; independent Go worker processes
search over real gRPC connections. There is no database or frontend build step.
Part 2 is a separate Go module; see the [repository guide](../../README.md).
Existing benchmark CSV files are retained as raw data, without a Part 4 analysis.

For input `data`, the search calculates `SHA256(data + decimal_string(nonce))`.
Difficulty 1–8 means **at least that many leading hexadecimal zeros**, not bits.
Every submitted proof is independently recomputed by the coordinator.

## Requirements and installation

- Windows PowerShell 5.1+ for the launcher scripts. The Go programs can also
  run directly on Linux and macOS; see the [repository guide](../../README.md).
- Go 1.26 or later. Generated protobuf bindings are already committed.
- A 64-bit MinGW-w64 GCC for `go test -race` on Windows/amd64 only.
- A modern browser for the dashboard, or PowerShell/curl.exe for REST.

From the cloned repository root, open PowerShell:

```powershell
Set-Location 'Part 3/pow-distributed'
. .\scripts\env.ps1
go mod download
go build ./...
```

`env.ps1` uses Go on PATH, or finds the existing installation under
`..\..\Part 2\.tools\go\bin`. It keeps writable Go caches in this project's
`.tools`. It never installs into or edits Part 2. Dependencies are pinned in
`go.mod` and `go.sum`. Internet is needed for uncached dependencies. Downloaded
tools, compilers, and dependency caches are excluded from GitHub; install Go
on your own machine. No C compiler is needed for a normal build or run.

If local script execution is restricted, use this per-process invocation (no
Administrator privileges or machine-wide policy change):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start-coordinator.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start-workers.ps1 -Count 4
# When finished:
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\stop-system.ps1
```

## Quick start

```powershell
.\scripts\start-coordinator.ps1
.\scripts\start-workers.ps1 -Count 4
Start-Process 'http://localhost:8080/'
```

Stop Part 2's server first: it also uses port 50051 by default.
The scripts build binaries and start background processes with separate PIDs.
Worker `i` uses ID `worker-i` and port `50051+i`: four workers use 50052–50055.
They retry registration if the coordinator is temporarily unavailable. Each
worker runs one search at a time. Re-running the launcher while its group is
alive is rejected. Stop the current group before changing its count.

The dashboard can submit jobs, follow SSE, cancel the selected search, open
history and inspect worker assignments. Multiple jobs can remain active while
you inspect one. It uses only real API data. It reconnects SSE automatically,
polls history/workers every two seconds, and closes SSE for terminal jobs.
Completed jobs can be reopened. Refreshing the browser keeps coordinator jobs;
restarting the coordinator clears all jobs and registrations.

Submit and inspect the first job without the dashboard:

```powershell
$base = 'http://localhost:8080'
$job = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"hello","difficulty":4}'
$job.job_id
Invoke-RestMethod "$base/jobs/$($job.job_id)" | Format-List
curl.exe -N "$base/jobs/$($job.job_id)/events"
```

Stop the script-launched system:

```powershell
.\scripts\stop-system.ps1
```

View logs in another terminal (`Ctrl+C` stops log following):

```powershell
Get-Content .run/coordinator.err.log -Wait
Get-Content .run/worker-1.err.log -Wait
```

`slog` writes logs to stderr, hence the `.err.log` names even for normal
messages. Each launch replaces that process's logs. PID, executable path and
start time are recorded in `.run/*.json`; stop scripts check all three to avoid
stopping unrelated processes after PID reuse. Stop scripts forcibly terminate
their processes, also useful for demonstrating crashes. For graceful shutdown,
use foreground commands below and press `Ctrl+C`.

## Manual processes and configuration

Run each command in its own terminal after loading `env.ps1`:

```powershell
go run ./cmd/coordinator -http 127.0.0.1:8080 -grpc 127.0.0.1:50051 -chunk-size 1000000 -heartbeat-timeout 3s -assignment-timeout 2m
go run ./cmd/worker -id worker-1 -port 50052 -coordinator 127.0.0.1:50051 -heartbeat-interval 500ms
go run ./cmd/worker -id worker-2 -port 50053 -coordinator 127.0.0.1:50051 -heartbeat-interval 500ms
go run ./cmd/worker -id worker-3 -port 50054 -coordinator 127.0.0.1:50051 -heartbeat-interval 500ms
go run ./cmd/worker -id worker-4 -port 50055 -coordinator 127.0.0.1:50051 -heartbeat-interval 500ms
```

The stop scripts manage only processes they launched. Stop manually launched
programs in their own terminals with `Ctrl+C`.

| Setting | Coordinator flag/default | Worker flag/default |
|---|---|---|
| REST address | `-http 127.0.0.1:8080` | — |
| Coordination address | `-grpc 127.0.0.1:50051` | `-coordinator 127.0.0.1:50051` |
| Range size | `-chunk-size 1000000` | — |
| Lease expiry | `-heartbeat-timeout 3s` | — |
| Maximum Search duration | `-assignment-timeout 2m` | — |
| Worker identity | — | `-id worker-1` |
| Search endpoint | — | `-host 127.0.0.1 -port 50052` |
| Advertised endpoint | — | `-advertise host:port` (defaults to bind address) |
| Heartbeat / retry interval | — | `-heartbeat-interval 500ms` |
| Unary RPC timeout | — | `-rpc-timeout 1s` |
| Progress interval | — | `-progress-interval 200ms` |

Keep the heartbeat timeout comfortably above the heartbeat interval and RPC
timeout. Keep assignment timeouts above the expected time to search one chunk.
An expired assignment is cancelled and retried, never marked exhausted.

Alternate ports, useful if Windows has reserved 8080 or another app uses it:

```powershell
.\scripts\start-coordinator.ps1 -HttpPort 18080 -GrpcPort 51051 -ChunkSize 100000
.\scripts\start-workers.ps1 -Count 4 -BasePort 51052 -Coordinator '127.0.0.1:51051' -HeartbeatInterval '500ms'
$base = 'http://localhost:18080'
Start-Process "$base/"
```

## REST and event format

| Method and path | Behavior |
|---|---|
| `GET /` | Embedded dashboard |
| `POST /jobs` | JSON `data` and integer `difficulty`; 202 with `job_id` |
| `GET /jobs/{id}` | Job snapshot; 404 if unknown |
| `GET /jobs/{id}/events` | SSE snapshots, including final state for late subscribers |
| `DELETE /jobs/{id}` | Cancel; 200 with snapshot, 404 unknown, 409 terminal |
| `GET /jobs` | Array of snapshots, submission order |
| `GET /workers` | Worker identity/address, `alive`, `last_heartbeat`, optional `current_assignment` |

Job fields are `job_id`, `data`, `difficulty`, `status`, `nonces_checked`,
`elapsed_seconds`, plus `nonce` and `hash` only when FOUND. Nonce zero is present
as `"nonce":0`. Time begins at submission, includes waiting/retries and freezes
on completion/cancellation. `nonces_checked` sums **reported hash attempts**,
including repeated attempts after failure, not distinct nonce coverage. Work
performed since a crashed worker's last report is unknowable. Terminal counts
freeze; cancellation may leave some final in-flight attempts unreported.

An SSE event contains a complete job snapshot, using one of `progress`, `found`
or `cancelled`. For example, schematic syntax (values come from the real job):

```text
event: progress
data: {"job_id":"...","data":"hello","difficulty":4,"status":"RUNNING","nonces_checked":50000,"elapsed_seconds":0.25}

```

FOUND adds a numeric `nonce` and full lowercase `hash`. CANCELLED omits both.
The server immediately sends the current state, publishes worker progress,
sends a snapshot at least every second and closes after the final event.
Each subscriber has a one-slot latest-snapshot buffer. Slow clients may miss
intermediate updates, but can always retrieve the terminal snapshot. Writes
have a five-second deadline. SSE is a state stream, not a durable event log;
there are no event IDs or replay guarantees. `curl.exe -N` disables buffering.

## Manual demonstration guide

### 1. Normal job and independent verification

Start the coordinator and four workers using Quick start, then:

```powershell
$base = 'http://localhost:8080'
$job = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"hello","difficulty":4}'
do {
    $result = Invoke-RestMethod "$base/jobs/$($job.job_id)"
    $result | Select-Object job_id,status,nonces_checked,elapsed_seconds
    if ($result.status -eq 'RUNNING') { Start-Sleep -Milliseconds 250 }
} while ($result.status -eq 'RUNNING')
$result | Format-List
$sha = [Security.Cryptography.SHA256]::Create()
$hash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes("hello$($result.nonce)"))).Replace('-','').ToLower()
$sha.Dispose()
"Matches returned hash: $($hash -eq $result.hash)"
"Meets difficulty: $($hash.StartsWith('0000'))"
```

Any valid nonce is acceptable; distributed execution does not guarantee the
lowest nonce. No nonce in this guide is supplied as a fabricated result.

### 2. SSE

Use the returned ID, or `$job.job_id` in the same PowerShell terminal:

```powershell
$job = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"hello","difficulty":6}'
curl.exe -N "$base/jobs/$($job.job_id)/events"
# Equivalent: replace JOB_ID with the actual ID, without braces.
curl.exe -N http://localhost:8080/jobs/JOB_ID/events
```

Observe `progress`, then `found` with its proof. If you connect after completion,
you receive the final event immediately. Multiple terminals can follow one job.
`Ctrl+C` disconnects the viewer without cancelling the job.

### 3. Client cancellation

```powershell
$cancelJob = Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body '{"data":"cancel-demo","difficulty":8}'
Invoke-RestMethod "$base/jobs/$($cancelJob.job_id)" -Method Delete
Invoke-RestMethod "$base/jobs/$($cancelJob.job_id)"
```

First confirm the job is RUNNING with active assignments in `/workers` or the
dashboard. Observe coordinator `client cancelled job` / `cancel search` and
worker `search cancelled` logs. Status remains CANCELLED even if a result
arrives late. Very occasionally a difficult job can finish before cancellation;
then DELETE correctly returns 409. Repeat with another input to demonstrate.

### 4. Worker failure and reassignment

With four script-launched workers, this bounded functional check submits a real
difficulty-6 job, kills worker-2 while assigned, verifies dead status, checks
requeue/reassignment logs and independently verifies the final hash:

```powershell
.\tests\failure-process.ps1 -BaseUrl $base
```

This real-process demonstration is timing-sensitive. A valid proof can arrive
after the worker is killed but before its range is reassigned, producing
`Missing range requeue/reassignment evidence` even though the job completes
correctly. Retry with a fresh worker group; using `-ChunkSize 100000` when
starting the coordinator gives shorter assignments and more opportunities to
observe reassignment. The Go integration tests cover retries deterministically.

For a manual presentation, submit a difficulty-6 job and run:

```powershell
Invoke-RestMethod "$base/workers" | ConvertTo-Json -Depth 5
.\scripts\stop-workers.ps1 -Id worker-2
Invoke-RestMethod "$base/workers" | ConvertTo-Json -Depth 5
Get-Content .run/coordinator.err.log -Tail 30
```

Capture worker-2's active `[start,end)` first. Look for `range requeued`, followed
by `range assigned` with the same job/start/end and a different assignment ID.
A broken RPC usually detects a killed process before the heartbeat timeout.
If the RPC remains connected but heartbeats stop, the lease timeout cancels it.
Poll the job until FOUND. If it finished before the kill, repeat with a fresh
input/higher difficulty. To replace the failed worker in a separate terminal:

```powershell
. .\scripts\env.ps1
go run ./cmd/worker -id worker-2 -port 50053
```

Wait for its old lease to expire if a duplicate-ID rejection appears. Other jobs
continue. If all workers stop, RUNNING jobs wait and resume when workers return.
This script kills worker-2 intentionally; restart a clean group afterward.

### 5. Concurrent jobs

POST returns immediately, so these quick submissions overlap their searches:

```powershell
$submitted = @('first','second','third') | ForEach-Object {
    Invoke-RestMethod "$base/jobs" -Method Post -ContentType 'application/json' -Body (@{data=$_;difficulty=5} | ConvertTo-Json)
}
$submitted
Invoke-RestMethod "$base/jobs" | Format-Table job_id,status,nonces_checked,elapsed_seconds
```

Verify unique IDs and independent valid final results. Open any job from the
dashboard history. A single worker shares jobs fairly at range boundaries;
with multiple workers, assignments for different jobs run simultaneously.

### 6. Error handling

`curl.exe -i` shows the actual HTTP status. Using an input file avoids PowerShell
5.1 native-command quote handling differences:

```powershell
'{"data":"hello","difficulty":9}' | Set-Content -Encoding ascii .run/invalid.json
curl.exe -i -H 'Content-Type: application/json' --data-binary '@.run/invalid.json' "$base/jobs"
curl.exe -i "$base/jobs/unknown"
curl.exe -i -X DELETE "$base/jobs/$($cancelJob.job_id)"
```

Expected: 400, 404, then 409 for the already-cancelled job. Missing/empty data,
missing/null difficulty, decimals, malformed JSON, unknown fields and extra
JSON values also return 400. Requests are limited to 1 MiB.

### 7. Counts and chunk configuration

```powershell
.\scripts\stop-system.ps1
.\scripts\start-coordinator.ps1 -ChunkSize 100000
.\scripts\start-workers.ps1 -Count 1
Invoke-RestMethod http://localhost:8080/workers
.\scripts\stop-workers.ps1
.\scripts\start-workers.ps1 -Count 2
Invoke-RestMethod http://localhost:8080/workers
.\scripts\stop-workers.ps1
.\scripts\start-workers.ps1 -Count 4
.\scripts\stop-workers.ps1
.\scripts\start-workers.ps1 -Count 8
```

Stopped workers remain listed as dead after timeout; count `alive` entries, not
all historical registrations. Restart the coordinator for a completely clean
state. These are functionality checks; do not treat them as Part 4 experiments.
Later experiments can use identical difficulty-5 input, chunk/timeout settings
and a fresh coordinator for each 1/2/4/8-worker run. Record returned elapsed time
and attempt count; no benchmark comparison has been performed here.

## Automated tests

```powershell
. .\scripts\env.ps1
go mod tidy
go build ./...
go test -count=1 -v ./...
go test -race ./...
go vet ./...
```

Tests cover hashing/hex difficulty, REST, real TCP gRPC registration and search,
progress/exhaustion/cancellation, concurrent jobs, distinct ranges, retries,
heartbeat expiry, replacement/stale sessions, invalid results, premature EOF,
SSE subscribers/disconnections, zero nonce, overflow and concurrent state reads.

For Windows race testing, install a
64-bit MinGW-w64 toolchain, put its `bin` on PATH, and set:

```powershell
$env:CGO_ENABLED = '1'
$env:CC = 'gcc'
go test -race ./...
```

Do not put an unquoted path containing spaces in `CC`; select `gcc` via PATH.
The locally downloaded compiler is detected automatically by `env.ps1`.

Process-level launcher test (stop the current script-launched group first):

```powershell
.\scripts\stop-system.ps1
.\tests\launchers.ps1
.\tests\configuration.ps1
```

The launcher test starts a fresh coordinator/four workers on 18080/51051–51055
and stops them in a `finally` block. The configuration test verifies 1/2/4/8
registered processes, port sequences and lease expiry with chunk size 12345;
it performs no benchmark jobs. See [TESTING.md](TESTING.md) for actual evidence.

Optional browser tests use Playwright only as a development test tool, without
changing the dashboard's dependencies or requiring a frontend build. Start the
system, then use an existing Playwright installation:

```powershell
$env:PLAYWRIGHT_MODULE = 'C:\path\to\node_modules\playwright'
$env:CHROMIUM_PATH = 'C:\path\to\chrome.exe' # Optional if Playwright browser is installed
$env:POW_URL = 'http://127.0.0.1:8080'
node .\tests\dashboard.cjs
```

Alternatively install test tooling outside the project with
`npm install --prefix "$env:TEMP\pow-browser-tests" playwright` and
`& "$env:TEMP\pow-browser-tests\node_modules\.bin\playwright.cmd" install chromium`,
then set `PLAYWRIGHT_MODULE` to that installation. Node is not needed to run
the coordinator, workers or dashboard.

## Screenshots for the optional bonus

Actual browser captures are in [docs/screenshots](docs/screenshots):

- `desktop-ready.png`: submission form, four workers and empty job state.
- `desktop-running.png`: live SSE counts and distinct active worker ranges.
- `desktop-found.png`: independently verified nonce and full hash.
- `mobile-cancelled.png`: responsive layout and cancelled state at 390px width.

To capture presentation evidence yourself, open the dashboard, start a job,
take a running screenshot, then capture FOUND with the complete hash. Start a
second difficult job and cancel it. Use browser responsive mode at 390×844 for
mobile evidence. Capture `/workers` or its cards after killing one worker,
and the reassignment log. Browser test captures include intentional literal
`<script>` input to verify safe rendering; it is displayed as ordinary text.

## Troubleshooting and limitations

- **Port unavailable:** choose alternate ports as above. Windows may reserve
  ports without a visible listening process. Local verification used 18080
  because binding 8080 was refused in this environment.
- **Jobs stay RUNNING with zero checks:** inspect `/workers` and logs. Workers
  must register with the coordinator's gRPC address, not its REST address.
- **Duplicate ID:** use unique IDs or wait until the old session expires. A new
  process cannot replace a live session accidentally.
- **Worker marked dead:** inspect Search errors/timeouts and heartbeat settings.
  Its heartbeat loop re-registers after failure. A whole failed chunk retries.
- **SSE looks quiet:** `curl.exe -N` avoids client buffering; low-difficulty jobs
  can finish before the first periodic report. Late subscribers get FOUND.
- **Frontend change not visible:** assets are embedded at build time; rebuild
  and restart the coordinator. Assets use `Cache-Control: no-cache`.
- **Running jobs disappear after restart:** expected; state is in memory.
- **No solution yet:** high difficulty has variable completion time; there is
  no completion percentage, guaranteed duration or fixed benchmark result.
- Default listeners are loopback, with plaintext HTTP/gRPC for this local
  assignment. No authentication, persistence or coordinator crash recovery.
- Search uses the representable half-open uint64 domain `[0, 2^64-1)`.
  `2^64-1` is reserved as the final exclusive endpoint. Ranges never wrap;
  an exhausted entire domain would remain RUNNING rather than invent a proof.
- State/history is unbounded for this short-lived educational application.
  Attempt counters saturate at `uint64` maximum rather than wrapping.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the scheduling and cancellation design.
