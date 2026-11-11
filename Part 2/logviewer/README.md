# Live Log Viewer — Part 2

A small Go application for Part 2 of ENCS5324 Assignment #1. A gRPC client requests server information and watches five simulated application logs. Part 3 is a separate application in this repository; see the [repository guide](../../README.md).

```text
Go client                       Go server (localhost:50051)
   | -- GetServerInfo(request) --> |
   | <-- name and status -------- |  Unary: one response
   |                              |
   | -- WatchLogs(service_name) -> |
   | <-- INFO log --------------- |  Streaming: individual responses
   |       about one second       |
   | <-- INFO log --------------- |
   | <-- WARNING log ------------ |
   | <-- ERROR log -------------- |
   | <-- INFO log --------------- |
   | <-- normal end (EOF) ------- |
```

## How the RPCs work

**Unary RPC:** `GetServerInfo` takes an empty `InfoRequest` and returns one `InfoResponse`: name `Log Server`, status `RUNNING`.

**Server-streaming RPC:** `WatchLogs` takes one `LogRequest` with a service name. The server sends the first log immediately, then four more about one second apart. Each response contains a level and a message. The client calls `Recv()` repeatedly and prints each response immediately. `io.EOF` means the stream completed successfully.

The service name labels the simulated log source; it does not connect to a real service. All service names use the same five sample events. The client defaults to `demo-app`. RPC deadlines and Ctrl+C cancel the stream. The server checks cancellation while waiting and stops sending when the client disconnects.

## Files

| File | Purpose |
| --- | --- |
| `go.mod`, `go.sum` | Module definition, dependency versions, and checksums |
| `proto/log.proto` | Messages and RPC definitions |
| `proto/log.pb.go` | Generated Go message types; do not edit manually |
| `proto/log_grpc.pb.go` | Generated gRPC client/server interfaces; do not edit manually |
| `server/main.go` | RPC handlers, sample logs, listener, and shutdown |
| `server/main_test.go` | Real local gRPC tests for info, stream timing, EOF, and cancellation |
| `client/main.go` | Connection, unary call, stream display, timeouts, and Ctrl+C |
| `README.md` | Setup, explanation, and manual testing instructions |

## Tools and setup (Windows, Linux, or macOS)

Install Go 1.26 or newer and make sure `go version` works in a new terminal. Generated protobuf files are included, so running, building, and testing need only Go. `protoc` and the Go plugins below are optional, used only after changing the schema. Dependencies are pinned in `go.mod` and `go.sum`; there are no databases or external services.

From the cloned repository root, open a terminal and run:

```sh
cd "Part 2/logviewer"
go version
go mod download
go build ./...
```

The first dependency download requires internet. To install Go on Windows, you can use `winget install --id GoLang.Go -e`, then reopen the terminal. Downloaded portable tools and prebuilt executables are excluded from the repository.

## Generate Protocol Buffers code (optional)

Skip this section for a normal run. To regenerate after editing `proto/log.proto`, install `protoc` from [Protocol Buffers releases](https://github.com/protocolbuffers/protobuf/releases) and put it on PATH. From `Part 2/logviewer`, run the following in PowerShell:

```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
$pluginDir = if ($env:GOBIN) { $env:GOBIN } else { Join-Path (go env GOPATH) 'bin' }
$env:PATH = "$pluginDir;$env:PATH"
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/log.proto
go mod tidy
go build ./...
go test -count=1 -v ./...
```

## Run the server

In the first terminal, run from `Part 2/logviewer`:

```powershell
go run ./server
```

Expect `Log Server listening on localhost:50051`. Keep this terminal open. It will show client addresses, RPC calls, sent logs, and completion or cancellation.

## Run the client

In a second terminal, change into `Part 2/logviewer` from the repository root, then run:

```powershell
go run ./client
# Optional: select a simulated service.
go run ./client -service payments
```

Expected output:

```text
===== Unary RPC =====
Server Name: Log Server
Status: RUNNING

===== Server Streaming RPC =====
Watching logs for demo-app...

[INFO] Server started
[INFO] User connected
[WARNING] High memory usage
[ERROR] Database connection failed
[INFO] Connection restored

Stream completed successfully.
```

The five log lines should appear individually over about four seconds. There is no extra wait after the final log. The client uses a five-second unary deadline (including connection establishment) and a fifteen-second stream deadline. An unavailable server produces an error and a nonzero exit code.

## Stop and test cancellation

Press **Ctrl+C** in the server terminal to stop it. Windows **Ctrl+Break** also sends an interrupt handled by this program. The server allows active RPCs up to five seconds to finish, then forces shutdown if necessary. Press **Ctrl+C** in the client terminal while logs are arriving to cancel; expect `Stream canceled.` and a cancellation message in the server terminal. Start the client again to confirm the server still works.

The automated tests start their own server on a temporary local port and clean it up afterward:

```powershell
go mod tidy
go build ./...
go test -count=1 -v ./...
```

The streaming test records receive timestamps, checks spacing, requires at least five different messages and all three levels, and checks normal EOF. Cancellation and deadline tests also check that the server handler returns promptly.

## Verification performed

- Generated both Go protobuf files with the compiler and plugins above.
- `go mod tidy`, `go build ./...`, `go test -count=1 -v ./...`, and `go vet ./...` passed. There are four server integration tests; the client and generated package compile but have no separate unit tests.
- Built `bin/server.exe` and `bin/client.exe`, started the server, and ran the actual client. Verified the unary response, five individual messages approximately one second apart, and successful stream completion.
- Verified an unavailable server produces a useful client error and exit code 1.
- Verified graceful server shutdown with a Windows Ctrl+Break console interrupt. Ctrl+C delivery through the automated terminal was unreliable; interactive client Ctrl+C is left for manual checking. RPC context cancellation and deadline expiry were verified automatically.
- Prebuilt executables and local tools are excluded from GitHub. Use `go run ./server` and `go run ./client` as shown above.

Initial setup attempts encountered restricted network access and an incomplete Go extraction. These were resolved before successful generation, builds, and tests. Initial tests also failed with `Unimplemented` before the handlers were added, then passed after implementation.

## Troubleshooting

- **`go` is not recognized:** install Go, add it to PATH, and reopen the terminal. `protoc` is only needed for optional regeneration.
- **`protoc-gen-go` / `protoc-gen-go-grpc` not found:** run the two `go install` commands and add the plugin directory to PATH.
- **Connection refused, unavailable, or deadline exceeded:** start the server first and check that both applications use `localhost:50051`. Dependency downloads need internet access; the running application does not.
- **Address already in use:** stop the older server with Ctrl+C. Use `Get-NetTCPConnection -LocalPort 50051` to identify the listener.
- **No `go.mod` found:** change into `Part 2/logviewer` from the repository root before running Go commands.
- **Part 3 cannot start on port 50051:** stop this part's server first. Both parts use the same default gRPC port.
- **Generated imports fail after editing the proto:** regenerate both files, then run `go mod tidy` and `go build ./...`.
- **Stream canceled:** normal after client Ctrl+C. Restart the client for another sample stream.

## Assignment screenshots

1. The `.proto` file showing unary and server-streaming RPC definitions.
2. Two terminals together: server startup/activity and complete client output showing both RPC sections and successful completion.
3. `go build ./...` and `go test -count=1 -v ./...` output, including streaming timestamps and passing cancellation test.
4. Optionally, the client cancellation message alongside the server cancellation log.
