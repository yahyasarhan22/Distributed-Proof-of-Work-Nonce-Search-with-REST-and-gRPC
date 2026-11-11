# Part 3 implementation plan

The supplied assignment is the specification. Work stays inside Part 3;
Part 2 is read only. No Part 4 measurements will be taken.

1. Define protobuf unary registration/heartbeat and server streaming search.
   Generate checked-in Go bindings using the existing Part 2 tools.
2. Test SHA-256/decimal nonce/hex difficulty, then implement `internal/pow`.
3. Test REST validation and state transitions, then implement coordinator state,
   fair round-robin scheduling, retry queues, sessions, heartbeats and SSE.
4. Test real gRPC search/progress/cancellation, then implement worker service
   and registration lifecycle. Add signal-aware command entry points.
5. Exercise real TCP integration: distinct ranges, concurrent jobs, cancellation,
   worker errors/timeouts/restarts, stale results and independent validation.
6. Add PowerShell launch/stop tools, demonstrate separate processes, run tidy,
   build, verbose tests, race tests and vet, and record actual evidence.
7. Finish README and architecture guide with manual scenarios and limitations.

Design: one coordinator mutex protects jobs, worker sessions and assignments.
Network I/O runs outside the mutex. A wake channel and periodic maintenance
drive scheduling without spinning. Each worker has one active RPC. Job ranges
are half-open uint64 intervals; failed assignments retry the entire range.
Session and assignment identifiers fence stale messages. Checked counts are
cumulative reported attempts (including retries), not unique nonce coverage;
unreported work lost to a crash cannot be counted. Terminal snapshots freeze.
SSE uses bounded latest-value channels and a frozen terminal snapshot.
