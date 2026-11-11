# Product

## Platform and stack
Local web dashboard, embedded in Go, using HTML, CSS and vanilla JavaScript.
No frontend build step, separate server, cloud service or database.

## Users and purpose
A computer engineering student demonstrates distributed nonce searches for
ENCS5324. Submit work, see real progress, inspect independent workers, cancel
searches and revisit concurrent jobs. Every result must be verifiable.

## Constraints
Preserve REST/gRPC contracts. Use native EventSource. Work on desktop and mobile.
Expose running, found, cancelled, empty, disconnected and API error states.
No simulated progress, fabricated measurements or Part 4 benchmark experiments.
