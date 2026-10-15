# raft-replicated-key-value-store

A Go project working toward a replicated, linearizable key-value store with Raft
consensus implemented from scratch, gRPC, durable storage, and deterministic
fault testing.

## Current milestone: M0 skeleton

The node starts an HTTP server with JSON logs. The repository also contains
versioned peer protobufs, generated Go types, and provisional `Clock`, `Ticker`,
and `Transport` interfaces. Raft, KV operations, persistence, working peer
transport, metrics, and a cluster are not implemented yet.

## Setup

Development checks target Linux. Install Go **1.26.3**, GNU Make, Git, curl, tar,
coreutils (`sha256sum`, `install`), diffutils, and a C compiler for Go's race detector.
Ensure `go` is on your `PATH`; for a standard Go installation:

```bash
export PATH="/usr/local/go/bin:$PATH"
```

From the repository root:

```bash
make tools
make check
```

`make tools` downloads release binaries into ignored `bin/` and verifies their
published SHA256 checksums. It does not install tools globally. Re-run it when
tool versions change. Initial setup and Go dependency downloads require network
access.

| Tool | Pinned version |
| --- | --- |
| [Buf](https://github.com/bufbuild/buf/releases/tag/v1.74.0) | 1.74.0 |
| [golangci-lint](https://github.com/golangci/golangci-lint/releases/tag/v2.14.0) | 2.14.0 |
| protobuf Go generator and runtime | 1.36.12, pinned together in `go.mod` |

## Run a node

```bash
make build
./bin/kvd
```

`make run` also starts a node using `go run`. Defaults are node ID `1` and HTTP
address `127.0.0.1:8080`. To override them:

```bash
./bin/kvd --node-id 2 --http-addr 127.0.0.1:8081
```

Node IDs must be positive. Invalid IDs and startup failures exit unsuccessfully.
Logs are JSON on stderr and include `node_id`.

In another terminal, check the default node:

```bash
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/readyz
```

| Endpoint | M0 response | Meaning |
| --- | --- | --- |
| `GET /healthz` | `200`, `ok` | The process is serving HTTP. |
| `GET /readyz` | `503`, `not ready` | The node cannot serve KV requests yet. |

Other methods on these endpoints return `405` with `Allow: GET`. Readiness stays
unavailable throughout M0; it will eventually require a known leader and a
caught-up state machine.

Press Ctrl-C or send SIGTERM to shut down. The server stops accepting connections
and gives active requests up to five seconds to finish.

## Review and development commands

| Command | Purpose |
| --- | --- |
| `make build` | Build `bin/kvd`. |
| `make run` | Start the node with default flags. |
| `make test` | Run `go test -race ./...`. |
| `make fmt` | Format Go and protobuf source. |
| `make fmt-check` | Check formatting without rewriting files. |
| `make proto-lint` | Run Buf's standard protobuf lint rules. |
| `make proto` | Regenerate Go protobuf types. |
| `make proto-check` | Generate into a temporary directory and compare with `pkg/api`. |
| `make lint` | Run golangci-lint's default linters. |
| `make check` | Run formatting, protobuf, lint, race-test, and build checks. |

Edit `proto/raft/v1/raft.proto`, then run `make proto` and include the generated
`pkg/api/raft/v1/raft.pb.go` in the same change. Do not edit generated files by
hand. Generation runs the Go generator from the protobuf module pinned in
`go.mod`; it needs neither a separate `protoc` installation nor a gRPC plugin.
`make proto-check` detects changed, missing, or extra generated files without
rewriting the working tree.

GitHub Actions runs the same `make tools` and `make check` commands on pushes to
`main`, pull requests, and manual workflow dispatches. Simulation checks will be
added with the simulator. M0 acceptance requires passing local checks, a node
serving `/healthz`, and a successful GitHub CI run; local checks alone do not
verify remote CI.
