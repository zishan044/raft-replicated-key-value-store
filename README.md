# raft-replicated-key-value-store

A Go project working toward a replicated, linearizable key-value store with Raft
consensus implemented from scratch, gRPC, durable storage, and deterministic
fault testing.

## Current milestone: M1 elections and simulator v0

The pure Raft core implements RequestVote, seeded election timeouts, leader
heartbeats, and a `Ready`/`Advance` output contract. A deterministic simulator
drives in-memory nodes and injects network faults while checking election safety.

The `kvd` binary still serves the M0 HTTP skeleton with JSON logs. KV operations,
log replication, durable storage, real peer networking, and metrics are not
implemented yet. The provisional `Clock`, `Ticker`, and `Transport` interfaces
remain available for later node integration.

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

| Endpoint | Current response | Meaning |
| --- | --- | --- |
| `GET /healthz` | `200`, `ok` | The process is serving HTTP. |
| `GET /readyz` | `503`, `not ready` | The node cannot serve KV requests yet. |

Other methods on these endpoints return `405` with `Allow: GET`. Readiness stays
unavailable throughout M1; it will eventually require a known leader and a
caught-up state machine.

Press Ctrl-C or send SIGTERM to shut down. The server stops accepting connections
and gives active requests up to five seconds to finish.

## Election simulator

```bash
make sim                    # 10,000 seeds, starting at zero
make sim SEEDS=500           # smaller uncached campaign
make sim SEED=123            # one seed with its full event trace
```

`SEEDS` must be positive. `SEED` selects one unsigned 64-bit seed, including zero,
and takes precedence over `SEEDS`. To save a replay, redirect its output:
`make sim SEED=123 > /tmp/raft-seed-123.log`. A failure prints the seed, the replay
command, and the full trace. Reproduction requires the same code and pinned Go
toolchain. These commands use Go tests with the race detector and disable the
test-result cache; no additional binary or dependency is required.

Even seeds use three nodes and odd seeds use five. Each successive pair rotates
through healthy delivery, loss, delay, duplication, reordering, symmetric
partitions, asymmetric partitions, leader isolation, combined changing faults,
and a blackout that forces split votes. Delay schedules also include a fixed
20-tick delay to exercise elections before vote traffic can arrive. Leader
isolation first establishes a leader, then checks that the connected majority
elects its replacement.

Every schedule has a 200-tick fault phase. Healing restores reliable one-tick
delivery and drains old queued messages on the next tick without discarding
them. After that drain, all nodes must agree on one leader within 200 ticks;
the suite checks agreement at that deadline and requires the same leader and
term on every tick for another 40 ticks. Election deadlines are 10–19 ticks and
heartbeats occur every two ticks. Simulation uses logical time without sleeps.

After every core input, the simulator checks that no two nodes have ever become
leader in the same term, remembering old leaders after they step down. An
isolated old leader can retain its role in an older term until higher-term
contact arrives; CheckQuorum and PreVote are deferred to M6. Traces record the
seed, configuration, faults, message scheduling/delivery, saved election state,
and leadership transitions. Identical seeded schedules produce identical traces.

Term and vote are saved in memory before dependent messages are queued. There is
no crash durability, crash/restart simulation, or disk fault model yet (M3).
These tests establish election safety and bounded recovery for the tested
schedules, not log safety, KV linearizability, or a universal randomized-election
deadline. M1 acceptance requires a passing 10,000-seed campaign and repository
checks; real-process cluster demonstrations come later.

## Review and development commands

| Command | Purpose |
| --- | --- |
| `make build` | Build `bin/kvd`. |
| `make run` | Start the node with default flags. |
| `make test` | Run `go test -race ./...`, including 500 simulation seeds. |
| `make sim` | Run 10,000 simulation seeds without cached results. |
| `make sim SEEDS=500` | Run the first 500 simulation seeds without cached results. |
| `make sim SEED=123` | Replay one seed with its full event trace. |
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

GitHub Actions runs `make tools` and `make check` on pushes to `main`, pull
requests, manual workflow dispatches, and its nightly schedule. These checks
include 500 simulation seeds. Nightly runs also execute `make sim SEEDS=20000`
without cached results. Local checks alone do not verify hosted CI execution.
