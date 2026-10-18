# raft-replicated-key-value-store

A Go project working toward a replicated, linearizable key-value store with Raft
consensus implemented from scratch, gRPC, durable storage, and deterministic
fault testing.

## Current status: M2 complete; M3 in progress

The pure Raft core implements elections, an owned in-memory log, AppendEntries,
conflict hints, leader proposals, an election no-op, current-term majority
commitment, and ordered application through `Ready`/`Advance`. Idle leaders send
empty AppendEntries. A deterministic simulator submits commands under network
faults and checks all five Raft safety invariants.

The `kvd` binary still serves the M0 HTTP skeleton with JSON logs. KV operations,
node persistence integration, real peer networking, and metrics are not
implemented yet. The provisional `Clock`, `Ticker`, and `Transport` interfaces
remain available for later node integration.

M3 subset 1 adds the WAL record codec and logical recovery in `internal/wal`.
Each record holds one protobuf persistence batch with optional hard state and a
replacement log suffix. A versioned 20-byte header protects both metadata and
payload with CRC32C. Recovery accepts complete batches atomically, preserves
committed entries, and reports the last complete byte offset when the newest
segment ends in an incomplete record. Complete-record checksum failures and
incomplete older segments are rejected.

M3 subset 2 adds the segmented filesystem WAL. `wal.Open(dir)` returns a writer
and an independent recovered state; `Save(batch)` validates the transition,
appends one record, and syncs before succeeding. Segments grow on demand toward
64 MiB, use consecutive names such as `00000000000000000001.wal`, and rotate
without splitting records. An oversized record occupies its own segment.
Creating a segment also requires a directory sync. Recovery truncates incomplete
newest tails, then syncs the newest segment and directory before returning.

The WAL directory must already exist, its creation must be durable, and it must
be exclusively owned and contain only segments. `Open` does not create or lock
it. Failed writes or syncs disable further saves until close and recovery; a
failed save may survive as a complete batch, so its outcome is uncertain.
`OpenFS` accepts the file-operations boundary for disk fault testing.

M3 subset 3 adds `internal/raftio`, a synchronous processor shared by the
simulator and real-file integration tests. After each core input, it processes
`Ready` in this order: durably save changed hard state and entries, release
messages, apply committed entries (including no-op positions), then `Advance`.
Empty and messages-only batches skip persistence. A save or apply failure is
terminal: the processor will not retry or advance the batch, and the pending
`Ready` prevents the core from accepting further input.

`raftio.Restore` builds a fresh follower from recovered storage, replays its
committed prefix into an empty application, and consumes the initial batch
without rewriting the WAL. Uncommitted entries remain in the log for later
replication. Partial application must be discarded before restarting. The
simulator still retains data in memory; simulated disk faults and node lifecycle
controls arrive in subset 4. `kvd` still serves only its HTTP skeleton.

Run the focused checks and decoder fuzzing with:

```bash
go test -race ./internal/wal ./internal/raftio
go test ./internal/wal -run '^$' -fuzz '^FuzzReplay$' -fuzztime=30s
```

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
unavailable throughout M2; it will eventually require a known leader and a
caught-up state machine.

Press Ctrl-C or send SIGTERM to shut down. The server stops accepting connections
and gives active requests up to five seconds to finish.

## Replication simulator

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
20-tick delay to exercise elections before vote traffic arrives. Leader isolation
first establishes a leader and replicates a baseline command, then checks that
the connected majority elects its replacement.

Every schedule has a 200-tick fault phase. Every ten ticks, each node that calls
itself leader receives a unique command; a seeded random target also receives a
proposal to exercise follower rejection. This includes both an isolated old
leader and its replacement. Local acceptance returns an index and term; it does
not promise commitment or application. A minority's accepted commands may be
replaced after healing.

Healing restores reliable one-tick delivery and drains old queued messages on
the next tick without discarding them. During the next 200 ticks, the suite
submits a fresh marker whenever all nodes agree on a recovered leader. At the
deadline, every node must agree on the leader and have applied that leader's
marker with an identical preceding entry sequence. The same leader and term
must remain on every node for another 40 ticks. Election deadlines are 10–19
ticks and heartbeat appends occur every two ticks. Simulation uses logical time
without sleeps.

The simulator checks these invariants after core inputs, reusing immutable log
snapshots while logs are unchanged:

- **Election Safety:** one leader per term, including leaders already deposed.
- **Leader Append-Only:** a leader retains its earlier entries throughout its term.
- **Log Matching:** equal index and term imply identical entries and prefixes.
- **Leader Completeness:** later leaders contain previously committed entries,
  including when an older commitment is observed after a later leader steps down.
- **State Machine Safety:** every application at an index agrees with the
  historical application at that index, across nodes and leadership changes.

Historical leader logs, committed entries, and applied entries are retained for
the whole run. Focused tests plant invalid observations to prove the checkers
reject them. Traces include proposal payloads and outcomes, append positions,
conflict hints, retained entries, commit advances, application, faults and
leadership changes. Identical seeded schedules produce identical event traces.
An isolated leader can retain its role in an older term until higher-term
contact arrives; CheckQuorum and PreVote remain deferred to M6.

The core copies proposal data and constructor entries. `Log()` and `Ready()`
return independent snapshots. Consumers retain a `Ready` batch's entries and
hard state before sending its messages, apply its committed entries in order,
then call `Advance`. No-op entries advance the applied position without
executing a command. Retrying `Ready` returns the same batch; consumers must
track completed application when resuming a partially processed batch. Initial
entries are emitted for retention, and their committed prefix is replayed for
application. All retention is currently in memory.

M2 acceptance requires repository checks and a passing uncached 10,000-seed
campaign. Unit tests cover all six Figure 7 log shapes and both Figure 8
commitment branches from the [Raft paper](https://raft.github.io/raft.pdf), with
real votes and follower responses. Figure 8 holds back the generated no-op by
delivering a valid prefix of a captured append batch to isolate the commit rule
from full-suffix batching; automatic election no-ops remain enabled.

These tests establish log safety and bounded recovery for the tested schedules.
They do not establish KV linearizability or a universal randomized-election
recovery deadline. Crash durability, restart simulation, and disk faults belong
to M3; the KV API and client sessions belong to M4. Bounded batches, pipelining,
and proposal backpressure are deferred. Real-process cluster demonstrations
come later.

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
include 500 replication simulation seeds. Nightly runs also execute `make sim SEEDS=20000`
without cached results. Local checks alone do not verify hosted CI execution.
