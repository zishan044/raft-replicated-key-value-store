package raftio_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/sim"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func crashConfig() raftcore.Config {
	return raftcore.Config{ID: 1, Voters: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}
}

func crashInput(appendEntries bool) *raftv1.Message {
	msg := &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 7}
	if appendEntries {
		msg.Type, msg.LeaderCommit = raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, 2
		msg.Entries = []*raftv1.Entry{
			{Index: 1, Term: 7, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
			{Index: 2, Term: 7, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("command")},
		}
	}
	return msg
}

func crashNode(t *testing.T) (*sim.Disk, *wal.WAL, *raftcore.Core) {
	t.Helper()
	disk := sim.NewDisk()
	w, _, err := wal.OpenFS(disk, ".")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Save(&raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, state, err := wal.OpenFS(disk, ".")
	if err != nil {
		t.Fatal(err)
	}
	core, err := raftio.Restore(crashConfig(), state, func(*raftv1.Entry) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return disk, w, core
}

// Both the production processor's crash tests and the planted mutation use
// this check. Persisting the term alone does not justify a granted vote or ack.
func checkPublished(state wal.State, messages []*raftv1.Message) error {
	for _, msg := range messages {
		if msg.Term > state.HardState.Term {
			return errors.New("message preceded its persisted term")
		}
		if msg.Type == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE && !msg.Reject && state.HardState.Vote != msg.To {
			return errors.New("vote response preceded its persisted vote")
		}
		if msg.Type == raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE && !msg.Reject && msg.MatchIndex > uint64(len(state.Entries)) {
			return errors.New("append response preceded its persisted entries")
		}
	}
	return nil
}

var errCrashSignal = errors.New("simulated process exit")

func untilProcessExit(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if value := recover(); value != errCrashSignal {
			if value != nil {
				panic(value)
			}
			t.Fatal("process-exit cut was not reached")
		}
	}()
	fn()
}

func TestCrashAtReadyProcessingBoundaries(t *testing.T) {
	for _, appendEntries := range []bool{false, true} {
		cuts := []string{"before-save", "after-save", "before-send", "after-send", "before-advance", "after-advance"}
		if appendEntries {
			cuts = append(cuts, "before-apply-1", "after-apply-1", "before-apply-2", "after-apply-2")
		}
		for _, cut := range cuts {
			t.Run(fmt.Sprintf("append_%t/%s", appendEntries, cut), func(t *testing.T) {
				disk, writer, core := crashNode(t)
				point := func(stage string) {
					if stage == cut {
						if err := disk.Crash(0); err != nil {
							t.Fatal(err)
						}
						panic(errCrashSignal) // abandon the interrupted process, including its stack
					}
				}
				var sent []*raftv1.Message
				var volatileApplied []uint64
				p, err := raftio.New(core, func(batch *raftv1.WALBatch) error {
					point("before-save")
					err := writer.Save(batch)
					point("after-save")
					return err
				}, func(messages []*raftv1.Message) {
					point("before-send")
					sent = append(sent, messages...)
					point("after-send")
					if !appendEntries {
						point("before-advance")
					}
				}, func(entry *raftv1.Entry) error {
					point(fmt.Sprintf("before-apply-%d", entry.Index))
					volatileApplied = append(volatileApplied, entry.Index)
					point(fmt.Sprintf("after-apply-%d", entry.Index))
					if entry.Index == 2 {
						point("before-advance")
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := core.Step(crashInput(appendEntries)); err != nil {
					t.Fatal(err)
				}
				untilProcessExit(t, func() {
					if err := p.Process(); err != nil {
						t.Fatal(err)
					}
					point("after-advance")
				})
				if len(volatileApplied) > 0 && cut != "after-advance" && core.Status().AppliedIndex != 0 {
					t.Fatal("interrupted partial application advanced the old core")
				}
				_, state, err := wal.OpenFS(disk, ".")
				if err != nil {
					t.Fatal(err)
				}
				if err := checkPublished(state, sent); err != nil {
					t.Fatal(err)
				}
				applied := uint64(0)
				restored, err := raftio.Restore(crashConfig(), state, func(entry *raftv1.Entry) error {
					if entry.Index != applied+1 {
						t.Fatal("fresh application replay was not contiguous")
					}
					applied++
					return nil
				})
				if err != nil || restored.Status().AppliedIndex != state.HardState.Commit || applied != state.HardState.Commit {
					t.Fatalf("crash left an unreplayable application: %v", err)
				}
				if appendEntries && cut != "before-save" && applied != 2 {
					t.Fatal("committed batch was lost after its successful save")
				}
			})
		}
	}
}

func TestPersistAfterSendMutationIsDetected(t *testing.T) {
	for _, appendEntries := range []bool{false, true} {
		t.Run(fmt.Sprintf("append_%t", appendEntries), func(t *testing.T) {
			disk, _, core := crashNode(t)
			if err := core.Step(crashInput(appendEntries)); err != nil {
				t.Fatal(err)
			}
			// Deliberately wrong: release the real core's reply before saving its
			// Ready, then crash in that gap. This mutation stays in test code.
			sent := core.Ready().Messages
			if len(sent) != 1 || sent[0].Reject {
				t.Fatal("mutation fixture did not generate a granted response")
			}
			if err := disk.Crash(0); err != nil {
				t.Fatal(err)
			}
			_, state, err := wal.OpenFS(disk, ".")
			if err != nil {
				t.Fatal(err)
			}
			err = checkPublished(state, sent)
			want := "vote"
			if appendEntries {
				want = "append"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("persist-after-send mutation was not detected: %v", err)
			}
		})
	}
}
