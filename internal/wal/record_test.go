package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func entry(index, term uint64, data string) *raftv1.Entry {
	return &raftv1.Entry{Index: index, Term: term, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte(data)}
}

func encode(t *testing.T, batch *raftv1.WALBatch) []byte {
	t.Helper()
	record, err := Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// Construct framing independently of Encode, including for invalid protobuf
// and semantically invalid batches which the encoder intentionally rejects.
func frame(payload []byte) []byte {
	record := make([]byte, 20+len(payload))
	copy(record, "RWAL")
	binary.LittleEndian.PutUint32(record[4:], 1)
	binary.LittleEndian.PutUint32(record[8:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(record[12:], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	binary.LittleEndian.PutUint32(record[16:], crc32.Checksum(record[:16], crc32.MakeTable(crc32.Castagnoli)))
	copy(record[20:], payload)
	return record
}

func rawBatch(t *testing.T, batch *raftv1.WALBatch) []byte {
	t.Helper()
	payload, err := proto.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	return frame(payload)
}

func replay(t *testing.T, state *State, record []byte) {
	t.Helper()
	offset, err := state.Replay(bytes.NewReader(record), int64(len(record)), false)
	if err != nil || offset != int64(len(record)) {
		t.Fatalf("replay offset=%d error=%v", offset, err)
	}
}

func assertState(t *testing.T, got State, hs raftcore.HardState, entries []*raftv1.Entry) {
	t.Helper()
	if got.HardState != hs || len(got.Entries) != len(entries) {
		t.Fatalf("unexpected recovered state: %+v; want hard state %+v, %d entries", got, hs, len(entries))
	}
	for i := range entries {
		if !proto.Equal(got.Entries[i], entries[i]) {
			t.Fatalf("recovered entry %d: got %v, want %v", i+1, got.Entries[i], entries[i])
		}
	}
}

func TestRecordFormat(t *testing.T) {
	// A present zero hard state must survive protobuf presence and replay.
	batch := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{}}
	record := encode(t, batch)
	if len(record) != 22 || string(record[:4]) != "RWAL" ||
		binary.LittleEndian.Uint32(record[4:8]) != 1 ||
		binary.LittleEndian.Uint32(record[8:12]) != 2 ||
		hex.EncodeToString(record[20:]) != "0a00" {
		t.Fatalf("unexpected v1 framing: %x", record)
	}
	if !bytes.Equal(record, frame([]byte{0x0a, 0x00})) || !bytes.Equal(record, encode(t, batch)) {
		t.Fatal("record framing or deterministic encoding changed")
	}
	var state State
	replay(t, &state, record)
	assertState(t, state, raftcore.HardState{}, nil)
	for _, newest := range []bool{false, true} {
		if offset, err := state.Replay(bytes.NewReader(nil), 0, newest); offset != 0 || err != nil {
			t.Fatalf("empty segment: offset=%d error=%v", offset, err)
		}
	}
}

func TestReplayBatchesAndSegments(t *testing.T) {
	initial := []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "B"), entry(3, 2, "old"), entry(4, 2, "old tail")}
	replacement := entry(3, 3, "C")
	noop := &raftv1.Entry{Index: 4, Term: 4, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP}
	batches := []*raftv1.WALBatch{
		{HardState: &raftv1.PersistedHardState{Term: 2, Vote: 1, Commit: 1}, Entries: initial},
		{HardState: &raftv1.PersistedHardState{Term: 3, Commit: 1}},
		{Entries: []*raftv1.Entry{replacement}},
		{HardState: &raftv1.PersistedHardState{Term: 3, Vote: 2, Commit: 2}},
		{HardState: &raftv1.PersistedHardState{Term: 4, Vote: 1, Commit: 4}, Entries: []*raftv1.Entry{noop}},
		// An identical committed prefix may be retained again without changing it.
		{Entries: []*raftv1.Entry{initial[0], initial[1], replacement, noop}},
	}
	var state, combined State
	var records []byte
	for i, batch := range batches {
		record := encode(t, batch)
		replay(t, &state, record)
		records = append(records, record...)
		if i == 2 {
			assertState(t, state, raftcore.HardState{Term: 3, Commit: 1}, []*raftv1.Entry{initial[0], initial[1], replacement})
		}
	}
	replay(t, &combined, records)
	want := []*raftv1.Entry{initial[0], initial[1], replacement, noop}
	hs := raftcore.HardState{Term: 4, Vote: 1, Commit: 4}
	assertState(t, state, hs, want)
	assertState(t, combined, hs, want)
	// Persisted terms, votes and log are acceptable to the existing core.
	if _, err := raftcore.New(raftcore.Config{
		ID: 1, Voters: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2,
		HardState: state.HardState, Entries: state.Entries,
	}); err != nil {
		t.Fatalf("recovered state rejected by core: %v", err)
	}
	initial[0].Data[0] = 'x'
	replacement.Data[0] = 'x'
	records[20] ^= 1
	if string(state.Entries[0].Data) != "A" || string(combined.Entries[2].Data) != "C" {
		t.Fatal("recovery retained caller-owned input memory")
	}
}

func TestReplayIncompleteTailAtEveryByte(t *testing.T) {
	base := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 1, Commit: 1}, Entries: []*raftv1.Entry{entry(1, 1, "committed")}}
	prefix := encode(t, base)
	last := encode(t, &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 2, Vote: 2, Commit: 2},
		Entries:   []*raftv1.Entry{entry(2, 2, "new command")},
	})
	for cut := 0; cut < len(last); cut++ {
		data := append(bytes.Clone(prefix), last[:cut]...)
		for _, newest := range []bool{false, true} {
			var state State
			offset, err := state.Replay(bytes.NewReader(data), int64(len(data)), newest)
			if offset != int64(len(prefix)) {
				t.Fatalf("cut=%d newest=%t: retained partial batch at %d", cut, newest, offset)
			}
			if newest || cut == 0 {
				if err != nil {
					t.Fatalf("cut=%d newest=%t: %v", cut, newest, err)
				}
			} else if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("cut=%d: older segment damage accepted: %v", cut, err)
			}
			assertState(t, state, raftcore.HardState{Term: 1, Vote: 1, Commit: 1}, base.Entries)
		}
	}
	var complete State
	replay(t, &complete, append(bytes.Clone(prefix), last...))
	assertState(t, complete, raftcore.HardState{Term: 2, Vote: 2, Commit: 2}, append(base.Entries, entry(2, 2, "new command")))
}

func TestReplayRejectsEverySingleBitFlip(t *testing.T) {
	original := encode(t, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Vote: 2}, Entries: []*raftv1.Entry{entry(1, 1, "payload")}})
	for index := range original {
		for bit := range 8 {
			data := bytes.Clone(original)
			data[index] ^= 1 << bit
			for _, newest := range []bool{false, true} {
				var state State
				offset, err := state.Replay(bytes.NewReader(data), int64(len(data)), newest)
				if offset != 0 || !errors.Is(err, ErrCorrupt) {
					t.Fatalf("byte=%d bit=%d newest=%t: offset=%d error=%v", index, bit, newest, offset, err)
				}
				assertState(t, state, raftcore.HardState{}, nil)
			}
		}
	}
	// Corruption cannot be bypassed by discovering another valid record after it.
	bad := bytes.Clone(original)
	bad[len(bad)-1] ^= 1
	data := append(append(bytes.Clone(original), bad...), original...)
	var state State
	offset, err := state.Replay(bytes.NewReader(data), int64(len(data)), true)
	if offset != int64(len(original)) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("middle corruption: offset=%d error=%v", offset, err)
	}
}

func TestReplayRejectsInvalidFramingAndProtobuf(t *testing.T) {
	valid := encode(t, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{}})
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"unsupported version", func() []byte {
			data := bytes.Clone(valid)
			binary.LittleEndian.PutUint32(data[4:], 2)
			binary.LittleEndian.PutUint32(data[16:], crc32.Checksum(data[:16], crcTable))
			return data
		}()},
		{"unsupported magic", func() []byte {
			data := bytes.Clone(valid)
			copy(data, "NOPE")
			binary.LittleEndian.PutUint32(data[16:], crc32.Checksum(data[:16], crcTable))
			return data
		}()},
		{"malformed protobuf", frame([]byte{0xff})},
		{"empty batch", frame(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, newest := range []bool{false, true} {
				var state State
				if _, err := state.Replay(bytes.NewReader(tc.data), int64(len(tc.data)), newest); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("newest=%t: expected corruption, got %v", newest, err)
				}
			}
		})
	}
	// A valid header advertising 4 GiB must not allocate it when only two payload
	// bytes exist. This is a torn record, not permission to trust its length.
	binary.LittleEndian.PutUint32(valid[8:], math.MaxUint32)
	binary.LittleEndian.PutUint32(valid[16:], crc32.Checksum(valid[:16], crcTable))
	var state State
	if offset, err := state.Replay(bytes.NewReader(valid), int64(len(valid)), true); offset != 0 || err != nil {
		t.Fatalf("oversized incomplete record: offset=%d error=%v", offset, err)
	}
	if _, err := state.Replay(bytes.NewReader(valid), int64(len(valid)), false); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized older record: %v", err)
	}
}

func TestInvalidBatchesAreAtomic(t *testing.T) {
	base := &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 3, Vote: 2, Commit: 2},
		Entries:   []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "B"), entry(3, 2, "C")},
	}
	for _, tc := range []struct {
		name  string
		batch *raftv1.WALBatch
	}{
		{"empty", &raftv1.WALBatch{}},
		{"term regression", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 2, Vote: 2, Commit: 2}}},
		{"commit regression", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Vote: 2, Commit: 1}}},
		{"double vote", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Vote: 1, Commit: 2}}},
		{"clear vote", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Commit: 2}}},
		{"commit past log", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 4, Commit: 4}}},
		{"missing prefix", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(5, 3, "gap")}}},
		{"index gap", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 3, "C"), entry(5, 3, "gap")}}},
		{"decreasing terms", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 3, "C"), entry(4, 2, "D")}}},
		{"term below prefix", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 1, "old")}}},
		{"term above hard state", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 4, "future")}}},
		{"zero entry term", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 0, "zero")}}},
		{"replace committed", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(2, 3, "replacement")}}},
		{"change same term", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 2, "replacement")}}},
		{"truncate committed", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(1, 1, "A")}}},
		{"nil entry", &raftv1.WALBatch{Entries: []*raftv1.Entry{nil}}},
		{"zero index", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(0, 1, "zero")}}},
		{"index overflow", &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(math.MaxUint64, 1, "max"), entry(0, 1, "wrapped")}}},
		{"unknown entry type", &raftv1.WALBatch{Entries: []*raftv1.Entry{{Index: 3, Term: 3, Type: 99}}}},
		{"unspecified entry type", &raftv1.WALBatch{Entries: []*raftv1.Entry{{Index: 3, Term: 3}}}},
		{"no-op payload", &raftv1.WALBatch{Entries: []*raftv1.Entry{{Index: 3, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP, Data: []byte("bad")}}}},
		{"zero term vote", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Vote: 1}}},
		{"zero term commit", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Commit: 1}}},
		{"invalid suffix and new hard state", &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 4, Vote: 1, Commit: 3}, Entries: []*raftv1.Entry{entry(3, 5, "future")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state State
			replay(t, &state, encode(t, base))
			data := rawBatch(t, tc.batch)
			offset, err := state.Replay(bytes.NewReader(data), int64(len(data)), true)
			if offset != 0 || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("invalid transition accepted: offset=%d error=%v", offset, err)
			}
			assertState(t, state, raftcore.HardState{Term: 3, Vote: 2, Commit: 2}, base.Entries)
		})
	}
	if _, err := Encode(nil); err == nil {
		t.Fatal("nil batch encoded")
	}
	for _, batch := range []*raftv1.WALBatch{
		{}, {Entries: []*raftv1.Entry{nil}}, {Entries: []*raftv1.Entry{entry(0, 1, "zero")}},
		{Entries: []*raftv1.Entry{entry(1, 1, "A"), entry(3, 1, "gap")}},
		{HardState: &raftv1.PersistedHardState{Term: 1}, Entries: []*raftv1.Entry{entry(1, 2, "future")}},
		{HardState: &raftv1.PersistedHardState{Vote: 1}},
	} {
		if _, err := Encode(batch); err == nil {
			t.Fatalf("invalid batch encoded: %v", batch)
		}
	}
}

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }

func TestReplayPropagatesReadFailures(t *testing.T) {
	record := encode(t, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1}})
	diskErr := errors.New("disk read failed")
	for _, readErr := range []error{diskErr, io.ErrUnexpectedEOF, io.EOF} {
		for _, cut := range []int{0, 4, 20, len(record) - 1} {
			reader := io.MultiReader(bytes.NewReader(record[:cut]), failedReader{readErr})
			var state State
			if offset, err := state.Replay(reader, int64(len(record)), true); offset != 0 || err == nil {
				t.Fatalf("cut=%d read error=%v: offset=%d error=%v", cut, readErr, offset, err)
			} else if readErr == diskErr && !errors.Is(err, diskErr) {
				t.Fatalf("read failure not propagated: %v", err)
			}
			assertState(t, state, raftcore.HardState{}, nil)
		}
	}
	var state State
	if _, err := state.Replay(bytes.NewReader(nil), -1, true); err == nil {
		t.Fatal("negative segment size accepted")
	}
}

func TestApplyOwnsChangedEntriesAndPreservesPreviousState(t *testing.T) {
	base := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 2, Commit: 1}, Entries: []*raftv1.Entry{entry(1, 1, "A"), entry(2, 1, "B")}}
	var state State
	if err := state.apply(base); err != nil {
		t.Fatal(err)
	}
	previous := state
	changed := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Commit: 2}, Entries: []*raftv1.Entry{entry(2, 3, "C")}}
	if err := state.apply(changed); err != nil {
		t.Fatal(err)
	}
	changed.HardState.Commit = 0
	changed.Entries[0].Data[0] = 'x'
	changed.Entries[0] = nil
	assertState(t, state, raftcore.HardState{Term: 3, Commit: 2}, []*raftv1.Entry{entry(1, 1, "A"), entry(2, 3, "C")})
	assertState(t, previous, raftcore.HardState{Term: 2, Commit: 1}, base.Entries)
}

func FuzzReplay(f *testing.F) {
	f.Add([]byte{}, true)
	f.Add([]byte("RWAL"), false)
	f.Add(frame(nil), true)
	f.Add(frame([]byte{0xff}), true)
	batch := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 1, Commit: 1}, Entries: []*raftv1.Entry{entry(1, 1, "A")}}
	record, err := Encode(batch)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(record, true)
	f.Add(record[20:], true) // valid protobuf for the rechecksummed-payload path
	f.Add(append(bytes.Clone(record), record...), false)
	f.Add(record[:len(record)-1], true)
	f.Fuzz(func(t *testing.T, data []byte, newest bool) {
		// Exercise arbitrary framing AND arbitrary protobuf with valid checksums;
		// otherwise random mutations mostly stop at the header CRC check.
		for _, input := range [][]byte{data, frame(data)} {
			var state State
			offset, err := state.Replay(bytes.NewReader(input), int64(len(input)), newest)
			if offset < 0 || offset > int64(len(input)) {
				t.Fatalf("invalid replay boundary: %d", offset)
			}
			if err != nil {
				continue
			}
			if !newest && offset != int64(len(input)) {
				t.Fatal("repaired an older segment")
			}
			// The retained prefix must replay exactly, with no tail repair needed.
			var again State
			boundary, err := again.Replay(bytes.NewReader(input[:offset]), offset, false)
			if err != nil || boundary != offset || !reflect.DeepEqual(again, state) {
				t.Fatalf("non-repeatable recovery at %d: %v", offset, err)
			}
		}
	})
}
