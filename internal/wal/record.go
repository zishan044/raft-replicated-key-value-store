// Package wal defines the atomic WAL record format and logical recovery rules.
// Filesystem persistence is separate; decoding alone provides no durability.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

// Version 1 uses a 20-byte header followed by one protobuf WALBatch:
// magic "RWAL" (4), version (4), payload length (4), payload CRC32C (4),
// header CRC32C (4). Integers are little-endian; the header CRC covers the
// first 16 bytes, including the payload checksum. Verify it before trusting
// the length, so a flipped length cannot masquerade as an incomplete tail.
const (
	headerSize    = 20
	recordVersion = 1
)

var (
	ErrCorrupt = errors.New("corrupt WAL")
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	errTorn    = errors.New("incomplete WAL record")
)

// State is the recovered, uncompacted log and hard state. Start with its zero
// value and Replay segments in order. The caller owns its entries and data.
// Membership validation belongs to core startup, using the configured voters.
type State struct {
	HardState raftcore.HardState
	Entries   []*raftv1.Entry
}

// Encode returns one owned, checksummed record. It validates the batch's shape;
// transitions relative to earlier records are checked during replay. A batch
// must contain hard state or entries; a present zero hard state is valid.
func Encode(batch *raftv1.WALBatch) ([]byte, error) {
	if err := validateBatch(batch); err != nil {
		return nil, err
	}
	size := proto.Size(batch)
	if uint64(size) > math.MaxUint32 || size > int(^uint(0)>>1)-headerSize {
		return nil, errors.New("WAL payload exceeds record length limit")
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("encode WAL batch: %w", err)
	}
	record := make([]byte, headerSize+len(payload))
	copy(record[:4], "RWAL")
	binary.LittleEndian.PutUint32(record[4:8], recordVersion)
	binary.LittleEndian.PutUint32(record[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(record[12:16], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(record[16:20], crc32.Checksum(record[:16], crcTable))
	copy(record[headerSize:], payload)
	return record, nil
}

// Replay consumes a segment of exactly size bytes at the reader's current
// position. It returns the complete-record boundary to retain. Only an
// incomplete record at the end of the newest segment is repairable; no bytes
// are changed here. Physical truncation and sync belong to the file writer.
//
// Each batch is applied atomically. On error, State may contain earlier valid
// batches, but must not be used to restart a node. I/O errors are propagated,
// never treated as a repairable tail. The reader's size must come from its
// actual backing storage; record lengths are checked against it before allocation.
func (s *State) Replay(r io.Reader, size int64, newest bool) (int64, error) {
	if size < 0 {
		return 0, errors.New("negative WAL segment size")
	}
	var offset int64
	for offset < size {
		batch, count, err := decode(r, size-offset)
		if errors.Is(err, errTorn) && newest {
			return offset, nil
		}
		if err != nil {
			return offset, fmt.Errorf("WAL offset %d: %w", offset, err)
		}
		if err := s.apply(batch); err != nil {
			return offset, fmt.Errorf("%w at offset %d: %v", ErrCorrupt, offset, err)
		}
		offset += count
	}
	return offset, nil
}

func decode(r io.Reader, remaining int64) (*raftv1.WALBatch, int64, error) {
	if remaining < headerSize {
		return nil, 0, fmt.Errorf("%w: %w", ErrCorrupt, errTorn)
	}
	var header [headerSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(header[:16], crcTable) != binary.LittleEndian.Uint32(header[16:20]) {
		return nil, 0, fmt.Errorf("%w: header checksum mismatch", ErrCorrupt)
	}
	if string(header[:4]) != "RWAL" || binary.LittleEndian.Uint32(header[4:8]) != recordVersion {
		return nil, 0, fmt.Errorf("%w: unsupported record magic or version", ErrCorrupt)
	}
	length := int64(binary.LittleEndian.Uint32(header[8:12]))
	if length > remaining-headerSize {
		return nil, 0, fmt.Errorf("%w: %w", ErrCorrupt, errTorn)
	}
	if length > int64(int(^uint(0)>>1)) {
		return nil, 0, fmt.Errorf("%w: payload exceeds platform size limit", ErrCorrupt)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(header[12:16]) {
		return nil, 0, fmt.Errorf("%w: payload checksum mismatch", ErrCorrupt)
	}
	batch := new(raftv1.WALBatch)
	if err := proto.Unmarshal(payload, batch); err != nil {
		return nil, 0, fmt.Errorf("%w: invalid batch protobuf: %v", ErrCorrupt, err)
	}
	return batch, headerSize + length, nil
}

func validateBatch(batch *raftv1.WALBatch) error {
	if batch == nil || (batch.HardState == nil && len(batch.Entries) == 0) {
		return errors.New("WAL batch must contain hard state or entries")
	}
	term := uint64(math.MaxUint64)
	if hs := batch.HardState; hs != nil {
		term = hs.Term
		if term == 0 && (hs.Vote != 0 || hs.Commit != 0) {
			return errors.New("vote and commit require a positive term")
		}
	}
	if len(batch.Entries) == 0 {
		return nil
	}
	first := batch.Entries[0]
	if first == nil || first.Index == 0 {
		return errors.New("WAL suffix must start at a positive index")
	}
	return raftcore.ValidateEntries(batch.Entries, first.Index-1, 0, term)
}

func (s *State) apply(batch *raftv1.WALBatch) error {
	if err := validateBatch(batch); err != nil {
		return err
	}
	next := s.HardState
	if hs := batch.HardState; hs != nil {
		next = raftcore.HardState{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit}
	}
	if next.Term < s.HardState.Term || next.Commit < s.HardState.Commit {
		return errors.New("term and commit must not decrease")
	}
	if next.Term == s.HardState.Term && s.HardState.Vote != 0 && next.Vote != s.HardState.Vote {
		return errors.New("persisted vote must not change within a term")
	}
	last := uint64(len(s.Entries))
	if len(batch.Entries) > 0 {
		first := batch.Entries[0].Index
		if first > last+1 {
			return errors.New("WAL suffix leaves a gap")
		}
		var prevTerm uint64
		if first > 1 {
			prevTerm = s.Entries[first-2].Term
		}
		if err := raftcore.ValidateEntries(batch.Entries, first-1, prevTerm, next.Term); err != nil {
			return err
		}
		for _, entry := range batch.Entries {
			if entry.Index > last {
				break
			}
			old := s.Entries[entry.Index-1]
			if !proto.Equal(old, entry) && (entry.Index <= s.HardState.Commit || old.Term == entry.Term) {
				return fmt.Errorf("WAL suffix changes committed or same-term entry %d", entry.Index)
			}
		}
		last = batch.Entries[len(batch.Entries)-1].Index
	}
	if next.Commit > last {
		return errors.New("commit exceeds retained log")
	}
	// Validate the whole transition before changing either field.
	// ponytail: copy the prefix slice per batch to preserve old state; stage
	// in-place appends if profiling shows these copies dominate recovery.
	if len(batch.Entries) > 0 {
		prefix := batch.Entries[0].Index - 1
		entries := make([]*raftv1.Entry, int(prefix), int(last))
		copy(entries, s.Entries[:prefix])
		for _, entry := range batch.Entries {
			entries = append(entries, proto.Clone(entry).(*raftv1.Entry))
		}
		s.Entries = entries
	}
	s.HardState = next
	return nil
}
