package sim

import (
	"bytes"
	"fmt"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

type nodeView struct {
	status  raftcore.Status
	entries []*raftv1.Entry // owned, immutable snapshot, refreshed on log changes
}

type commitRecord struct {
	entry *raftv1.Entry
	term  uint64 // term in which commitment was first observed, not the entry's term
}

func sameEntry(a, b *raftv1.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Type == b.Type && bytes.Equal(a.Data, b.Data)
}

func (s *Simulator) checkLogs(view nodeView, changed bool) error {
	status, entries := view.status, view.entries
	previous := s.observed[status.ID]
	if status.LastLogIndex != uint64(len(entries)) || status.HardState.Commit > uint64(len(entries)) ||
		status.HardState.Commit < previous.status.HardState.Commit || status.AppliedIndex > status.HardState.Commit {
		return fmt.Errorf("node %d: invalid log, commit or applied bounds", status.ID)
	}
	if err := s.checkLeader(status); err != nil {
		return err
	}
	s.observed[status.ID] = view
	newLeader := status.Role == raftcore.Leader &&
		(previous.status.Role != raftcore.Leader || previous.status.HardState.Term != status.HardState.Term)
	if status.Role == raftcore.Leader && (changed || newLeader) {
		old := s.leaderLogs[status.HardState.Term]
		if len(entries) < len(old) {
			return fmt.Errorf("leader append-only: node %d truncated its log in term %d", status.ID, status.HardState.Term)
		}
		for i, entry := range old {
			if !sameEntry(entry, entries[i]) {
				return fmt.Errorf("leader append-only: node %d overwrote index %d in term %d", status.ID, i+1, status.HardState.Term)
			}
		}
		s.leaderLogs[status.HardState.Term] = entries
	}
	if changed {
		for _, peer := range s.voters {
			if peer == status.ID {
				continue
			}
			other := s.observed[peer].entries
			prefixMatches := true
			for i := 0; i < min(len(entries), len(other)); i++ {
				prefixMatches = sameEntry(entries[i], other[i]) && prefixMatches
				if entries[i].Term == other[i].Term && !prefixMatches {
					return fmt.Errorf("log matching: nodes %d and %d disagree through index %d at term %d", status.ID, peer, i+1, entries[i].Term)
				}
			}
		}
	}
	newCommit := status.HardState.Commit != previous.status.HardState.Commit
	if changed || newCommit {
		for i, entry := range entries[:status.HardState.Commit] {
			if i < len(s.committed) {
				if !sameEntry(s.committed[i].entry, entry) {
					return fmt.Errorf("leader completeness: conflicting commitments at index %d", i+1)
				}
			} else {
				s.committed = append(s.committed, commitRecord{entry: entry, term: status.HardState.Term})
			}
		}
	}
	// Unchanged snapshots preserve previous matching/append-only checks. Check
	// history whenever logs, commitment or leadership change, including leaders
	// already deposed before an older leader's delayed acknowledgment commits.
	if changed || newCommit || newLeader {
		for _, term := range s.leaderTerms {
			log := s.leaderLogs[term]
			for i, record := range s.committed {
				if term > record.term && (i >= len(log) || !sameEntry(log[i], record.entry)) {
					return fmt.Errorf("leader completeness: node %d in term %d lacks committed index %d from term %d", s.leaders[term], term, i+1, record.term)
				}
			}
		}
	}
	return nil
}

func (s *Simulator) checkApplied(id uint64, entry *raftv1.Entry) error {
	if entry.Index == uint64(len(s.applied))+1 {
		s.applied = append(s.applied, entry)
	} else if entry.Index == 0 || entry.Index > uint64(len(s.applied)) || !sameEntry(s.applied[entry.Index-1], entry) {
		return fmt.Errorf("state machine safety: node %d applies a different entry at index %d", id, entry.Index)
	}
	return nil
}
