package fdraft

import (
	"errors"
	"fmt"
	"os"

	"github.com/sasha-s/go-deadlock"
)

type Persister struct {
	mu        deadlock.Mutex
	id int
	raftstate []byte
	snapshot  []byte
	raftFile, snapshotFile string
}

func MakePersister(id int) *Persister {
	persister := new(Persister)
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	persister.id = id
	persister.raftFile = fmt.Sprintf("%s/files/raft_%d",dir , id)
	persister.snapshotFile = fmt.Sprintf("%s/files/snapshot_%d", dir, id)
	return persister
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister(ps.id)
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	np.raftFile = ps.raftFile
	np.snapshotFile = ps.snapshotFile
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	tempFile := fmt.Sprintf("%s-temp", ps.raftFile)
	// write to temp file and replace the existing file 
	if err := os.WriteFile(tempFile, state, 0666); err != nil {
		panic(err)
	}
	if err := os.Rename(tempFile, ps.raftFile); err != nil {
		panic(err)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, err := os.ReadFile(ps.raftFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make([]byte, 0)
		}
		fmt.Print(err)
		panic(err)
	}
	return data
	// return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	fileInfo, err := os.Stat(ps.raftFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		panic(err)
	}
	return fileInfo.Size()
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	tempRaftFile := fmt.Sprintf("%s-temp", ps.raftFile)
	// write to temp file and replace the existing file 
	if err := os.WriteFile(tempRaftFile, state, 0666); err != nil {
		panic(err)
	}
	if err := os.Rename(tempRaftFile, ps.raftFile); err != nil {
		panic(err)
	}
	tempSnapFile := fmt.Sprintf("%s-temp", ps.snapshotFile)
	// write to temp file and replace the existing file 
	if err := os.WriteFile(tempSnapFile, state, 0666); err != nil {
		panic(err)
	}
	if err := os.Rename(tempSnapFile, ps.snapshotFile); err != nil {
		panic(err)
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	data, err := os.ReadFile(ps.snapshotFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make([]byte, 0)
		}
		panic(err)
	}
	return data
}

func (ps *Persister) SnapshotSize() int64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	fileInfo, err := os.Stat(ps.snapshotFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		panic(err)
	}
	return fileInfo.Size()
}

