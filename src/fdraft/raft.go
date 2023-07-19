package fdraft

// TODO: change the call function for request vote, append entries and install snapshot.

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	labgob "encoding/gob"
	ewma "fd-raft/fdraft/ewma"
	lablog "fd-raft/fdraft/logger"

	"github.com/sasha-s/go-deadlock"
	"golang.org/x/exp/slices"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Type         int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	LEADER_HEARTBEAT_TIME        time.Duration = 100
	ELECTION_TIMEOUT_BASE        time.Duration = 600 // TODO: Test with 300 the timer was running out too quickly.
	NULL_VALUE                   int           = -1
	RANDOM_RANGE                               = 400
	SNAPSHOT_CACHE_SIZE                        = 20
	QUORUM_CHECK_TIME            time.Duration = 4000 * time.Millisecond // Time interval to check the quorum performance.
	RPC_TIMEOUT_DURATION         time.Duration = 1500 * time.Millisecond
	SNAPSHOT_TIMEOUT_DURATION    time.Duration = 5000 * time.Millisecond
	BATCHER_DURATION             time.Duration = 5
	RESPONSE_LATENCY_REQUIREMENT float64       = 30 * 1000000
	COMMIT_QUORUM_ONLY           int           = iota
	ALL_NODES
	COMMIT_QUORUM_SIZE int = 10 // Actual Commit Quorum Leader + commit quorum
	MAX_UNANSWERED_RPC int = 20
	CLIENT_OPERATION   int = iota
	NO_OP
)

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex // Lock to protect shared access to this peer's state
	peers     []Node         // RPC end points of all peers
	persister *Persister     // Object to hold this peer's persisted state
	me        int            // this peer's index into peers[]
	dead      int32          // set by Kill()
	rpcServer *Server
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm, LastKnownLeader                      int
	LeaderId, VotedFor, commitIndex, lastApplied      int
	electionTimeoutDuration                           time.Duration
	lastHeartBeatTime, electionTime, lastContact      time.Time
	nextIndex, matchIndex                             map[int]int
	Logs                                              []Log
	applyCh                                           chan ApplyMsg
	rand                                              *rand.Rand
	role                                              Role
	applyCond                                         *deadlock.Cond
	startIndex                                        int
	newEntry                                          bool
	reqSerialMap                                      map[int]int64
	LastIncludedIndex, LastIncludedTerm               int
	retry                                             bool
	snapshotReqSerialMap                              map[int]int
	snapshot                                          []byte
	pendingSnapshot                                   bool
	skipSnapshot                                      bool
	snapShotCache                                     []Log
	pendingSnapshotLastTerm, pendingSnapshotLastIndex int
	ewmavgMap                                         map[int]EwmaEntry
	CommitQuorum                                      []int
	pendingCommitQuorum                               []int
	pendingQuorumChange                               bool
	pendingQuorumChangeIndex                          int
	unansweredRPCCount                                map[int]int
	quorumChangeChan                                  chan bool // Channel to initiate commit quorum change when one of the followers hasn't responded for 20 continuos RPCs, only use non blocking send.
	zone                                              int
	batching                                          int
}

type Log struct {
	Term          int
	Index         int
	Command       interface{}
	Type          int
	Configuration NoOpEntry
}

// Message Definitions.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

type EwmaEntry struct {
	id  int
	avg *ewma.EWMA
}

type NoOpEntry struct {
	LeaderId     int
	CommitQuorum []int
}

func (ewmaEntry EwmaEntry) addValue(value float64) {
	ewmaEntry.avg.AddValue(value)
}

func (ewmaEntry EwmaEntry) getAvg() float64 {
	return ewmaEntry.avg.GetEWMA()
}

func (ewmaEntry EwmaEntry) String() string {
	return fmt.Sprintf("Id: %d, EWMA: %f", ewmaEntry.id, ewmaEntry.getAvg())
}

type EwmaHeap []EwmaEntry

func (h EwmaHeap) Len() int           { return len(h) }
func (h EwmaHeap) Less(i, j int) bool { return h[i].getAvg() < h[j].getAvg() }
func (h EwmaHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *EwmaHeap) Push(x EwmaEntry) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x)
}

func (h *EwmaHeap) Pop() EwmaEntry {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) encodedRaftState() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.LastIncludedIndex)
	encoder.Encode(rf.LastIncludedTerm)
	encoder.Encode(rf.LastKnownLeader)
	encoder.Encode(rf.CommitQuorum)
	encoder.Encode(rf.Logs)
	return buffer.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.LastIncludedIndex)
	encoder.Encode(rf.LastIncludedTerm)
	encoder.Encode(rf.LastKnownLeader)
	encoder.Encode(rf.CommitQuorum)
	encoder.Encode(rf.Logs)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
	// lablog.Debug(rf.me, lablog.Persist, "Written to Storage VotedFor: %d, CurrentTerm: %d, Logs: %v", rf.VotedFor, rf.CurrentTerm, rf.Logs)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var CurrentTerm, VotedFor, LastIncludedIndex, LastIncludedTerm, LastKnownLeader int
	var CommitQuourm []int
	var Logs []Log
	if decoder.Decode(&VotedFor) != nil ||
		decoder.Decode(&CurrentTerm) != nil ||
		decoder.Decode(&LastIncludedIndex) != nil ||
		decoder.Decode(&LastIncludedTerm) != nil ||
		decoder.Decode(&LastKnownLeader) != nil ||
		decoder.Decode(&CommitQuourm) != nil ||
		decoder.Decode(&Logs) != nil {
		panic("Decode Error")
	}
	rf.VotedFor = VotedFor
	rf.CurrentTerm = CurrentTerm
	// TODO: Add read from snapshot state here.
	rf.LastIncludedIndex = LastIncludedIndex
	rf.LastIncludedTerm = LastIncludedTerm
	rf.LastKnownLeader = LastKnownLeader
	rf.CommitQuorum = CommitQuourm
	rf.Logs = Logs
	// lablog.Debug(rf.me, lablog.Persist, "Read Storage VotedFor: %d, CurrentTerm: %d, Logs: %v", rf.VotedFor, rf.CurrentTerm, rf.Logs)

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {


	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	// //fmt.Println(rf.me, "Taking snapshot for the index: ", index, "and temp log: ", rf.snapShotCache)
	defer rf.mu.Unlock()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	lablog.Debug(rf.me, lablog.Snap, "Received Snapshot for index %d, my last included index %d, and current lastLogIndex %d and lastLogTerm %d and last applied %d", index, rf.LastIncludedIndex, lastLogIndex, lastLogTerm, rf.lastApplied)
	lastIndex, _ := rf.getLastLogIndexAndTerm()
	if index <= rf.LastIncludedIndex || index > lastIndex || index > rf.lastApplied {
		return
	}
	// remove the entries through the index(actual = index - 1), and keep the entries starting from index + 1(actual index = index)
	// copy them into a new slice
	// persist to the memory and return.
	lastIncludedIndex, lastIncludedTerm := rf.LastIncludedIndex, rf.LastIncludedTerm
	if index-1-rf.LastIncludedIndex >= 0 {
		lastIncludedIndex, lastIncludedTerm = rf.Logs[index-1-rf.LastIncludedIndex].Index, rf.Logs[index-1-rf.LastIncludedIndex].Term
	}
	// fill in raft cache
	// temp := make([]Log, 0)
	// rf.Logs = rf.Logs[index-rf.LastIncludedIndex:]
	// for i := index - rf.LastIncludedIndex; i < len(rf.Logs); i++ {
	// 	temp = append(temp, rf.Logs[i])
	// }
	// rf.Logs = make([]Log, len(temp))
	// copy(rf.Logs, temp)
	// rf.Logs = make([]Log, len(temp))
	// copy(rf.Logs, temp)
	cache := make([]Log, 0, SNAPSHOT_CACHE_SIZE)
	if index-rf.LastIncludedIndex < 20 {
		for i := 0; i < index-rf.LastIncludedIndex && i < len(rf.Logs); i++ {
			cache = append(cache, rf.Logs[i])
		}
	} else {
		cache = append(cache, rf.Logs[index-rf.LastIncludedIndex-20:index-rf.LastIncludedIndex]...)
	}
	rf.snapShotCache = cache
	rf.Logs = rf.Logs[index-rf.LastIncludedIndex:]
	rf.LastIncludedIndex, rf.LastIncludedTerm = lastIncludedIndex, lastIncludedTerm
	raftState := rf.encodedRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	// lastLogIndex, lastLogTerm = rf.getLastLogIndexAndTerm()
	lablog.Debug(rf.me, lablog.Snap, "Install snapshot successful for last included index %d", lastLogIndex)
	// lablog.Debug(rf.me, lablog.Snap, "Snapshot successful for index %d, my last included index %d and  my log is %v and current lastLogIndex %d and lastLogTerm and value at index 12 %v", index, rf.LastIncludedIndex, rf.Logs, lastLogIndex, lastLogTerm)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	// defer
	// defer rf.mu.Unlock()
	index := -1
	term := rf.CurrentTerm
	isLeader := rf.role == Leader
	if isLeader {
		index, _ = rf.getLastLogIndexAndTerm()
		index += 1
		entry := Log{
			Term:    rf.CurrentTerm,
			Index:   index,
			Command: command,
			Type:    CLIENT_OPERATION,
		}

		rf.Logs = append(rf.Logs, entry)
		lablog.Debug(rf.me, lablog.HeartBeatLeader, "**********Starting agreement for index %d and term %d****************", index, term)
		rf.newEntry = true
		term := rf.CurrentTerm
		if rf.batching == 0 {
			rf.persist()
			rf.sendAppendEntries(term, ALL_NODES)
		}
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Cond.Broadcast()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []Node, me int,
	persister *Persister, applyCh chan ApplyMsg, rpcServer *Server, batching int) *Raft {

	// TODO: Read from persistent storage.
	rf := &Raft{}
	rf.peers = peers
	rf.rpcServer = rpcServer
	rf.persister = persister
	rf.me = me
	rf.startIndex = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.LeaderId = -1
	rf.LastKnownLeader = -1
	rf.role = Follower
	rf.retry = true
	rf.Logs = make([]Log, 0)
	rf.applyCh = applyCh
	rf.persister = persister
	rf.nextIndex = make(map[int]int, 0)
	rf.matchIndex = make(map[int]int, 0)
	rf.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.lastHeartBeatTime = time.Now()
	rf.batching = batching
	rf.applyCond = new(deadlock.Cond)
	rf.applyCond.Cond = *sync.NewCond(&rf.mu)
	// sync.NewCond(&rf.mu)
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	// TODO: Add snapshot read.
	rf.CommitQuorum = make([]int, 0)
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex
	// rf.ewmavg = ewma.NewEWMA(0)
	rf.pendingCommitQuorum = make([]int, 0)
	rf.pendingQuorumChange = false
	rf.pendingQuorumChangeIndex = NULL_VALUE
	rf.quorumChangeChan = make(chan bool) // only use non blocking send.
	rf.zone = rf.me                       // TODO: Read from config.

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.heartBeatTicker()
	go rf.committer()

	//fmt.Println("Me", rf.me, " ,Commit Quourm: ", rf.CommitQuorum, " Last Leader: ", rf.LastKnownLeader)
	return rf
}

func (rf *Raft) batcher() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.newEntry && rf.role == Leader {
			rf.newEntry = false
			term := rf.CurrentTerm
			rf.persist()
			rf.sendAppendEntries(term, COMMIT_QUORUM_ONLY)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(BATCHER_DURATION * time.Millisecond)
	}
}

/*
**************************************************************************************************************************
       	    	 	 	 	 	 	 	 	Election Region
**************************************************************************************************************************
*/

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	ok := rf.rpcServer.Call(server, "Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutDuration = time.Duration((ELECTION_TIMEOUT_BASE + time.Duration(rf.rand.Intn(RANDOM_RANGE))) * time.Millisecond)
	rf.electionTime = time.Now().Add(rf.electionTimeoutDuration)
	rf.lastContact = time.Now()
	lablog.Debug(rf.me, lablog.Timer, "Reset the election timer to %v", rf.electionTime)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// time.Sleep(5 * time.Second)
	for !rf.killed() {
		time.Sleep(10 * time.Second)
		var sleepTime time.Duration
		rf.mu.Lock()
		if rf.role == Leader {
			rf.resetElectionTimeout()
			sleepTime = rf.electionTimeoutDuration
		} else {
			if time.Now().After(rf.electionTime) {
				// run election
				////////fmt.Println("starting election ", rf.me)
				lablog.Debug(rf.me, lablog.VoteCandidate, "Changing to candidate and starting election")
				//fmt.Println(rf.me, " starting election")
				rf.startElection()
			} else {
				// sleep for some more time.
				sleepTime = time.Until(rf.electionTime)
			}
		}
		rf.mu.Unlock()
		time.Sleep(sleepTime)

	}
}

func (rf *Raft) startElection() {
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.role = Candidate
	rf.VotedFor = rf.me
	rf.persist()
	// Request vote request
	votesCount := 1
	votesReceived := make(map[int]struct{})
	votesReceived[rf.me] = struct{}{}
	////////fmt.Println("Initial vote count for me: ", rf.me, " is: ", voteCount)
	lastIndex, lastTerm := rf.getLastLogIndexAndTerm()
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	defer rf.resetElectionTimeout()
	// Debug()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(peer int, requestVoteArgs *RequestVoteArgs /*, votesCount *int*/) {
			rf.mu.Lock()
			if rf.role != Candidate || rf.CurrentTerm != requestVoteArgs.Term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			requestVoteReply := new(RequestVoteReply)
			fmt.Println(rf.me, " sending request vote to the follower ", peer)
			ok := rf.sendRequestVote(peer, requestVoteArgs, requestVoteReply)
			if !ok {
				return
			}
			rf.mu.Lock()
			// defer rf.mu.Unlock()
			if rf.CurrentTerm != requestVoteArgs.Term {
				lablog.Debug(rf.me, lablog.StaleRpcResponse, "Got a stale response from server S-%d, with request term %d and my current term %d", peer, requestVoteArgs.Term, rf.CurrentTerm)
				rf.mu.Unlock()
				return
			}
			if rf.CurrentTerm < requestVoteReply.Term {
				////////fmt.Println("Me ", rf.me, " becoming follower start election")
				rf.becomeFollower(requestVoteReply.Term)
				rf.VotedFor = -1
				rf.resetElectionTimeout()
				rf.persist()
				rf.mu.Unlock()
				return
			}
			// Stale RPC reply
			if rf.role != Candidate {
				rf.mu.Unlock()
				return
			}
			if requestVoteReply.VoteGranted {
				votesCount += 1
				votesReceived[peer] = struct{}{}
				// TODO: Make dynamic for flexible commit.
				if ((rf.LastKnownLeader == NULL_VALUE || COMMIT_QUORUM_SIZE+1 >= len(rf.peers)) && votesCount >= (len(rf.peers)/2)+1) ||
					(COMMIT_QUORUM_SIZE+1 < len(rf.peers) && (rf.commitQuorumVoteSatisfied(votesCount, votesReceived) || rf.pessimisticVoteQuorumSatisfied(votesCount))) {
					////////fmt.Println("Me changing to leader ", rf.me, "with votes", votesCount, "total nodes", len(rf.peers), "with term", rf.CurrentTerm)
					// convert to leader
					rf.becomeLeader()
					term := rf.CurrentTerm
					lablog.Debug(rf.me, lablog.Leader, "Won the election for the term %d and vote count %d", rf.CurrentTerm, votesCount)
					rf.sendAppendEntries(term, ALL_NODES)
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()

		}(index, requestVoteArgs /*, &voteCount*/)
	}

}

func (rf *Raft) commitQuorumVoteSatisfied(votesCount int, votesReceived map[int]struct{}) bool {
	var isCommitQuorumSatisfied = false
	for _, v := range rf.CommitQuorum {
		if _, ok := votesReceived[v]; ok {
			isCommitQuorumSatisfied = true
			break
		}
	}
	if _, ok := votesReceived[rf.LastKnownLeader]; ok {
		isCommitQuorumSatisfied = true
	}
	if isCommitQuorumSatisfied && votesCount > (len(rf.peers)/2)+1 {
		//fmt.Println(rf.me, " Candidate vote map: ", votesReceived)
		return true
	}
	return false
}

func (rf *Raft) pessimisticVoteQuorumSatisfied(votesCount int) bool {
	// if size of peers is 3 -> then no commit quorum is possible
	return votesCount > len(rf.peers)-COMMIT_QUORUM_SIZE
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	////////fmt.Println("Got vote request from", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if rf.CurrentTerm < args.Term {
		////////fmt.Println("Me ", rf.me, " becoming follower Request vote")
		rf.becomeFollower(args.Term)
		rf.VotedFor = -1
	}
	reply.Term = rf.CurrentTerm
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// check if log is at least upto date
		if rf.isRPCLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.VotedFor = args.CandidateId
			// reset election timer after granting vote. Figure 2 Rules for follower.
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			rf.resetElectionTimeout()

			lablog.Debug(rf.me, lablog.Vote, "Granting vote to candidate %d with term %d", args.CandidateId, args.Term)
			return
		}
	}
	lablog.Debug(rf.me, lablog.Vote, "Rejecting candidate %d with term %d, as current term %d", args.CandidateId, args.Term, rf.CurrentTerm)
}

/*
**************************************************************************************************************************
       	    	 	 	 	 	 	 	 	Leader Region
**************************************************************************************************************************
*/

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.LeaderId = rf.me
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.reqSerialMap = make(map[int]int64)
	rf.snapshotReqSerialMap = make(map[int]int)
	rf.unansweredRPCCount = make(map[int]int)
	rf.snapShotCache = make([]Log, 0)
	rf.ewmavgMap = map[int]EwmaEntry{}
	rf.LastKnownLeader = rf.me
	lastIndex, _ := rf.getLastLogIndexAndTerm()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.nextIndex[index] = lastIndex + 1
		rf.matchIndex[index] = 0
		rf.reqSerialMap[index] = 0
		rf.snapshotReqSerialMap[index] = 0
		rf.unansweredRPCCount[index] = 0
		rf.ewmavgMap[index] = EwmaEntry{
			id:  index,
			avg: ewma.NewEWMA(0),
		}
	}
	go rf.heartBeatTicker()
	if rf.batching == 1 {
		go rf.batcher() //TODO:  Use while doing performance testing.
	}
	// TODO: Select the closest nodes as the commit quorum and do majority commit.
	if COMMIT_QUORUM_SIZE+1 < len(rf.peers) {
		rf.pendingCommitQuorum = rf.getNearestNodes()
		rf.pendingQuorumChange = true
		rf.pendingQuorumChangeIndex = lastIndex + 1
		entry := Log{
			Term:  rf.CurrentTerm,
			Index: lastIndex + 1,
			Type:  NO_OP,
			Configuration: NoOpEntry{
				LeaderId:     rf.me,
				CommitQuorum: rf.pendingCommitQuorum,
			},
		}
		rf.Logs = append(rf.Logs, entry)
		rf.persist()
		go rf.quorumAdapter()
	}

}

func (rf *Raft) getNearestNodes() []int {
	// nodes id are synonymous to zone. Zones are arranged in the form of a zone.
	commitQuorum := make([]int, 0)
	for i := 0; i < COMMIT_QUORUM_SIZE; i++ {
		//fmt.Println(rf.me, " Nearby Node: ", rf.peers[rf.me].NearByNodes[i])
		commitQuorum = append(commitQuorum, rf.peers[rf.me].NearByNodes[i])
	}
	//fmt.Println(rf.me, "Nearest Node quorum: ", commitQuorum)
	// commit quorum will be in sorted state. except for 2 node quorum i.e COMMIT_QUORUM_SIZE = 1
	// sort.Slice(commitQuorum, func(i, j int) bool {
	// 	return commitQuorum[i] < commitQuorum[j]
	// })
	return commitQuorum
}

// Sending append entries to followers in parallel

func (rf *Raft) sendAppendEntries(term, quorumType int) {
	// //fmt.Println("Leader: ", rf.me, " Quorum Type: ", quorumType, " Commit Quorum: ", rf.CommitQuorum)
	if quorumType == COMMIT_QUORUM_ONLY && len(rf.CommitQuorum) > 0 && COMMIT_QUORUM_SIZE+1 < len(rf.peers) {
		for _, value := range rf.CommitQuorum {
			if value == rf.me {
				continue
			}
			go rf.appendEntrySender(term, value)
			// fmt.Print("CommitQuorum Type, send rpc to: ", value)
		}
	} else {
		for index := range rf.peers {
			if index != rf.me {
				go rf.appendEntrySender(term, index)
			}
		}
	}

}

// check if the request can be retried
func (rf *Raft) shouldRetry(serialNumber int64, term, peer int) bool {
	if !rf.retry || rf.role != Leader || rf.CurrentTerm != term || serialNumber < rf.reqSerialMap[peer] || rf.killed() {
		return false
	}
	return true
}

// Actual AppendEntries sender for a follower.
func (rf *Raft) appendEntrySender(term, peer int) {
	okChan := make(chan bool, 1)
send:
	rf.mu.Lock()
	lablog.Debug(rf.me, lablog.Heart, "commit index %d, next indexes: %v", rf.commitIndex, rf.nextIndex)
	if term != rf.CurrentTerm || rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	if (len(rf.snapShotCache) > 0 && (rf.nextIndex[peer] < rf.snapShotCache[0].Index)) || (rf.nextIndex[peer] <= rf.LastIncludedIndex && len(rf.snapShotCache) == 0) {
		// TODO: Send snapshot
		i := rf.reqSerialMap[peer] + 1
		rf.reqSerialMap[peer] = i
		go rf.installSnapshotSender(term, peer)
		rf.mu.Unlock()
		return
	}
	args := rf.prepareAppendEntriesRequest(peer)
	if args == nil {
		rf.mu.Unlock()
		return
	}
	i := rf.reqSerialMap[peer] + 1
	rf.reqSerialMap[peer] = i
	divisor := 1
	if l := len(args.Entries); l > 0 {
		divisor = l
	}
	rf.mu.Unlock()
	reply := new(AppendEntriesReply)

	// retry:
	var startTime int64 = time.Now().UnixNano()
	// rpc function for timeout based rpcs.
	go func(args *AppendEntriesArgs, reply *AppendEntriesReply, okChan chan bool) {
		ok := rf.sendAppendEntry(peer, args, reply)
		select {
		case okChan <- ok: // non blocking send
		default:
		}
	}(args, reply, okChan)

	select {
	case <-time.After(RPC_TIMEOUT_DURATION):
		// add
		//fmt.Println(rf.me, " Leader failed append entries for the follower after timeout ", peer)
		rf.mu.Lock()
		if rf.role == Leader && rf.CurrentTerm == term {
			// TODO: Check with prof if this is fine or should divide with the entries size
			rf.ewmavgMap[peer].addValue(float64((time.Now().UnixNano() - startTime) / int64(divisor)))
			rf.unansweredRPCCount[peer] = rf.unansweredRPCCount[peer] + 1
			rf.sendWakeOnQuorumChangeChannel(peer)
		}
		if !rf.shouldRetry(i, term, peer) {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		// retry
		////fmt.Println("Leader:  ", rf.me, " Timeout for the follower: ", peer)
		// go rf.appendEntrySender(term, peer)
		goto send
	case ok := <-okChan:
		end := time.Now().UnixNano()
		// if !timer.Stop() {
		// ////fmt.Println("Leader:  ", rf.me," Timer not stopped: ", peer)
		// 	<-timer.C
		// }
		rf.mu.Lock()

		if rf.role != Leader || rf.CurrentTerm != args.Term || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.ewmavgMap[peer].addValue(float64((time.Now().UnixNano() - startTime) / int64(divisor)))
		if !ok {
			rf.unansweredRPCCount[peer] = rf.unansweredRPCCount[peer] + 1
			rf.sendWakeOnQuorumChangeChannel(peer)
			// retry
			// //fmt.Println(rf.me, " Leader failed append entries for the follower after timeout ", peer, ", start ", startTime, ", end: ", end)

			if !rf.shouldRetry(i, term, peer) {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			goto send
			// return
		}
		// defer rf.mu.Unlock()
		//  || rf.reqSerialMap[peer] != i
		if reply.Term > rf.CurrentTerm {
			rf.becomeFollower(reply.Term)
			rf.VotedFor = -1
			rf.persist()
			rf.resetElectionTimeout()
			rf.mu.Unlock()
			return
		}
		rf.ewmavgMap[peer].addValue(float64(end - startTime))
		rf.unansweredRPCCount[peer] = 0
		if !reply.Success {
			if reply.XLen != NULL_VALUE {
				// follower's log is too short
				rf.nextIndex[peer] = reply.XLen
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
			// resend the rpc or install snapshot request with newly received nextIndex value.
			rf.mu.Unlock()
			goto send

		} else {
			if len(args.Entries) != 0 {
				rf.nextIndex[peer] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[peer])
				rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
			}
			// TODO: Test for async commit
			rf.checkCommit()
			rf.mu.Unlock()
		}

	}
}

// Prepare AppendEntries request for a follower. Only used when nextIndex[peer] is greater or equal to LastIncludedIndex
func (rf *Raft) prepareAppendEntriesRequest(peer int) *AppendEntriesArgs {
	a, _ := rf.getLastLogIndexAndTerm()
	lablog.Debug(rf.me, lablog.Heart, "Preparing append entries request for follower %d with next index %d, and my last log index %d", peer, rf.nextIndex[peer], a)
	if rf.nextIndex[peer] > rf.LastIncludedIndex {
		prevIndex := rf.nextIndex[peer] - 1
		prevTerm := rf.LastIncludedTerm
		if prevIndex-1-rf.LastIncludedIndex >= 0 {
			prevTerm = rf.Logs[prevIndex-1-rf.LastIncludedIndex].Term
		}
		temp := rf.Logs
		if prevIndex > rf.LastIncludedIndex {
			temp = rf.Logs[prevIndex-rf.LastIncludedIndex:]
		}
		entries := make([]Log, len(temp))
		copy(entries, temp)
		temp = nil
		args := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		// lablog.Debug(rf.me, lablog.Heart, "Append entries request: %v for the follower %d with next index %d", *args, peer, rf.nextIndex[peer])
		return args
	}
	if len(rf.snapShotCache) > 0 && rf.nextIndex[peer] > rf.snapShotCache[0].Index && rf.nextIndex[peer] <= rf.snapShotCache[len(rf.snapShotCache)-1].Index {
		lablog.Debug(rf.me, lablog.Heart, "Preparing append entries request for the follower %d with next index %d using cache, cache first index %d, cache last index %d and last included index %d", peer, rf.nextIndex[peer], rf.snapShotCache[0].Index, rf.snapShotCache[len(rf.snapShotCache)-1].Index, rf.LastIncludedIndex)
		prevIndex := rf.nextIndex[peer] - 1
		i := 0
		for ; i < len(rf.snapShotCache); i++ {
			if rf.snapShotCache[i].Index == prevIndex {
				break
			}
		}
		prevTerm := rf.snapShotCache[i].Term
		var temp []Log
		if i+1 >= len(rf.snapShotCache) {
			temp = rf.Logs
		} else {
			temp = append(temp, rf.snapShotCache[i+1:]...)
			temp = append(temp, rf.Logs...)
		}
		entries := make([]Log, len(temp))
		copy(entries, temp)
		temp = nil
		args := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		// lablog.Debug(rf.me, lablog.Heart, "Append entries request using cache: %v for the follower %d with next index %d", *args, peer, rf.nextIndex[peer])
		return args
	}
	go rf.installSnapshotSender(rf.CurrentTerm, peer)
	return nil

}

func (rf *Raft) findLastEntryForXTerm(term, index int) int {
	// TODO: Maybe use binary search here, and while log compaction use lastIncluded

	return NULL_VALUE
}

// TODO: Test
// TODO: DO the check in a new go routine?
func (rf *Raft) checkCommit() {
	// if pending commit quorum -> use majority commit
	// else use commit quorum.
	// //fmt.Println(rf.me, " Leader Checking the committer, commit Index: ", rf.commitIndex, " last applied: ", rf.lastApplied, "last index and term", len(rf.Logs))
	for i := len(rf.Logs) - 1; i >= 0; i-- {
		commitQuorumSatisfied := false
		index := rf.Logs[i].Index
		term := rf.Logs[i].Term
		count := 1
		if !rf.pendingQuorumChange && len(rf.CommitQuorum) > 0 {
			// check match index of the commit quorum if it
			// //fmt.Println("Checking commit quorum for commit")
			quorumCount := 0
			for _, peer := range rf.CommitQuorum {
				if rf.matchIndex[peer] == index {
					quorumCount += 1
				} else {
					break
				}
			}
			if quorumCount == COMMIT_QUORUM_SIZE {
				commitQuorumSatisfied = true
			}
			// //fmt.Println("Quorum Count ", quorumCount)
		}
		if !commitQuorumSatisfied {
			// //fmt.Println("Checking majority quorum for commit")
			for peer := range rf.matchIndex {
				if rf.matchIndex[peer] == index {
					count += 1
				}
			}
		}

		// TODO: Might have to change i to index while working with compaction.
		lablog.Debug(rf.me, lablog.RaftCondNotify, "Leader commit index %d, last applied %d, index %d, term %d, currentTerm %d", rf.commitIndex, rf.lastApplied, index, term, rf.CurrentTerm)
		if (commitQuorumSatisfied || count >= (len(rf.peers)/2)+1) && term == rf.CurrentTerm && index > rf.commitIndex {
			// Resetting the values of commit quorum.
			if rf.pendingQuorumChange && rf.pendingQuorumChangeIndex != NULL_VALUE && index >= rf.pendingQuorumChangeIndex {
				//fmt.Println("Pending Commit quorum: ", rf.pendingCommitQuorum)
				rf.CommitQuorum = rf.pendingCommitQuorum
				rf.pendingQuorumChange = false
				rf.pendingQuorumChangeIndex = NULL_VALUE
				rf.pendingCommitQuorum = make([]int, 0)
				rf.persist()
				//fmt.Println("Commit quorum changed successfully!")
			}
			// //fmt.Println("Commit using commit quourm: ", commitQuorumSatisfied, " Pending change: ", rf.pendingQuorumChange, "Commit quorum len: ", len(rf.CommitQuorum))
			lablog.Debug(rf.me, lablog.Commit, "Updating the commit index to index %d", index)
			rf.commitIndex = index
			lablog.Debug(rf.me, lablog.RaftCondNotify, "Broadcast to notify committer in leader committer for commit index %d and last applied %d", rf.commitIndex, rf.lastApplied)
			rf.applyCond.Cond.Broadcast()
			break
		}
		if index <= rf.commitIndex {
			break
		}
	}
}

// Actual RPC call for AppendEntries.
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	ok := rf.rpcServer.Call(server, "Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) heartBeatTicker() {
	for !rf.killed() {
		time.Sleep(LEADER_HEARTBEAT_TIME * time.Millisecond)
		rf.mu.Lock()
		// TODO: may be use a timer later on for easy of use and reset.
		if rf.role == Leader {
			////////fmt.Println("Me sending append entries", rf.me)
			lablog.Debug(rf.me, lablog.HeartBeatLeader, "*********** Sending heartbeats to the followers HeartBeatTicker *******************")
			term := rf.CurrentTerm
			rf.sendAppendEntries(term, ALL_NODES)
			// //fmt.Println(rf.me, " Sending heartbeat")
			// //fmt.Println("Math indexes: ", rf.matchIndex)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

/*
**************************************************************************************************************************
       	    	 	 	 	 	 	 	 	Utils Region
**************************************************************************************************************************
*/

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	length := len(rf.Logs)
	if length > 0 {
		entry := rf.Logs[length-1]
		return entry.Index, entry.Term
	}
	return rf.LastIncludedIndex, rf.LastIncludedTerm

}

func (rf *Raft) isRPCLogUpToDate(rpcIndex, rpcTerm int) bool {
	lastIndex, lastTerm := rf.getLastLogIndexAndTerm()
	// if lastIndex <= rpcIndex && lastTerm <= rpcTerm {
	// 	return true
	// }
	if lastTerm < rpcTerm {
		return true
	}
	if lastTerm == rpcTerm && lastIndex <= rpcIndex {
		return true
	}
	return false
}

func (rf *Raft) findXIndex(prevLogIndex, term int) int {
	// TODO: check while working on log compaction.
	// TODO: Update to binary search here
	// for i := prevLogIndex - 1; i >= 0; i-- {
	// 	if rf.Logs[i].Term != term {
	// 		return rf.Logs[i+1].Index
	// 	}
	// }
	// return prevLogIndex
	for i := prevLogIndex - 1; i >= 0; i-- {
		if rf.Logs[i].Term != term {
			return rf.Logs[i+1].Index
		}
	}
	return rf.LastIncludedIndex + 1
}

/*
**************************************************************************************************************************
       	    	 	 	 	 	 	 	 	Follower Region
**************************************************************************************************************************
*/

// Follower's Handler for append entries.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// lablog.Debug(rf.me, lablog.Heart, "Received AppendEntries Req %v,and commit index %d", args, rf.commitIndex)
	defer time.Sleep(time.Duration(rf.me*rf.rand.Intn(10)) * time.Millisecond)

	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm || rf.killed() {
		return
	}
	defer rf.resetElectionTimeout()
	defer rf.persist()
	if args.Term > rf.CurrentTerm || (rf.CurrentTerm == args.Term && rf.role != Follower) {
		rf.becomeFollower(args.Term)
		// doPersist = true
	}
	rf.LeaderId = args.LeaderId
	rf.LastKnownLeader = args.LeaderId
	reply.Term = rf.CurrentTerm
	// TODO: Test.

	// find the entry at index args.PrevLogIndex at followers log.
	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	// leader's log is way ahead of follower's log. Follower doesn't have PrevLogIndex
	if args.PrevLogIndex > lastLogIndex {
		reply.XLen = lastLogIndex + 1
		// lablog.Debug(rf.me, lablog.Heart, "args.PrevLogIndex > lastLogIndex Rejecting AppendEntries Req %v, lastLogIndex %d, LastIncludedINDEX %d and resp %v", args, lastLogIndex, rf.LastIncludedIndex, *reply)
		return
	}
	if rf.LastIncludedIndex > args.PrevLogIndex {
		reply.XIndex = rf.LastIncludedIndex + 1
		reply.XLen = NULL_VALUE
		reply.XTerm = NULL_VALUE
		// lablog.Debug(rf.me, lablog.Heart, "rf.LastIncludedIndex > args.PrevLogIndex Rejecting AppendEntries Req %v, LastIncludedTerm %d > PrevLogIndex %d and resp %v", args, rf.LastIncludedIndex, args.PrevLogIndex, *reply)
		return
	}
	matchTerm := rf.LastIncludedTerm
	matchIndex := 0
	if args.PrevLogIndex > rf.LastIncludedIndex {
		matchIndex = args.PrevLogIndex - 1 - rf.LastIncludedIndex // -1 as Raft Log index is actual array index + 1
		matchTerm = rf.Logs[matchIndex].Term
		// lablog.Debug(rf.me, lablog.Heart, "args.PrevLogIndex > rf.LastIncludedIndex Found matchTerm %d, matchIndex %d, arg Index %d, args Term %d and log %v", matchTerm, rf.Logs[matchIndex],args.PrevLogIndex, args.PrevLogTerm, rf.Logs)

	}
	if matchTerm != args.PrevLogTerm {
		// find the first index of the conflicting term.
		XIndex := rf.findXIndex(matchIndex, matchTerm)
		if XIndex == 0 {
			XIndex = 1
		}
		reply.XIndex = XIndex
		lablog.Debug(rf.me, lablog.Heart, "matchTerm != args.PrevLogTerm matchTerm %d, matchIndex %d, args.PrevIndex %d, args.PrevTerm %d", matchTerm, matchIndex, args.PrevLogIndex, args.PrevLogTerm)
		// lablog.Debug(rf.me, lablog.Heart, "matchTerm != args.PrevLogTerm Rejecting AppendEntries Req %v, XINDEX %d and resp %v", args, XIndex, *reply)
		reply.XTerm = matchTerm
		reply.XLen = NULL_VALUE
		return
	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
	// iterate over the logs, find conflicting entries and remove all the entries after conflict index
	i, j := args.PrevLogIndex-rf.LastIncludedIndex, 0
	var conflict bool = false
	entriesLen := len(args.Entries)
	for ; i < len(rf.Logs) && j < entriesLen; i, j = i+1, j+1 {
		// lablog.Debug(rf.me, lablog.Heart, "Comparing %d Log: %v, %d Req Entry: %v", i, rf.Logs[i], j, args.Entries[j])
		if rf.Logs[i].Term != args.Entries[j].Term {
			conflict = true
			// doPersist = true
			break
		}
	}
	if conflict {
		rf.Logs = rf.Logs[:i]
	}
	rf.Logs = append(rf.Logs, args.Entries[j:]...)
	for ; j < len(args.Entries); j++ {
		entry := args.Entries[j]
		////fmt.Println("entry type : ", entry.Type)
		if entry.Type == NO_OP {
			////fmt.Println("NO_OP entry: ", entry)
			no_op := entry.Configuration
			rf.CommitQuorum = no_op.CommitQuorum
		}
		// rf.Logs = append(rf.Logs, entry)
	}
	// if j != entriesLen {
	// 	// doPersist = true
	// }

	// if args.LeaderCommit != rf.commitIndex {
	lastIndex, _ := rf.getLastLogIndexAndTerm()
	rf.commitIndex = max(rf.commitIndex, min(lastIndex, args.LeaderCommit))
	// }
	// lablog.Debug(rf.me, lablog.Heart, "Appended entry success, args %v", args)
	if rf.commitIndex > rf.lastApplied {
		lablog.Debug(rf.me, lablog.Heart, "Follower Broadcast to notify committer in append entries, commit index %d and last applied %d", rf.commitIndex, rf.lastApplied)
		rf.applyCond.Cond.Broadcast()
	}

	lablog.Debug(rf.me, lablog.Heart, "Processed AppendEntries and commit index %d and log %v", rf.commitIndex, rf.Logs)

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) becomeFollower(term int) {
	////////fmt.Println("Me", rf.me, " becoming follower, current term", rf.CurrentTerm, " incoming term ", term)
	// rf.VotedFor = -1
	rf.CurrentTerm = term
	rf.role = Follower
	rf.matchIndex = nil
	rf.nextIndex = nil
	rf.reqSerialMap = nil
	rf.unansweredRPCCount = nil
	rf.ewmavgMap = nil
	rf.snapShotCache = make([]Log, 0)
}

/*
**************************************************************************************************************************
       	    	 	 	 	 	 	 	 	Common Region
**************************************************************************************************************************
*/

func (rf *Raft) committer() {
	defer close(rf.applyCh)
	for !rf.killed() {
		lablog.Debug(rf.me, lablog.RaftLock, "Calling the lock before main loop committer")
		rf.mu.Lock()
		lablog.Debug(rf.me, lablog.RaftLock, "Got the lock before main loop committer")

		for !rf.killed() && rf.commitIndex == rf.lastApplied && !rf.pendingSnapshot {
			// TODO: add cond wait here.
			// DPrintf("time: %v, me: %d going to sleep in committer")
			lablog.Debug(rf.me, lablog.Heart, "Going to sleep on wait in committer")
			rf.applyCond.Cond.Wait()
			lablog.Debug(rf.me, lablog.Heart, "Waking up in committer")
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot {
			apply := ApplyMsg{}
			snap := make([]byte, len(rf.snapshot))
			copy(snap, rf.snapshot)
			// if rf.lastApplied < rf.pendingSnapshotLastIndex {
			apply = ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap,
				SnapshotTerm:  rf.pendingSnapshotLastTerm,
				SnapshotIndex: rf.pendingSnapshotLastIndex,
			}

			// }
			rf.commitIndex = max(rf.commitIndex, rf.pendingSnapshotLastIndex)
			rf.lastApplied = max(rf.lastApplied, rf.pendingSnapshotLastIndex)
			// rf.LastIncludedIndex = rf.pendingSnapshotLastIndex
			// rf.LastIncludedTerm = rf.pendingSnapshotLastTerm
			lablog.Debug(rf.me, lablog.Snap, "Applied snapshot, lastIncludedIndex %d, lastIncludedTerm %d, lastApplied %d and commitIndex %d", rf.pendingSnapshotLastIndex, rf.pendingSnapshotLastTerm, rf.lastApplied, rf.commitIndex)
			rf.snapshot = nil
			rf.pendingSnapshot = false
			rf.pendingSnapshotLastIndex = 0
			rf.pendingSnapshotLastTerm = 0
			rf.skipSnapshot = false
			// rf.commitIndex = max(rf.commitIndex, rf.)
			rf.mu.Unlock()
			if apply.SnapshotValid {
				rf.applyCh <- apply
			}

			rf.mu.Lock()
			lablog.Debug(rf.me, lablog.Snap, "Applied snapshot successfully, lastIncludedIndex %d, lastIncludedTerm %d, valid %v, lastApplied %d and commitIndex %d", apply.SnapshotIndex, apply.SnapshotTerm, apply.SnapshotValid, rf.lastApplied, rf.commitIndex)

		}

		for rf.lastApplied < rf.commitIndex && !rf.pendingSnapshot {
			rf.lastApplied++
			lablog.Debug(rf.me, lablog.Heart, "Applying %d, commit index %d", rf.lastApplied, rf.commitIndex)
			apply := ApplyMsg{
				CommandValid: true, //rf.Logs[rf.lastApplied-rf.LastIncludedIndex-1].Type == CLIENT_OPERATION, // false for the NoOp command as it should be applied to the application state.
				Command:      rf.Logs[rf.lastApplied-rf.LastIncludedIndex-1].Command,
				CommandIndex: rf.Logs[rf.lastApplied-rf.LastIncludedIndex-1].Index,
				Type:         rf.Logs[rf.lastApplied-rf.LastIncludedIndex-1].Type,
			}
			lablog.Debug(rf.me, lablog.Heart, "Applying %d, last applied %d, commit index %d, and apply message %v", rf.lastApplied, rf.lastApplied, rf.commitIndex, apply)
			rf.mu.Unlock()
			rf.applyCh <- apply
			rf.mu.Lock()
		}

		rf.mu.Unlock()
	}

}

/*
* Snapshot Region
 */

func (rf *Raft) installSnapshotSender(term, peer int) {
	rf.mu.Lock()
	// Only leaders send the install snapshot rpc,
	// The routine can be scheduled later than expected. so check the term.
	if rf.role != Leader || rf.CurrentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	//fmt.Println(rf.me, " Sending snapshot to the follower ", peer)
	nextIndex := rf.nextIndex[peer]
	if nextIndex > rf.LastIncludedIndex {
		// snapshot was already installed
		rf.mu.Unlock()
		return
	}
	args := new(InstallSnapshotArgs)
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
	index := rf.snapshotReqSerialMap[peer] + 1
	rf.snapshotReqSerialMap[peer] = index
	rf.mu.Unlock()
	lablog.Debug(rf.me, lablog.Snap, "Sending install snapshot to the follower %d, with LastIncludedIndex %d and LastIncluded Term %d", peer, args.LastIncludedIndex, args.LastIncludedTerm)
	reply := new(InstallSnapshotReply)
	okChan := make(chan bool)
retry:
	startTime := time.Now().UnixNano()
	go func() {
		ok := rf.sendInstallSnapshot(peer, args, reply)
		select {
		case okChan <- ok:
		default:
		}
	}()
	select {
	case <-time.After(SNAPSHOT_TIMEOUT_DURATION):
		rf.mu.Lock()
		if rf.role == Leader && rf.CurrentTerm == term {
			rf.unansweredRPCCount[peer] = rf.unansweredRPCCount[peer] + 1
			rf.ewmavgMap[peer].addValue(float64(time.Now().UnixNano() - startTime))
			rf.sendWakeOnQuorumChangeChannel(peer)
		}
		if rf.role != Leader || rf.CurrentTerm != term || rf.snapshotReqSerialMap[peer] > index || rf.nextIndex[peer] > rf.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		//fmt.Println(rf.me, " retrying snapshot for the follower ", peer)
		goto retry
	case ok := <-okChan:
		rf.mu.Lock()
		if !ok {
			// update the counter
			// retry
			// check the
			//fmt.Println(rf.me, " retrying snapshot for the follower ", peer)
			if rf.role != Leader || rf.CurrentTerm != term || !rf.retry {
				rf.mu.Unlock()
				return
			}
			rf.unansweredRPCCount[peer] = rf.unansweredRPCCount[peer] + 1
			rf.sendWakeOnQuorumChangeChannel(peer)
			if rf.snapshotReqSerialMap[peer] > index || rf.nextIndex[peer] > rf.LastIncludedIndex {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			goto retry
		}
		if reply.Term > rf.CurrentTerm {
			rf.becomeFollower(reply.Term)
			rf.VotedFor = -1
			rf.persist()
			rf.resetElectionTimeout()
			rf.mu.Unlock()
			return
		}
		if args.Term != rf.CurrentTerm || rf.role != Leader {
			// stale request or leadership has passed.
			rf.mu.Unlock()
			return
		}
		rf.ewmavgMap[peer].addValue(float64(time.Now().UnixNano() - startTime))
		if !reply.Success {
			rf.mu.Unlock()
			return
		}
		// update match and next index
		nextIndex, matchIndex := rf.nextIndex[peer], rf.matchIndex[peer]
		rf.nextIndex[peer] = max(nextIndex, args.LastIncludedIndex+1)
		rf.matchIndex[peer] = max(matchIndex, args.LastIncludedIndex)
		lablog.Debug(rf.me, lablog.Snap, "Install Snapshot successful for the follower %d, next index %d and match index %d", peer, rf.nextIndex[peer], rf.matchIndex[peer])
		rf.mu.Unlock()
		go rf.appendEntrySender(rf.CurrentTerm, peer)

	}
}

// args *AppendEntriesArgs, reply *AppendEntriesReply
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	a, b := rf.getLastLogIndexAndTerm()
	lablog.Debug(rf.me, lablog.Snap, "Got install snapshot request from the leader with lastIncludedIndex %d and LastIncludedIndex %d, my lastIndex %d and lastTerm %d", args.LastIncludedIndex, args.LastIncludedTerm, a, b)
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm || rf.killed() {
		reply.Success = false
		return
	}
	defer rf.resetElectionTimeout()
	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.role != Follower) {
		if rf.role != Follower {
			rf.VotedFor = -1
		}
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.CurrentTerm
	rf.LeaderId = args.LeaderId
	rf.LastKnownLeader = args.LeaderId

	if args.LastIncludedIndex <= rf.LastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		// already snapshotted till index args.LastIncludedIndex.
		reply.Success = true
		return
	}
	//
	// remove the log up to the index in the req
	// persist the snapshot and the state to disk
	// send wake on the conditional variable.
	lastIndex, _ := rf.getLastLogIndexAndTerm()
	if args.LastIncludedIndex >= lastIndex {
		rf.Logs = make([]Log, 0)
		lablog.Debug(rf.me, lablog.Snap, "Discarding whole logs as args.LastIncludedIndex %d >= LastLogIndex %d", args.LastIncludedIndex, lastIndex)
	} else {
		// discard entries through the index args.LastIncluded index and keep the entries proceeding it.
		i := 0
		for ; i < len(rf.Logs); i++ {
			if rf.Logs[i].Index == args.LastIncludedIndex {
				break
			}
		}
		rf.Logs = rf.Logs[i+1:]
		lablog.Debug(rf.me, lablog.Snap, "Discarding the logs from the index %d, args.LastIncludedIndex %d, and  LastLogIndex %d", i+1, args.LastIncludedIndex, lastIndex)
	}
	rf.snapshot = args.Data
	rf.snapShotCache = make([]Log, 0)
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.pendingSnapshot = true
	rf.pendingSnapshotLastIndex = args.LastIncludedIndex
	rf.pendingSnapshotLastTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = args.LastIncludedIndex
	// rf.CommitQuorum = args.CommitQuorum
	lablog.Debug(rf.me, lablog.Snap, "Install snapshot successful, LastIncludedIndex %d, LastIncludedTerm %d, CommitIndex %d, and LastAppliedIndex %d", rf.LastIncludedIndex, rf.LastIncludedTerm, rf.commitIndex, rf.lastApplied)
	encodedState := rf.encodedRaftState()
	rf.persister.SaveStateAndSnapshot(encodedState, args.Data)
	rf.applyCond.Cond.Broadcast()
	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	ok := rf.rpcServer.Call(server, "Raft.InstallSnapshot", args, reply)
	return ok
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) quorumAdapter() {
	// timer := time.NewTimer(QUORUM_CHECK_TIME)
	////fmt.Println("Inside the quorum adapter")
	for !rf.killed() {
		// TODO: Add select for the quorum change channel.
		// TODO: if leader was part of the commit quorum then change the quorum immediately.  consider the nearest nodes. or
		//
		// //fmt.Println("Quourm going into select")
		select {
		case <-time.After(QUORUM_CHECK_TIME):
			//fmt.Println("Quourm adapter timer")
		case <-rf.quorumChangeChan:
			//fmt.Println("Quourm adapter wake")
		}
		// //fmt.Println("Quourm adapter timer outside select")
		// if timer is still active. stop it.
		rf.mu.Lock()
		//fmt.Println("Match indexes: ", rf.matchIndex)
		//fmt.Println("Current commit quourm", rf.CommitQuorum, " Pending commit quorum", rf.pendingCommitQuorum, " Pending quorum index", rf.pendingQuorumChangeIndex, " Commit index", rf.commitIndex)
		if rf.role != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		// //fmt.Println("Time reset done")
		// check if the commit quorum is maintaining the latency requirements.
		if rf.pendingQuorumChangeIndex != NULL_VALUE || rf.pendingQuorumChange {
			rf.mu.Unlock()
			continue
		}
		meetsLatencyRequirement := true
		for _, value := range rf.CommitQuorum {
			// if leader was part of the commit quorum, we need to change the quorum.
			if value == rf.me || rf.ewmavgMap[value].getAvg() > RESPONSE_LATENCY_REQUIREMENT || rf.unansweredRPCCount[value] >= MAX_UNANSWERED_RPC {
				meetsLatencyRequirement = false
				break
			}
		}
		// //fmt.Println(rf.CommitQuorum, " Meets Latency req: ", meetsLatencyRequirement, " EWMA: ", rf.ewmavgMap)
		if !meetsLatencyRequirement || len(rf.CommitQuorum) == 0 {
			// find the fastest quorum -> priority queue
			heap := make([]EwmaEntry, 0)
			for _, value := range rf.ewmavgMap {
				heap = append(heap, value)
			}
			var newCommitQuorum []int
			sort.Slice(heap, func(i, j int) bool {
				return (heap[i].getAvg() + float64(rf.unansweredRPCCount[heap[i].id])) < (heap[j].getAvg() + float64(rf.unansweredRPCCount[heap[j].id]))
			})
			for i := 0; i < COMMIT_QUORUM_SIZE && i < len(heap); i++ {
				newCommitQuorum = append(newCommitQuorum, heap[i].id)
			}

			// check if the newly selected quorum is equal to the old quorum
			// This could happen if the previous quorum is still the fastest and doesn't maintain latency requirements.
			sort.Slice(newCommitQuorum, func(i, j int) bool {
				return newCommitQuorum[i] < newCommitQuorum[j]
			})
			if checkSliceEquals(newCommitQuorum, rf.CommitQuorum) {
				// no need to change the quorum as they are equal.
				rf.mu.Unlock()
				continue
			}
			// append the entry to the log.
			// keep track of the index
			// update the metadata fields
			// maybe make commit quorum nil or empty
			lastLogIndex, _ := rf.getLastLogIndexAndTerm()
			//fmt.Println(rf.me, " Leader changing commit quorum: ", newCommitQuorum)
			noOpEntry := NoOpEntry{
				LeaderId:     rf.me,
				CommitQuorum: newCommitQuorum,
			}
			// //fmt.Println(noOpEntry)
			entry := Log{
				Term:          rf.CurrentTerm,
				Index:         lastLogIndex + 1,
				Type:          NO_OP,
				Configuration: noOpEntry,
			}
			rf.Logs = append(rf.Logs, entry)
			rf.pendingQuorumChange = true
			rf.pendingQuorumChangeIndex = lastLogIndex + 1
			rf.pendingCommitQuorum = newCommitQuorum
			rf.persist()
			rf.sendAppendEntries(rf.CurrentTerm, ALL_NODES)
			// //fmt.Println("Match indexes: ", rf.matchIndex)
		}
		rf.mu.Unlock()

	}
}

func checkSliceEquals(first, second []int) bool {
	if len(first) != len(second) {
		return false
	}

	for i := range first {
		if first[i] != second[i] {
			return false
		}
	}

	return true

}

func (rf *Raft) sendWakeOnQuorumChangeChannel(peer int) {
	// check if peer is part of commit quorum
	// if yes check if wake should be sent.
	// if idx := slices.IndexFunc(rf.CommitQuorum, func(c int) bool { return c == "key1" }
	// //fmt.Println("peer: ", peer, " commit quorum: ", rf.CommitQuorum, "unanswered count: ", rf.unansweredRPCCount[peer])
	if slices.Index(rf.CommitQuorum, peer) == -1 {
		return
	}
	if rf.unansweredRPCCount[peer] >= MAX_UNANSWERED_RPC && rf.pendingQuorumChangeIndex == NULL_VALUE {
		// //fmt.Println(rf.me, " Leader, sending wake to quorum change channel.")
		go func() {
			time.Sleep(1 * time.Millisecond)
			select {
			case rf.quorumChangeChan <- true:
			default:
			}
		}()
	}
}

func init() {
	labgob.Register(NoOpEntry{})
	labgob.Register(AppendEntriesArgs{})
	labgob.Register(AppendEntriesReply{})
	labgob.Register(RequestVoteArgs{})
	labgob.Register(RequestVoteReply{})
	labgob.Register(Log{})
	deadlock.Opts.Disable = true
	deadlock.Opts.PrintAllCurrentGoroutines = false

	// deadlock.Opts.PrintAllCurrentGoroutines = true
}
