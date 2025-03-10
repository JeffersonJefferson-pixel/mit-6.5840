package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartBeatInterval = 120

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all servers.
	state State

	currentTerm int        // latest term the server has seen.
	votedFor    int        // candidate id that received vote in current term.
	log         []LogEntry // log entries.

	// volatile state on all servers.
	commitIndex int // index of highest log entry known to be committed.
	lastApplied int // index of highest log entry applied to state machine.

	// volatile state on leaders.
	nextIndex  []int // index of next log entry to send for each server
	matchIndex []int // index of highest log entry known to be replicated for each server

	applyCh     chan ApplyMsg
	winElectCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
	stepDownCh  chan bool

	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic(errors.New("decode error"))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term.
	CandidateId  int // id of candidate.
	LastLogIndex int // index of candidate's last log entry.
	LastLogTerm  int // term of candidate's last log entry.
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // current term for candidate to update itself.
	VoteGranted bool // whether candidate received vote.
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	DPrintf("[%v, %v]: recieved request vote from %v at %v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// candidate has lower term.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// candidate has higher term
	if args.Term > rf.currentTerm {
		// step down to be follower
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// server has not voted for other or has voted for candidate
	// and candidate's log is at least up-to-date with server's log.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// grant vote
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// persist state.
		rf.persist()

		sendToChannel(rf.grantVoteCh, true)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if still candidate
	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	// there is leader with higher term
	if reply.Term > rf.currentTerm {
		// step down to follower
		rf.becomeFollower(args.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		// if received vote from majority, become the leader
		if rf.voteCount >= len(rf.peers)/2+1 {
			// send to win elect channel
			sendToChannel(rf.winElectCh, true)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader's term.
	LeaderId     int        // follower can use to redirect clients to leader.
	PrevLogIndex int        // index of log entry immediately preceding the new log entries.
	PrevLogTerm  int        // term of entry at PrevLogIndex.
	Entries      []LogEntry // log entries to store. (empty for heartbeat; may send more than one for efficiency).
	LeaderCommit int        // leader's commitIndex.
}

type AppendEntriesReply struct {
	Term          int  // currentTerm for leader to update itself.
	Success       bool // whether follower contained entry matching prevLogIndex and prevLogTerm.
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%v, %v]: recieved append entries from %v at %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	commitIndex := rf.commitIndex

	// leader has lower term
	if args.Term < rf.currentTerm {
		// it should no longer be leader
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		return
	}

	// leader has higher term.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	lastLogIndex := rf.getLastLogIndex()
	sendToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// shorter log index
	if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		return
	}

	// server doesn't contain an entry at prevLogIndex with matching prevLogTerm
	if rf.getLastLogTerm() != args.PrevLogTerm {
		reply.ConflictTerm = args.PrevLogTerm
		// find last matching log index in the conflict term
		for i := args.PrevLogIndex; i >= 0 && rf.log[i].Term == args.PrevLogTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	// handle conflicting log entries
	i, j := args.PrevLogIndex+1, 0
	for ; i < lastLogIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}

	rf.log = rf.log[:i]
	rf.log = append(rf.log, args.Entries[j:]...)

	rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = true

	// update commit index
	if args.LeaderCommit > commitIndex {
		newLastLogIndex := rf.getLastLogIndex()

		if args.LeaderCommit < newLastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = newLastLogIndex
		}

		go rf.applyLogs()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%v, %v]: send append entries to %v", rf.me, rf.currentTerm, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if still leader
	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	// thre is leader with higher term
	if reply.Term > rf.currentTerm {
		// step down to follower
		rf.becomeFollower(args.Term)
		return
	}

	if reply.Success {
		//update next index and match index for follower
		matchIndex := args.PrevLogIndex + len(args.Entries)
		if matchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = matchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm == -1 {
		// conflicting index case
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// conflicting term case
		// find conflict term in log
		nextIndex := rf.getLastLogIndex()
		for ; nextIndex >= 0; nextIndex-- {
			if rf.log[nextIndex].Term == reply.ConflictTerm {
				break
			}
		}
		if nextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = nextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// check if majority committed
	for n := rf.getLastLogIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Your code here (3B).
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{term, command})

	// persist log.
	rf.persist()

	return rf.getLastLogIndex(), term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		// raft state machine
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			// send heartbeat every 100 milliseconds
			case <-time.After(HeartBeatInterval * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			// start election
			case <-time.After(rf.getElectionTimeout()):
				rf.becomeCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				rf.becomeLeader()
			// try election again
			case <-time.After(rf.getElectionTimeout()):
				rf.becomeCandidate(Candidate)
			}
		}
	}
}

func (rf *Raft) getElectionTimeout() time.Duration {
	// between 360ms to 500ms
	ms := 360 + (rand.Intn(240))
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) becomeLeader() {
	DPrintf("[%v, %v]: become leader", rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex
	}

	rf.broadcastAppendEntries()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) getServerLastLogIndex(server int) int {
	if rf.nextIndex[server]-1 >= 0 {
		return rf.nextIndex[server] - 1
	} else {
		return 0
	}
}

func (rf *Raft) getServerLastLogTerm(server int) int {
	return rf.log[rf.getServerLastLogIndex(server)].Term
}

func (rf *Raft) getServerNextLogEntries(server int) []LogEntry {
	// make deep copy
	entries := rf.log[rf.nextIndex[server]:]
	entries_copy := make([]LogEntry, len(entries))
	copy(entries_copy, entries)
	return entries_copy
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("[%v, %v]: become follower at term %v", rf.me, rf.currentTerm, term)

	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

	if state != Follower {
		sendToChannel(rf.stepDownCh, true)
	}

	rf.persist()
}

func (rf *Raft) becomeCandidate(fromState State) {
	DPrintf("[%v, %v]: become candidate", rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1

	rf.persist()

	rf.broadcastRequestVote()
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
}

func (rf *Raft) broadcastRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for i := range rf.peers {
		// skip self.
		if i != rf.me {
			// different reply for each peer.
			reply := RequestVoteReply{}

			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	myLastLogIndex, myLastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()

	return (lastLogTerm > myLastLogTerm) || (lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("[%v, %v]: broadcast append entries", rf.me, rf.currentTerm)
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		// different reply for each peers.
		// skip self.
		if i != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.getServerLastLogIndex(i),
				PrevLogTerm:  rf.getServerLastLogTerm(i),
				LeaderCommit: rf.commitIndex,
				Entries:      rf.getServerNextLogEntries(i),
			}
			reply := AppendEntriesReply{}

			// send rpc
			go rf.sendAppendEntries(i, &args, &reply)
		}

	}
}

func sendToChannel(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	// init log for term 0
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.applyCh = applyCh
	rf.grantVoteCh = make(chan bool)
	rf.winElectCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.stepDownCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
