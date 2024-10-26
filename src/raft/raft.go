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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	Term int
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
	state         State
	electionTimer time.Time

	currentTerm int        // latest term the server has seen.
	votedFor    int        // candidate id that received vote in current term.
	log         []LogEntry // log entries.

	// volatile state on all servers.
	commitIndex int // index of highest log entry known to be committed.
	lastApplied int // index of highest log entry applied to state machine.

	// volatile state on leaders.
	nextIndex  []int // index of next log entry to send for each server
	matchIndex []int // index of highest log entry known to be replicated for each server

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
	rf.mu.Lock()
	term := rf.currentTerm
	votedFor := rf.votedFor
	var lastLogIndex int
	if rf.log != nil {
		lastLogIndex = len(rf.log) - 1
	} else {
		lastLogIndex = -1
	}
	rf.mu.Unlock()

	// candidate has lower term.
	if args.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	// candidate has higher term
	if args.Term > term {
		// update term and state to follower
		rf.mu.Lock()
		rf.currentTerm = term
		rf.state = Follower
		rf.mu.Unlock()
	}

	// server has not voted for other or has voted for candidate
	// and candidate's log is at least up-to-date with server's log.
	if (votedFor == -1 || votedFor == args.CandidateId) && args.LastLogIndex >= lastLogIndex {
		// grant vote
		reply.Term = args.Term
		reply.VoteGranted = true
		// reset election time out
		rf.mu.Lock()
		rf.electionTimer = time.Now()
		rf.mu.Unlock()
		return
	}

	reply.Term = term
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	Term    int  // currentTerm for leader to update itself.
	Success bool // whether follower contained entry matching prevLogIndex and prevLogTerm.
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	var lastLogIndex int
	var prevLogTerm int
	if rf.log != nil {
		lastLogIndex = len(rf.log) - 1
		if args.PrevLogIndex != -1 {
			prevLogTerm = rf.log[args.PrevLogIndex].Term
		} else {
			prevLogTerm = -1
		}
	} else {
		lastLogIndex = -1
		prevLogTerm = -1
	}
	rf.mu.Unlock()

	// leader has lower term
	if args.Term < term {
		// it should no longer be leader
		reply.Term = term
		reply.Success = false
		return
	}

	// leader has high term.
	if args.Term > term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.mu.Unlock()
	}

	// server doesn't contain an entry at prevLogIndex with matching prevLogTerm
	if lastLogIndex <= args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
		reply.Term = term
		reply.Success = false
		return
	}

	// append new entries.
	rf.mu.Lock()
	rf.log = append(rf.log, args.Entries...)
	rf.mu.Unlock()

	// update commit index
	if args.LeaderCommit > commitIndex {
		lastNewEntryIndex := len(rf.log) + len(args.Entries)
		var newCommitIndex int

		if args.LeaderCommit < lastNewEntryIndex {
			newCommitIndex = args.LeaderCommit
		} else {
			newCommitIndex = lastNewEntryIndex
		}

		rf.mu.Lock()
		rf.commitIndex = newCommitIndex
		rf.mu.Unlock()
	}

	reply.Term = term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
		// Check if a leader election should be started.
		if rf.shouldStartLeaderElection() {
			rf.startLeaderElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) shouldStartLeaderElection() bool {
	rf.mu.Lock()
	electionTimer := rf.electionTimer
	state := rf.state
	rf.mu.Unlock()

	return time.Since(electionTimer) >= getElectionTimeout() && state != Leader
}

func getElectionTimeout() time.Duration {
	// between 500ms to 1000ms
	ms := 500 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	// old term
	term := rf.currentTerm
	// update state to candidate
	rf.state = Candidate
	// increment current term
	rf.currentTerm++
	var lastLogIndex int
	var lastLogTerm int
	if rf.log != nil {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogIndex = -1
		lastLogTerm = -1
	}
	// reset election timer
	rf.electionTimer = time.Now()

	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// vote for self.
	var vote atomic.Int32
	vote.Add(1)
	for i := 0; i < len(rf.peers); i++ {
		// skip self.
		if i == rf.me {
			continue
		}
		// different reply for each peer.
		reply := RequestVoteReply{}

		go func(server int) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.VoteGranted {
					vote.Add(1)
					// if received vote from majority, become the leader
					if int(vote.Load()) >= len(rf.peers)/2+1 {
						rf.becomeLeader()
					}
				} else {
					// term in reply is bigger
					if reply.Term > term {
						// update term and state
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.mu.Unlock()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	// already leader
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Leader
	rf.mu.Unlock()

	go rf.sendHeartbeat()
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	// old term
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	var prevLogIndex int
	var prevLogTerm int
	if rf.log != nil {
		prevLogIndex = len(rf.log) - 1
		prevLogTerm = rf.log[prevLogIndex-1].Term
	} else {
		prevLogIndex = -1
		prevLogTerm = -1
	}
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}

	for !rf.killed() {
		// check if still leader
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			// different reply for each peers.
			reply := AppendEntriesReply{}
			// skip self.
			if i == rf.me {
				continue
			}

			// send rpc
			go func(server int) {
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					if reply.Term > term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.mu.Unlock()
					}
				}
			}(i)
		}

		// send heartbeat every 100 milliseconds
		time.Sleep(time.Duration(100) * time.Millisecond)
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
	rf.electionTimer = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
