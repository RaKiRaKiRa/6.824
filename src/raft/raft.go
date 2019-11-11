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
	"sync"
	"sync/atomic"
)
import "labrpc"

//import "bytes"
//import "labgob"
import "math/rand"
import "time"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type NodeState int8
const(
	Follower   = NodeState(1)
	Candidate  = NodeState(2)
	Leader	   = NodeState(3)
)

const(
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond // wait time for election is random
	ElectionTimeoutUpper = time.Duration(500) * time.Millisecond
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 锁保护共享访问这个对等的状态
	peers     []*labrpc.ClientEnd // RPC end points of all peers						所有的RPC端点
	persister *Persister          // Object to hold this peer's persisted state			保存这个对等的持续状态de对象
	me        int                 // this peer's index into peers[]						这个peer的索引

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm     int 				// lastest term that server has seen
	votedFor        int					// candidate Id that received vote in current term(or null if none) 当期获得投票的候选人ID
	state 			NodeState			//

	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term 		int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int   // candidate's term
	VoteGranted bool  // true mean candidate requesting vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// do not vote it
	// fig2 -> RequestVote RPC-> Receiver implementation
	if rf.currentTerm > args.Term || (rf.votedFor != -1 && rf.currentTerm >= args.Term){
		DPrintf("%v did not vote for node %d at term %d \n", rf, args.CandidateId, args.Term);
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.currentTerm = reply.Term
	reply.VoteGranted = true
	rf.electionTimer.Reset(randElectionTimer())
	rf.setState(Follower)
}

//
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
//

/*
	将RequestVote RPC发送到服务器的示例代码。
	server是rf.peers []中目标服务器的索引。期望args中的RPC参数。
	用RPC回复填充*reply，因此调用方应传递＆reply。传递给Call（）的args和reply的类型必须与在处理程序函数中声明的参数的类型（包括它们是否是指针）相同。
	labrpc软件包模拟了一个有损网络，在该网络中服务器可能无法访问，并且在其中请求和答复可能会丢失。
	Call（）发送请求并等待答复。如果答复在超时间隔内到达，则Call（）返回true；否则，返回true。否则，Call（）返回false。
	因此，Call（）可能不会暂时返回。错误的返回可能由服务器故障，无法访问的活动服务器，请求丢失或答复丢失引起。
	如果服务器端的处理函数未返回，则保证Call（）返回（可能在延迟之后）* except *。因此，无需在Call（）周围实现自己的超时。
	请查看../labrpc/labrpc.go中的评论以获取更多详细信息。
	如果您无法使RPC正常工作，请检查是否已大写通过RPC传递的结构中的所有字段名，并且调用方使用＆传递了应答结构的地址，而不是结构本身。
 */
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
/*
	服务或测试人员想要创建Raft服务器。
	所有Raft服务器（包括该Raft服务器）的端口都位于peers []中。
	该服务器的端口是peers [me]。
	所有服务器的peers []数组的顺序相同。
	持久性是该服务器保存其持久状态的位置，并且最初还保存最近保存的状态（如果有）。
	applyCh是测试人员或服务期望Raft发送ApplyMsg消息的通道。
	Make（）必须快速返回，因此对于任何长时间运行的工作，它都应该启动goroutines。
 */
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor    = -1
	rf.state       = Follower
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer  = time.NewTimer(randElectionTimer())

	// start a goroutine for Timer
	go func(rf *Raft){
		for{
			select {
			case<-rf.electionTimer.C: //start election, only for Follower, it will reset when recv a appendEntries or RequestVote
				rf.mu.Lock()
				if rf.state == Follower{
					rf.setState(Candidate)		// it will start a election
				}else {
					rf.startElection()			// this is for candidate continue (c): a period of time goes by with no winner -- each candidate will time out and start a new election
				}
				rf.mu.Unlock()

			case<-rf.heartbeatTimer.C: // only useful for leader, send heartbeat
				rf.mu.Lock()
				if(rf.state == Leader){
					rf.broadHeartbeat()
					rf.heartbeatTimer.Reset(HeartbeatInterval) // if state is not leader, it will stop
				}
				rf.mu.Unlock()
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randElectionTimer() time.Duration {
	return time.Duration(rand.Int63n(ElectionTimeoutUpper.Nanoseconds() - ElectionTimeoutLower.Nanoseconds()) + ElectionTimeoutLower.Nanoseconds()) * time.Nanosecond
}

func (rf *Raft) setState(state NodeState){
	if state == rf.state{
		return
	}

	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)

	rf.state = state
	// init for each state
	switch rf.state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randElectionTimer())
		rf.votedFor = -1

	case Candidate:
		rf.startElection()

	case Leader:
		rf.electionTimer.Stop()
		rf.broadHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval)

	}
}


// 2A, 5.2
func (rf *Raft) startElection(){
	rf.currentTerm += 1
	rf.electionTimer.Reset(randElectionTimer())
	// send RequestVote Rpc to each peer
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	var voteCount int32
	voteCount = 1
	rf.votedFor = rf.me
	for i := range rf.peers{
		if i == rf.me{
			continue;
		}
		// create a goroutine for RequestVote RPC
		go func(server int){
			var reply RequestVoteReply

			if rf.sendRequestVote(server, &args, &reply){
				rf.mu.Lock()
				// if has voted
				if reply.VoteGranted == true && rf.state == Candidate{
					atomic.AddInt32(&voteCount, 1)
					// if it receives votes from a majority of the servers
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2){
						rf.setState(Leader)
					}
				}else{
					// if the term of reply is large than currentTerm, the candiate recognizes the leader as legitimate and returns to follower state
					// Figure2 -> rules for servers -> all servers -> 2
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.setState(Follower)
					}
				}
				rf.mu.Unlock();
			}else{
				DPrintf("%v send request vote to %d failed", rf, server)
			}
		}(i)
	}
}

// TODO
// AppendEntries in 2A just is used as heartbeat
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term 		int    // leader's term
	LeaderId    int    // so follower can redirect clients
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term 		int   // currentTerm for leader to update itself
	Success     bool  // true mean follower contained entry
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Success = true
	rf.currentTerm = args.Term
	rf.electionTimer.Reset(randElectionTimer())
	rf.setState(Follower)
}


func (rf *Raft) sendAppendEntriesVote(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadHeartbeat(){
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	for i := range rf.peers{
		if i == rf.me{
			continue
		}
		// create a goroutine for AppendEntries RPC
		go func(server int){
			var reply AppendEntriesReply
			if rf.sendAppendEntriesVote(server, &args, &reply){
				rf.mu.Lock()
				//reply.term > rf.currentTer means false,and means there is a new Leader
				// Figure2 -> rules for servers -> all servers -> 2
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.setState(Follower)
				}
				rf.mu.Unlock()
			} else{
				DPrintf("%v send Append Entries to %d failed", rf, server)
			}
		}(i)
	}
}