// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// 10-20间随机生成的超时时间
	randomizedTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).

	// 检验c的有效性并从存储中加载
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, cs, _ := c.Storage.InitialState()

	// 新建Raft结点
	r := &Raft{
		id:               c.ID,                       // 节点的唯一标识符
		Term:             hs.Term,                    // 当前任期
		Vote:             hs.Vote,                    // 当前任期内投票给的候选人ID
		RaftLog:          newLog(c.Storage),          // Raft 日志
		Prs:              make(map[uint64]*Progress), // 集群中所有节点的日志复制进度
		State:            StateFollower,              // 初始状态为 Follower
		votes:            make(map[uint64]bool),      // 投票结果记录
		msgs:             []pb.Message{},             // 待发送的消息队列
		Lead:             None,                       // 当前的领导者ID
		leadTransferee:   None,                       // 正在进行领导者转移的目标节点ID
		heartbeatTimeout: c.HeartbeatTick,            // 心跳超时时间
		electionTimeout:  c.ElectionTick,             // 选举超时时间
	}

	// 设置进度
	if len(c.peers) == 0 {
		// 如果 peers 为空，则使用从存储中恢复的节点列表
		for _, p := range cs.Nodes {
			r.Prs[p] = &Progress{}
		}
	} else {
		// 否则，使用配置中提供的节点列表
		for _, p := range c.peers {
			r.Prs[p] = &Progress{}
		}
	}

	// 设置应用索引
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	id, ok := r.Prs[to]
	if !ok {
		return false
	}
	prevLogIndex := id.Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	// 将未完成同步的部分都发给对方
	firstIndex := r.RaftLog.FirstIndex()
	var entries []*pb.Entry
	for i := id.Next; i < r.RaftLog.LastIndex()+1; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend, // 消息类型：日志追加
		To:      to,                       // 目标节点ID
		From:    r.id,                     // 发送者节点ID
		Term:    r.Term,                   // 当前任期
		LogTerm: prevLogTerm,              // leader上一个日志条目的任期
		Index:   prevLogIndex,             // leader记录的follower上一个日志条目的索引
		Entries: entries,                  // 需要复制的日志条目
		Commit:  r.RaftLog.committed,      // 已提交的日志索引
	}

	r.msgs = append(r.msgs, msg)

	return false
}

// broadcastAppend 用于 Leader 向所有 Follower 发送日志复制请求
func (r *Raft) broadcastAppend() bool {
	// 单结点直接返回false
	if len(r.Prs) == 1 {
		return false
	}
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
	return true
}

// sendAppendResponse 发送 Follower 回复 Leader 的日志复制请求消息
func (r *Raft) sendAppendResponse(to uint64, index uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse, // 消息类型：日志追加响应
		Term:    r.Term,                           // 当前任期
		To:      to,                               // 目标节点ID
		From:    r.id,                             // 发送者节点ID
		Reject:  reject,                           // 是否拒绝该请求
		Index:   index,                            // 日志条目索引
	}
	r.msgs = append(r.msgs, msg)
	return
}

// broadcastAppend 用于 Leader 向所有的 Follower 广播发送心跳包
func (r *Raft) broadcastHeartBeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 检查节点是否在集群中
	if _, in := r.Prs[to]; !in {
		log.Panic("peer not in the cluster")
		return
	}

	// 构造心跳消息
	// Term: 用于确保当前心跳消息是最新的。如果接收者发现心跳消息的任期比自己当前的任期大，它会更新自己的任期，并将自己转变为跟随者。
	// Commit: 通常应该是无效索引，在这里可以设置一下，确保跟随者知道最新的提交索引。
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat, // 消息类型：心跳
		Term:    r.Term,                      // 当前任期
		Commit:  r.RaftLog.committed,         // 已提交的日志索引
		To:      to,                          // 目标节点ID
		From:    r.id,                        // 发送者节点ID
	}
	// 添加消息到消息队列
	r.msgs = append(r.msgs, msg)
}

// sendHeartBeatResponse sends a HeartBeatResponse to the given peer
func (r *Raft) sendHeartBeatResponse(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse, // 消息类型：心跳响应
		Term:    r.Term,                              // 当前任期
		To:      to,                                  // 目标节点ID
		From:    r.id,                                // 发送者节点ID
		Commit:  r.RaftLog.committed,                 // 已提交的日志索引
	}
	r.msgs = append(r.msgs, msg)
	return
}

// sendRequestVote sends a vote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).

	// 检查节点是否在集群中
	if _, in := r.Prs[to]; !in {
		log.Panic("peer not in the cluster")
		return
	}

	// 获取最后一个日志条目的任期
	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		return
	}

	// 构造请求投票消息
	// Term: 用于确保当前的选举是最新的，如果接收者发现请求投票的任期比自己当前的任期大，它会更新自己的任期并考虑投票。
	// Index: 这个字段同样用于确保候选者的日志是最新的。如果接收者的日志比候选者更新，则不会投票给候选者。
	// LogTerm: 这个字段用于确保候选者的日志是最新的，如果接收者的日志比候选者更新（即拥有更高的 LogTerm 或者相同 LogTerm 但更长的日志），则不会投票给候选者
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote, // 消息类型：请求投票
		Term:    r.Term,                        // 当前任期
		Index:   r.RaftLog.LastIndex(),         // 最后一个日志条目的索引
		LogTerm: logTerm,                       // 最后一个日志条目的任期
		To:      to,                            // 目标节点ID
		From:    r.id,                          // 发送者节点ID
	}

	// 添加消息到消息队列
	r.msgs = append(r.msgs, msg)
}

// sendRequestVoteResponse sends a vote response to the given peer
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse, // 消息类型：请求投票响应
		Term:    r.Term,                                // 当前任期
		Reject:  reject,                                // 是否拒绝投票
		To:      to,                                    // 目标节点ID
		From:    r.id,                                  // 发送者节点ID
	}
	r.msgs = append(r.msgs, msg)
}

/******** 随机数生成 ********/
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

/******** 随机数生成 ********/

// RandomizedElectionTimeout 限制选举时间 10 ~ 20 之间
func (r *Raft) RandomizedElectionTimeout() {
	r.randomizedTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	// Follower心跳超时和Candidate选举超时都会触发新一轮的选举
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedTimeout {
			r.electionElapsed = 0
			// 通过Step发送开始选举消息
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 使用MsgBeat触发发送心跳消息
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 重置到Follower状态
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.RandomizedElectionTimeout()
	r.Vote = None
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.RandomizedElectionTimeout()
	// 投票给自己并记录
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.RandomizedElectionTimeout()

	// 成为leader之后需要初始化每个节点的日志进度，包括Match和Next两个字段、
	// Match 表示当前跟随者已经复制的最大日志条目的索引，如果一个日志条目在大多数节点上的 Match 索引都大于等于这个日志条目的索引，那么领导者可以将这个日志条目提交。
	// Next 表示领导者需要发送给跟随者的下一个日志条目的索引，控制领导者向跟随者发送 AppendEntries 请求的进度，如果跟随者的日志缺失或落后，领导者会根据 Next 字段的值决定从哪个索引开始发送日志条目。
	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Match: 0,             // 初始为 0，表示还没有复制任何日志条目
			Next:  lastIndex + 1, // 初始为最后一个日志条目的下一个索引，表示从这个索引开始发送日志条目
		}
	}

	// 追加一条空日志，并更新自己的match和next
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIndex + 1})
	r.Prs[r.id].Next++
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1

	// 如果只有一个结点，直接尝试commit
	if len(r.Prs) == 1 {
		r.maybeCommit()
	}

	// 向peers发送添加请求，在进行2AB的时候暂且不用打开
	// TODO 在2AB测试的时候进行注释，避免重复消息
	r.broadcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 根据不同角色转发消息
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartVote()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleStartVote()
	}
	return err
}

func (r *Raft) stepCandidate(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartVote()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleStartVote()
	}
	return err
}

func (r *Raft) stepLeader(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartBeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTimeoutNow:
		r.handleStartVote()
	}
	return err
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleStartVote() {
	// Your Code Here (2A).
	r.electionElapsed = 0

	if _, in := r.Prs[r.id]; !in {
		return
	}

	// 切换为Candidate状态
	r.becomeCandidate()

	if len(r.Prs) == 1 {
		// 满足测试函数 TestLeaderElectionInOneRoundRPC2AA
		// 当集群中只有一个结点，不用投票直接变成Leader
		r.becomeLeader()
	} else {
		// 向所有的peers发送投票请求
		for id := range r.Prs {
			if id != r.id {
				r.sendRequestVote(id)
			}
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 更新任期和状态
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	// 如果领导人的任期小于接收者的当前任期，拒绝append
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), true)
		return
	}

	// 更新当前的Leader信息
	if m.From != r.Lead {
		r.Lead = m.From
	}

	// 日志一致性检查，检查日志条目的前一个索引和任期是否匹配
	// leader记录的follower的上一条索引值大于实际存储的最后一条日志的索引，拒绝append
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), true)
		return
	}
	// follower最后一条日志的任期和leader在同样位置处的日志任期不同，拒绝append
	if tmpTerm, _ := r.RaftLog.Term(m.Index); tmpTerm != m.LogTerm {
		r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), true)
		return
	}

	// 追加新条目，同时删除冲突
	for _, entry := range m.Entries {
		index := entry.Index
		oldTerm, err := r.RaftLog.Term(index)
		if index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if oldTerm != entry.Term || err != nil {
			// 删除冲突的条目和之后的所有条目
			r.RaftLog.entries = r.RaftLog.entries[:index-r.RaftLog.FirstIndex()]
			r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	// 发送追加的response
	r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), false)

	// 更新follower的commit值
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
}

// handleAppendEntriesResponse handle AppendEntriesResponse RPC response
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// Your Code Here (2A).
	// 日志同步失败，逐渐减少Next进行复制尝试
	if m.Reject {
		r.Prs[m.From].Next = min(m.Index+1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
		return
	}

	// 同步成功, 更新 follower 的 match 和 next
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// 记录旧的 commited ，尝试更新 committed
	oldCom := r.RaftLog.committed
	r.maybeCommit()

	// 如果committed更新，发送 AppendEntries RPC 来通知 Follower 已提交的日志条目。
	if r.RaftLog.committed != oldCom {
		r.broadcastAppend()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// 转换 leader
	if m.From != r.Lead {
		r.Lead = m.From
	}
	// 重置时间
	r.electionElapsed = 0
	// 回应
	r.sendHeartBeatResponse(m.From)
	return
}

// handleHeartbeatResponse handle HeartbeatResponse RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).

	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	// 如果follower的commit落后，leader发送append消息
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	// 更新任期和状态
	if r.Term < m.Term {
		r.Term = m.Term
		r.Vote = None
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}

	// 如果之前已经投过一次票，这次相同直接放行
	if r.Vote == m.From {
		r.sendRequestVoteResponse(m.From, false)
		return
	}

	// 如果当前节点还没有投票才能继续投票，避免多次投票
	if r.Vote == None {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		// 如果候选者的日志比当前节点的日志更新，则投票给候选者
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.sendRequestVoteResponse(m.From, false)
			// 投票并更新任期然后变换身份
			r.Vote = m.From
		} else {
			r.sendRequestVoteResponse(m.From, true)
		}
	} else {
		r.sendRequestVoteResponse(m.From, true)
	}
}

// handleRequestVoteResponse handle RequestVoteResponse RPC response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Your Code Here (2A).
	totalNum := len(r.Prs)
	agrNum := 0
	denNum := 0

	// 记录投票
	r.votes[m.From] = !m.Reject

	// 统计票数
	for _, vote := range r.votes {
		if vote {
			agrNum++
		} else {
			denNum++
		}
	}

	// 赞同超过半数就变为leader，拒绝超过半数直接变为follower
	if 2*agrNum > totalNum {
		r.becomeLeader()
	} else if 2*denNum >= totalNum {
		r.becomeFollower(r.Term, None)
	}
}

// handlePropose Leader 接收客户端日志，进行提交并通知其余结点
func (r *Raft) handlePropose(m pb.Message) {
	// 将客户端发来的日志添加到 Leader 自己的 Entry 中
	// 获取当前的最后一个日志索引
	lastIndex := r.RaftLog.LastIndex()

	// 遍历所有条目并追加到日志中
	for _, entry := range m.Entries {
		entry.Index = lastIndex + 1
		entry.Term = r.Term
		lastIndex = entry.Index
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	// 更新自己match和next
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 发送追加RPC，若单结点直接更新
	if single := r.broadcastAppend(); !single {
		r.maybeCommit()
	}
}

// maybeCommit 更新 committed，一半以上的结点完成日志复制即可提交
func (r *Raft) maybeCommit() {
	// 获取所有节点的Match索引
	matchIndexes := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		matchIndexes[i] = prs.Match
		i++
	}

	// 对Match索引进行排序
	sort.Sort(matchIndexes)

	// 获取大多数节点的Match索引
	majorityMatchIndex := matchIndexes[(len(r.Prs)-1)/2]

	// 检查是否存在一个N满足条件
	for N := majorityMatchIndex; N > r.RaftLog.committed; N-- {
		if term, _ := r.RaftLog.Term(N); term == r.Term {
			r.RaftLog.committed = N
			break
		}
	}
}

// softState 保存当前易失性状态
func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,  // 当前集群中的领导者节点 ID
		RaftState: r.State, // 当前节点的状态（Follower, Candidate, Leader）
	}
}

// hardState 保存当前持久性状态
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,              // 当前的任期编号
		Vote:   r.Vote,              // 任期内投票给的候选人 ID
		Commit: r.RaftLog.committed, // 已提交的日志条目的索引
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
