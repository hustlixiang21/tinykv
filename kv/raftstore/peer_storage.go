package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	// 如果没有新条目，则直接返回
	if len(entries) == 0 {
		return nil
	}

	// 获取新日志条目的起始和结束索引
	newFirstIndex := entries[0].Index
	newLastIndex := entries[len(entries)-1].Index

	// 获取当前存储的日志条目的起始和结束索引
	currentFirstIndex, _ := ps.FirstIndex()
	currentLastIndex, _ := ps.LastIndex()

	// 如果新条目的最后一个索引小于当前存储的第一个索引，则无需任何操作
	if newLastIndex < currentFirstIndex {
		log.Info("newLastIndex:%d < currentFirstIndex:%d", newLastIndex, currentFirstIndex)
		return nil
	}

	// 如果新条目的第一个索引小于当前存储的第一个索引，调整新条目的起始位置
	if newFirstIndex < currentFirstIndex {
		entries = entries[currentFirstIndex-newFirstIndex:]
	}

	// 将新条目添加到写入批次中
	for _, entry := range entries {
		err := raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, entry.Index), &entry)
		if err != nil {
			log.Error("Failed to set Raft log entry: ", err)
			return err
		}
	}

	// 删除新条目后面的所有冲突旧条目
	if newLastIndex < currentLastIndex {
		for i := newLastIndex + 1; i <= currentLastIndex; i++ {
			raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, i))
		}
	}

	// 更新 RaftLocalState
	ps.raftState.LastIndex = entries[len(entries)-1].Index
	ps.raftState.LastTerm = entries[len(entries)-1].Term

	return nil
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	return nil, nil
}

// SaveReadyState Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
// 1. 通过 raft.isEmptySnap() 方法判断是否存在 Snapshot，如果有，则调用 ApplySnapshot() 方法应用（2C）；
// 2. 调用 Append() 将需要持久化的 entries 保存到 raftDB；
// 3. 保存 ready 中的 HardState 到 ps.raftState.HardState，注意先使用 raft.isEmptyHardState() 进行判空；
// 4. 持久化 RaftLocalState 到 raftDB；
// 5. 通过 WriteRaft 和 WriteKV 进行数据库写入。
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).

	// 创建用于 Raft 和 KV 引擎的写入批次
	raftWB := new(engine_util.WriteBatch)
	kvWB := new(engine_util.WriteBatch)

	// 用于存储应用快照的结果（如果有的话）
	var applySnapResult *ApplySnapResult

	// 1. 将日志条目追加到 Raft 引擎
	if len(ready.Entries) > 0 {
		err := ps.Append(ready.Entries, raftWB)
		if err != nil {
			log.Error("Failed to append entries: ", err)
			return applySnapResult, err
		}
	}

	// 2. 如果有快照，则应用快照
	// TODO 完成ApplySnapshot
	if !raft.IsEmptySnap(&ready.Snapshot) {
		// 将快照应用到 KV 引擎
		applySnapResult, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			log.Error("Failed to apply snapshot: ", err)
			return applySnapResult, err
		}
	}

	// 3. 将 HardState 保存到 RaftLocalState
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}

	// 4. 更新 RaftLocalState 到 raftWB
	err := raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)
	if err != nil {
		log.Error("Failed to set Raft state: ", err)
		return applySnapResult, err
	}

	// 5. 将 raftWB 和 kvWB 写入DB
	if err := ps.Engines.WriteRaft(raftWB); err != nil {
		log.Error("Failed to write Raft batch: ", err)
		return applySnapResult, err
	}
	if err := ps.Engines.WriteKV(kvWB); err != nil {
		log.Error("Failed to write KV batch: ", err)
		return applySnapResult, err
	}

	return applySnapResult, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
