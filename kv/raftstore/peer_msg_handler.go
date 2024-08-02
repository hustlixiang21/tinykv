package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady
// 1. 通过 d.RaftGroup.HasReady() 方法判断是否有新的 Ready，没有的话就什么都不用处理。
// 2. 如果有 Ready 先调用 d.peerStorage.SaveReadyState(&ready) 将 Ready 中需要持久化的内容保存到 badger。如果 Ready 中存在 snapshot，则应用它。
// 3. 然后调用 d.Send(d.ctx.trans, ready.Messages) 方法将 Ready 中的 Messages 发送出去。
// 4. Apply ready.CommittedEntries 中的 entry。
// 5. 调用 d.RaftGroup.Advance(ready) 方法推进 RawNode。
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		// 将 Ready 中需要持久化的内容保存到 badger，如果有snapshot也要apply
		applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}

		// 根据日志应用结果调整ctx
		if applySnapResult != nil {
			if !reflect.DeepEqual(applySnapResult.PrevRegion, applySnapResult.Region) {
				d.peerStorage.SetRegion(applySnapResult.Region)
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
				d.ctx.storeMeta.Unlock()
			}
		}

		// 把ready中的信息发送出去
		d.Send(d.ctx.trans, ready.Messages)

		// 应用已提交的日志条目到 kvDB
		for _, committedEntry := range ready.CommittedEntries {
			kvWB := new(engine_util.WriteBatch)

			if committedEntry.EntryType == pb.EntryType_EntryNormal {
				// 处理普通条目
				err := d.applyNormalEntry(&committedEntry, kvWB)
				if err != nil {
					log.Errorf("Failed to apply normal entry: %v", err)
					return
				}
			} else if committedEntry.EntryType == pb.EntryType_EntryConfChange {
				// 处理配置变更条目
				err := d.applyConfChangeEntry(&committedEntry, kvWB)
				if err != nil {
					log.Errorf("Failed to apply confchange entry: %v", err)
					return
				}
			}
			// 更新以应用日志索引
			d.peerStorage.applyState.AppliedIndex = committedEntry.Index
		}

		// 推进状态
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) applyConfChangeEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) error {
	// 反序列化cmdRequest
	var cmdRequest raft_cmdpb.RaftCmdRequest
	err := cmdRequest.Unmarshal(entry.Data)
	if err != nil {
		return err
	}

	// 反序列化ConfChange
	var confChange pb.ConfChange
	err = confChange.Unmarshal(entry.Data)
	if err != nil {
		return err
	}

	// 更新 RegionState
	switch confChange.ChangeType {
	case pb.ConfChangeType_AddNode:
		d.applyAddNode(&confChange, &cmdRequest, kvWB)
	case pb.ConfChangeType_RemoveNode:
		d.applyRemoveNode(&confChange, kvWB)
	}

	// 调用 d.RaftGroup.ApplyConfChange() 方法，修改raft内部的Prs
	d.RaftGroup.ApplyConfChange(confChange)

	// 更新 scheduler 的 region 缓存，解决可能的 no region 错误
	d.notifyHeartbeatScheduler(d.Region(), d.peer)

	// 构造ChangePeer类型的AdminResponse
	adminResp := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}
	d.checkValidAndCallback(cmdResp, entry)

	return err
}

func (d *peerMsgHandler) applyAddNode(cc *pb.ConfChange, req *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) {
	// 结点不存在才能添加
	if !d.peer.peerExistInRegion(cc.NodeId) {
		// 更改 peer 并写入 RegionState
		d.ctx.storeMeta.Lock()
		defer d.ctx.storeMeta.Unlock()

		// 更新 RegionEpoch 的 ConfVer
		region := d.Region()
		region.RegionEpoch.ConfVer++

		// 添加新的 peer
		newPeer := &metapb.Peer{
			Id:      req.AdminRequest.ChangePeer.Peer.Id,
			StoreId: req.AdminRequest.ChangePeer.Peer.StoreId,
		}
		region.Peers = append(region.GetPeers(), newPeer)

		// 持久化修改后的 Region
		meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

		// 加入 peer 缓存
		d.insertPeerCache(newPeer)
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	}
}

func (d *peerMsgHandler) applyRemoveNode(cc *pb.ConfChange, kvWB *engine_util.WriteBatch) {
	// 分为销毁结点自身和其他结点
	if d.peer.PeerId() == cc.NodeId {
		// 调用 destroyPeer 删除节点自身
		d.destroyPeer()
	} else if d.peerExistInRegion(cc.NodeId) {
		// 更改 peer 并写入 RegionState
		d.ctx.storeMeta.Lock()
		defer d.ctx.storeMeta.Unlock()

		region := d.Region()
		// 更新 RegionEpoch 的 ConfVer
		region.RegionEpoch.ConfVer++

		// 删除指定的 peer
		newPeers := make([]*metapb.Peer, 0, len(region.Peers)-1)
		for _, pr := range region.Peers {
			if pr.Id != cc.NodeId {
				newPeers = append(newPeers, pr)
			}
		}
		region.Peers = newPeers

		// 持久化修改后的 Region
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

		// 从 peer 缓存中删除
		d.removePeerCache(cc.NodeId)
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

// applyNormalEntry 应用具体的日志条目（不同类型的请求）到 kvDB
func (d *peerMsgHandler) applyNormalEntry(entry *pb.Entry, kvWB *engine_util.WriteBatch) error {
	// 反序列化cmdRequest
	var cmdRequest raft_cmdpb.RaftCmdRequest
	err := cmdRequest.Unmarshal(entry.Data)
	if err != nil {
		return err
	}

	// 处理admin请求
	if cmdRequest.AdminRequest != nil {
		adminRequest := cmdRequest.GetAdminRequest()
		switch adminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.applyCompactLog(entry, adminRequest, kvWB)
		case raft_cmdpb.AdminCmdType_InvalidAdmin:
			err = fmt.Errorf("invalid admin cmd Type: %v", adminRequest)
			log.Error(err)
		}
	}

	for _, request := range cmdRequest.GetRequests() {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			d.applyGetReq(entry, request)
		case raft_cmdpb.CmdType_Put:
			d.applyPutReq(entry, request, kvWB)
		case raft_cmdpb.CmdType_Delete:
			d.applyDeleteReq(entry, request, kvWB)
		case raft_cmdpb.CmdType_Snap:
			d.applySnapReq(entry)
		case raft_cmdpb.CmdType_Invalid:
			err = fmt.Errorf("invalid normal cmd type: %v", request)
			log.Error(err)
		}
	}

	return err
}

// applyCompactLog 执行CompactLog类型的请求的具体内容，然后通过回调返回
func (d *peerMsgHandler) applyCompactLog(entry *pb.Entry, req *raft_cmdpb.AdminRequest, wb *engine_util.WriteBatch) {
	CompactIndex := req.GetCompactLog().CompactIndex

	if CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = CompactIndex
		d.peerStorage.applyState.TruncatedState.Term = req.GetCompactLog().CompactTerm
		err := wb.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
			return
		}
		d.ScheduleCompactLog(CompactIndex)
	}

	// 构造CompactLog类型的AdminResponse
	adminResp := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// applyGetReq 执行Get请求的具体内容，然后通过回调返回
func (d *peerMsgHandler) applyGetReq(entry *pb.Entry, req *raft_cmdpb.Request) {
	// 执行Get操作
	value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
	if err != nil {
		log.Errorf("Failed to get value: %v", err)
		return
	}

	// 构造Get类型的response
	resp := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Get,
		Get: &raft_cmdpb.GetResponse{
			Value: value,
		},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resp,
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// applyPutReq 执行Put请求的具体内容，然后通过回调返回
func (d *peerMsgHandler) applyPutReq(entry *pb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	// 执行Put操作
	kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	// 构造Put类型的response
	resp := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Put,
		Put:     &raft_cmdpb.PutResponse{},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resp,
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// applyDeleteReq 执行Delete请求的具体内容，然后通过回调返回
func (d *peerMsgHandler) applyDeleteReq(entry *pb.Entry, req *raft_cmdpb.Request, kvWB *engine_util.WriteBatch) {
	// 执行Delete操作
	kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB.Reset()

	// 构造Delete类型的response
	resp := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Delete,
		Delete:  &raft_cmdpb.DeleteResponse{},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resp,
	}
	d.checkValidAndCallback(cmdResp, entry)
}

// applySnapReq 执行scan请求的具体内容，然后通过回调返回
func (d *peerMsgHandler) applySnapReq(entry *pb.Entry) {
	// 构造Snap（Scan）类型的response
	resp := []*raft_cmdpb.Response{{
		CmdType: raft_cmdpb.CmdType_Snap,
		Snap: &raft_cmdpb.SnapResponse{
			Region: d.Region(),
		},
	}}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resp,
	}
	d.checkValidAndCallback(cmdResp, entry, d.peerStorage.Engines.Kv.NewTransaction(false))
}

func (d *peerMsgHandler) checkValidAndCallback(resp *raft_cmdpb.RaftCmdResponse, entry *pb.Entry, snapTxn ...*badger.Txn) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]

		if entry.Term < proposal.term {
			return
		}

		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Term == proposal.term && entry.Index < proposal.index {
			return
		}

		if entry.Term == proposal.term && entry.Index > proposal.index {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if entry.Index == proposal.index && entry.Term == proposal.term {
			if snapTxn != nil {
				proposal.cb.Txn = snapTxn[0]
			}
			proposal.cb.Done(resp)
			d.proposals = d.proposals[1:]
			return
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) checkReqValid(request *raft_cmdpb.Request) (bool, error) {
	var key []byte
	switch request.CmdType {
	case raft_cmdpb.CmdType_Invalid:
		return false, fmt.Errorf("invalid CmdType")
	case raft_cmdpb.CmdType_Get:
		key = request.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = request.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = request.Delete.Key
	case raft_cmdpb.CmdType_Snap:
		return true, nil
	}

	// 检查关键字是否在 Region 中
	err := util.CheckKeyInRegion(key, d.Region())
	if err != nil {
		return false, err
	}

	return true, err
}

// proposeToRaftGroup 将请求转化为字节数组然后创建新的propose交给raft层
func (d *peerMsgHandler) proposeToRaftGroup(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	// 将 Raft 命令请求序列化为字节数组
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("failed to transfer msg into byte array: %v", err) // 序列化失败，记录错误
		return err
	}

	// 创建一个新的提议对象，并添加到提议列表中
	proposal := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	d.proposals = append(d.proposals, proposal)

	// 向 Raft 集群提交提议
	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Errorf("propose failed! %v", err) // 提议失败，记录错误
		return err
	}

	return nil
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	for _, req := range msg.Requests {
		// 处理Normal类型的请求
		// 初步检查请求是否合法
		valid, err := d.checkReqValid(req)
		if !valid {
			cb.Done(ErrResp(err))
			continue
		}

		// 请求propose到raft层
		err = d.proposeToRaftGroup(msg, cb)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}

	if msg.AdminRequest != nil {
		// 处理Admin类型的请求
		adminReq := msg.GetAdminRequest()
		switch adminReq.CmdType {
		case raft_cmdpb.AdminCmdType_InvalidAdmin:
			cb.Done(ErrResp(fmt.Errorf("invalid adminCmdType")))
		case raft_cmdpb.AdminCmdType_ChangePeer:
			d.handleChangePeerReq(msg, adminReq, cb)
		case raft_cmdpb.AdminCmdType_CompactLog:
			// TODO 2C encountered a bug
			// d.handleCompactLogReq(msg, cb)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.handleTransferLeaderReq(adminReq, cb)
		case raft_cmdpb.AdminCmdType_Split:
			// TODO 处理分裂结点的请求
		}
	}
}

// handleChangePeerReq 处理AdminCmdType_ChangePeer类型的请求，调用 ProposeConfChange 进行配置更改提议
func (d *peerMsgHandler) handleChangePeerReq(msg *raft_cmdpb.RaftCmdRequest, adminReq *raft_cmdpb.AdminRequest, cb *message.Callback) {
	data, err := msg.Marshal()
	if err != nil {
		// 序列化失败，返回错误
		log.Errorf("failed to transfer msg into byte array: %v", err)
		cb.Done(ErrResp(err))
		return
	}
	if adminReq.ChangePeer == nil {
		// ChangePeer 字段为空，返回错误
		err = fmt.Errorf("invalid AdminCmdType_ChangePeer request, ChangePeer is required")
		log.Error(err)
		cb.Done(ErrResp(err))
		return
	}
	confChange := pb.ConfChange{
		ChangeType: adminReq.ChangePeer.ChangeType,
		NodeId:     adminReq.ChangePeer.Peer.Id,
		Context:    data,
	}
	proposal := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	d.proposals = append(d.proposals, proposal)

	// 调用 ProposeConfChange 进行配置更改提议
	err = d.RaftGroup.ProposeConfChange(confChange)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
}

func (d *peerMsgHandler) handleCompactLogReq(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// 请求propose到raft层
	err := d.proposeToRaftGroup(msg, cb)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
}

// handleTransferLeaderReq 处理AdminCmdType_TransferLeader类型的领导变更请求，不用提交proposal，调用TransferLeader直接Step转发
func (d *peerMsgHandler) handleTransferLeaderReq(adminReq *raft_cmdpb.AdminRequest, cb *message.Callback) {
	// 调用 d.RaftGroup.TransferLeader() 发送 MsgTransferLeader 到 raft 层
	d.RaftGroup.TransferLeader(adminReq.TransferLeader.Peer.Id)
	// 构造TransferLeader类型的AdminResponse
	adminResp := &raft_cmdpb.AdminResponse{
		TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
	}
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}
	cb.Done(resp)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
