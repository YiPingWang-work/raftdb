package logic

import (
	"RaftDB/kernel/raft_log"
	"RaftDB/kernel/types/pipe"
	"RaftDB/log_plus"
	"encoding/json"
	"errors"
)

var follower Follower

type Follower struct {
	voted int
}

func (f *Follower) init(me *Me) error {
	me.timer.Reset(me.followerTimeout)
	f.voted = -1
	return nil
}

/*
收到leader的心跳，重制定时器。
如果发现自己的LastLogKey比心跳中携带的leader的LastLogKey小，那么转到processLogAppend触发日志缺失处理。
这里如果自己的日志比leader大，不做处理，等到新消息到来时再删除。
*/

func (f *Follower) processHeartbeat(body *pipe.MessageBody, me *Me) error {
	me.timer.Reset(me.followerTimeout)
	log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: leader %d's heartbeat\n", body.From)
	if me.raftLogSet.GetLast().Less(body.LastLogKey) {
		log_plus.Println(log_plus.DEBUG_FOLLOWER, "Follower: my raftLogSet are not complete")
		return f.processAppend(body, me)
	}
	return nil
}

/*
追加日志申请，如果自己已提交的日志大于等于请求追加的日志，直接返回。
到这里自己的已提交的日志LogKey一定小于等于请求中的SecondLastLogKey且小于请求中的LastLogKey
如果自己日志对LastLogKey大于请求的SecondLastKey，那么删除日志直到自己的LastLogKey小于等于请求的SecondLastLogKey。
到这里，自己的最后一条日志一定比secondLastLogKey小或者等于它
之后如果日志中有信息（不是从Heartbeat得到的）且自己此时的LastLogKey就是请求的SecondLastLogKey，直接添加日志。并回复成功。
否则拒绝本次申请，同时在回复的SecondLastLogKey字段中给出自己的LastLogKey。
所有经过此函数发出的不同意的回复必须保证SecondLastLogKey小于发送过来的LastLogKey。
*/

func (f *Follower) processAppend(body *pipe.MessageBody, me *Me) error {
	reply := pipe.MessageBody{
		Type:       pipe.AppendRaftLogReply,
		From:       me.meta.Id,
		To:         []int{body.From},
		Term:       me.meta.Term,
		LastLogKey: body.LastLogKey,
	}
	if !me.raftLogSet.GetCommitted().Less(body.LastLogKey) || me.raftLogSet.Exist(body.LastLogKey) {
		return nil
	}
	if me.raftLogSet.GetLast().Greater(body.SecondLastLogKey) {
		if raftLogs, err := me.raftLogSet.Remove(body.SecondLastLogKey); err != nil {
			panic("remove committed log")
		} else {
			me.toBottomChan <- pipe.BottomMessage{Type: pipe.Store, Body: pipe.MessageBody{
				Type:   pipe.FileTruncate,
				Agree:  false,
				Others: raftLogs,
			}}
			for _, v := range raftLogs {
				if id, has := me.mapKeyClient[v.K]; has {
					me.syncFailedChan <- id
					delete(me.mapKeyClient, v.K)
				}
			}
		}
		log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: receive a less log %v from %d, remove raftLogSet until last log is %v\n",
			body.LastLogKey, body.From, me.raftLogSet.GetLast())
	}
	if me.raftLogSet.GetLast().Equals(body.SecondLastLogKey) && body.Type == pipe.AppendRaftLog {
		reply.Agree = true
		me.raftLogSet.Append(raft_log.RaftLog{K: body.LastLogKey, V: body.Content})
		me.toBottomChan <- pipe.BottomMessage{Type: pipe.Store, Body: pipe.MessageBody{
			Type:       pipe.FileAppend,
			Agree:      false,
			LastLogKey: body.LastLogKey,
		}}
		log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: accept %d's request %v\n", body.From, body.LastLogKey)
	} else {
		reply.Agree, reply.SecondLastLogKey = false, me.raftLogSet.GetLast()
		log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: refuse %d's request %v, my last log is %v\n", body.From, body.LastLogKey, me.raftLogSet.GetLast())
	}
	me.toBottomChan <- pipe.BottomMessage{Type: pipe.NodeReply, Body: reply}
	me.timer.Reset(me.followerTimeout)
	return nil
}

func (f *Follower) processAppendReply(*pipe.MessageBody, *Me) error {
	return nil
}

/*
follower将提交所有小于等于提交请求key的log。
*/

func (f *Follower) processCommit(body *pipe.MessageBody, me *Me) error {
	if !me.raftLogSet.GetCommitted().Less(body.LastLogKey) {
		return nil
	}
	previousCommitted := me.raftLogSet.Commit(body.LastLogKey)
	if me.raftLogSet.GetCommitted().Equals(previousCommitted) {
		return nil
	}
	me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = me.raftLogSet.GetCommitted().Term, me.raftLogSet.GetCommitted().Index
	if metaTmp, err := json.Marshal(*me.meta); err != nil {
		return err
	} else {
		me.toBottomChan <- pipe.BottomMessage{Type: pipe.Store, Body: pipe.MessageBody{Agree: true, Content: string(metaTmp)}}
	}
	from, _ := me.raftLogSet.GetNext(previousCommitted)
	to := me.raftLogSet.GetCommitted()
	me.timer.Reset(me.followerTimeout)
	for _, v := range me.raftLogSet.GetLogsByRange(from, to) {
		if id, has := me.mapKeyClient[v.K]; has {
			me.toCrownChan <- pipe.CrownMessage{ClientId: id, NeedReply: true, Content: v.V}
			delete(me.mapKeyClient, v.K)
		} else {
			me.toCrownChan <- pipe.CrownMessage{NeedReply: false, Content: v.V}
		}
	}
	log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: commit raftLogSet whose key from %v to %v\n",
		from, to)
	return nil
}

/*
处理投票回复，如果follower在本轮（Term）已经投过票了或者自己的LastLogKey比Candidate大，那么他将拒绝，否则同意。
*/

func (f *Follower) processVote(body *pipe.MessageBody, me *Me) error {
	reply := pipe.MessageBody{
		Type: pipe.VoteReply,
		From: me.meta.Id,
		To:   []int{body.From},
		Term: me.meta.Term,
	}
	if f.voted != -1 && f.voted != body.From || me.raftLogSet.GetLast().Greater(body.LastLogKey) {
		reply.Agree, reply.SecondLastLogKey = false, me.raftLogSet.GetLast()
		log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: refuse %d's vote, because vote: %d, myLastKey: %v, yourLastKey: %v\n",
			body.From, f.voted, reply.SecondLastLogKey, body.LastLogKey)
	} else {
		f.voted = body.From
		reply.Agree = true
		log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: agreeMap %d's vote\n", body.From)
	}
	me.toBottomChan <- pipe.BottomMessage{
		Type: pipe.NodeReply,
		Body: reply,
	}
	return nil
}

func (f *Follower) processVoteReply(*pipe.MessageBody, *Me) error {
	return nil
}

func (f *Follower) processPreVote(body *pipe.MessageBody, me *Me) error {
	me.toBottomChan <- pipe.BottomMessage{Type: pipe.NodeReply, Body: pipe.MessageBody{
		Type: pipe.PreVoteReply,
		From: me.meta.Id,
		To:   []int{body.From},
		Term: me.meta.Term,
	}}
	return nil
}

func (f *Follower) processPreVoteReply(*pipe.MessageBody, *Me) error {
	return nil
}

func (f *Follower) processFromClient(body *pipe.MessageBody, me *Me) error {
	log_plus.Printf(log_plus.DEBUG_FOLLOWER, "Follower: a body from client: %v\n", body)
	if body.Agree {
		return errors.New("warning: follower refuses to sync")
	}
	me.toCrownChan <- pipe.CrownMessage{ClientId: body.From, NeedReply: true, Content: body.Content}
	return nil
}

func (f *Follower) processClientSync(*pipe.MessageBody, *Me) error {
	return errors.New("warning: follower can not do sync")
}

func (f *Follower) processTimeout(me *Me) error {
	log_plus.Println(log_plus.DEBUG_FOLLOWER, "Follower: timeout")
	return me.switchToCandidate()
}

func (f *Follower) processExpansion(*pipe.MessageBody, *Me) error {
	return nil
}

func (f *Follower) processExpansionReply(*pipe.MessageBody, *Me) error {
	return nil
}

func (f *Follower) ToString() string {
	return "==== FOLLOWER ====\n==== FOLLOWER ===="
}
