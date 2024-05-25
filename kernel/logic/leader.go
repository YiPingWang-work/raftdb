package logic

import (
	"RaftDB/kernel/pipe"
	"RaftDB/kernel/raft_log"
	"RaftDB/log_plus"
	"encoding/json"
	"errors"
	"fmt"
)

/*
如果发现当前集群出现两个及其以上的leader，Panic退出，因为会造成数据不一致。
*/

var leader Leader

type Leader struct {
	agreeMap map[raft_log.RaftKey]map[int]struct{} // 对于哪条记录，同意的follower集合和这个消息来自哪个client
	index    int                                   // 当前日志的index
}

/*
初始化Leader，每当进行角色切换到额时候，必须调用此方法。
*/

func (l *Leader) init(me *Me) error {
	l.agreeMap, l.index = map[raft_log.RaftKey]map[int]struct{}{}, 0
	return l.processTimeout(me)
}

func (l *Leader) processHeartbeat(*pipe.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processAppend(*pipe.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processAppendReply(msg *pipe.Message, me *Me) error {
	reply := pipe.Message{
		Type:       pipe.Commit,
		From:       me.meta.Id,
		To:         []int{},
		Term:       me.meta.Term,
		LastLogKey: msg.LastLogKey,
	}
	if me.raftLogSet.GetLast().Less(msg.LastLogKey) {
		/*
			如果follower回复的Key比自己的LastKey都大，错误。
		*/
		return errors.New("error: a follower has greater key")
	}
	if msg.Agree == true {
		if !me.raftLogSet.GetCommitted().Less(msg.LastLogKey) {
			/*
				如果回复的key自己已经提交，则不用参与计票，直接对其确认，发送给源follower。
			*/
			reply.To = []int{msg.From}
			log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: %d should commit my committed log %v\n", msg.From, msg.LastLogKey)
		} else {
			/*
				计票，如果发现票数已经达到quorum，同时回复的key的Term为当前任期，则提交该日志，包括：更新元数据、内存更新日志、持久化日志到磁盘（上一次提交的日志到本条日志）。
				回复客户端数据提交成功。
				同时广播，让各个follower提交该日志。
			*/
			if _, has := l.agreeMap[msg.LastLogKey]; has {
				l.agreeMap[msg.LastLogKey][msg.From] = struct{}{}
			} else {
				l.agreeMap[msg.LastLogKey] = map[int]struct{}{msg.From: {}}
			}
			if len(l.agreeMap[msg.LastLogKey]) >= me.quorum && me.meta.Term == msg.LastLogKey.Term {
				me.meta.CommittedKeyTerm, me.meta.CommittedKeyIndex = msg.LastLogKey.Term, msg.LastLogKey.Index
				if meta, err := json.Marshal(*me.meta); err != nil {
					return err
				} else {
					me.toBottomChan <- pipe.Order{Type: pipe.Store, Msg: pipe.Message{Agree: true, Content: string(meta)}}
				}
				from, _ := me.raftLogSet.GetNext(me.raftLogSet.Commit(msg.LastLogKey))
				to := msg.LastLogKey
				for _, v := range me.raftLogSet.GetLogsByRange(from, to) {
					// 成功同步了，需要将同步的信息进行执行
					if id, has := me.mapKeyClient[v.K]; has {
						me.toCrownChan <- pipe.Something{ClientId: id, NeedReply: true, Content: v.V}
						delete(me.mapKeyClient, v.K)
					}
					if _, has := l.agreeMap[v.K]; has {
						delete(l.agreeMap, v.K)
					}
				}
				reply.To = me.members
				me.timer.Reset(me.leaderHeartbeat)
				log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: quorum have agreed request %v, I will commit and boardcast it\n", msg.LastLogKey)
			}
		}
		/*
			如果这条日志不是leader最新的日志，则尝试发送这条日志的下一条给源follower
		*/
		nextKey, _ := me.raftLogSet.GetNext(msg.LastLogKey)
		if nextKey.Term != -1 {
			req, err := me.raftLogSet.GetVByK(nextKey)
			if err != nil {
				return err
			}
			me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: pipe.Message{
				Type:             pipe.AppendRaftLog,
				From:             reply.From,
				To:               []int{msg.From},
				Term:             reply.Term,
				LastLogKey:       nextKey,
				SecondLastLogKey: msg.LastLogKey,
				Content:          req,
			}}
			log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: %d accept my request %v, but %d's raftLogSet is not complete, send request %v\n",
				msg.From, msg.LastLogKey, msg.From, nextKey)
		}
	} else {
		/*
			如果不同意这条消息，发送follower回复的最新消息的下一条（follower的最新消息在msg.SecondLastLogKey中携带）
		*/
		var err error
		reply.SecondLastLogKey = msg.SecondLastLogKey
		reply.LastLogKey, err = me.raftLogSet.GetNext(reply.SecondLastLogKey)
		if err != nil { // 如果无法获得返回值的key，也就是客户端存在有差错的key，那么leader将尝试自己的上一个key
			reply.LastLogKey, _ = me.raftLogSet.GetPrevious(msg.LastLogKey)
			reply.SecondLastLogKey, _ = me.raftLogSet.GetPrevious(reply.LastLogKey)
		}
		reply.Type, reply.To = pipe.AppendRaftLog, []int{msg.From}
		if v, _err := me.raftLogSet.GetVByK(reply.LastLogKey); _err != nil {
			return _err
		} else {
			reply.Content = v
		}
		log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: %d refuse my request %v, his raftLogSet are not complete, which is %v, send request %v\n",
			msg.From, msg.LastLogKey, msg.SecondLastLogKey, reply.LastLogKey)
	}
	if len(reply.To) != 0 {
		me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: reply}
	}
	return nil
}

func (l *Leader) processCommit(*pipe.Message, *Me) error {
	panic("maybe two leaders")
}

func (l *Leader) processVote(_ *pipe.Message, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processVoteReply(*pipe.Message, *Me) error {
	return nil
}

func (l *Leader) processPreVote(_ *pipe.Message, me *Me) error {
	return l.processTimeout(me)
}

func (l *Leader) processPreVoteReply(*pipe.Message, *Me) error {
	return nil
}

func (l *Leader) processFromClient(msg *pipe.Message, me *Me) error {
	log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: a msg from client: %v\n", msg)
	if msg.Agree {
		if err := l.processClientSync(msg, me); err != nil {
			return err
		}
	} else {
		me.toCrownChan <- pipe.Something{ClientId: msg.From, NeedReply: true, Content: msg.Content}
	}
	return nil
}

/*
记录日志的时机是上层成功执行一次同步操作后返回给logic层，logic层开始同步的时刻。只要内存中记录了日志，那么这条日志一定是操作在本节点过的。
上层一定操作过了这条日志。
*/

func (l *Leader) processClientSync(msg *pipe.Message, me *Me) error {
	secondLastKey := me.raftLogSet.GetLast()
	lastLogKey := raft_log.RaftKey{Term: me.meta.Term, Index: l.index}
	me.mapKeyClient[lastLogKey] = msg.From
	me.raftLogSet.Append(raft_log.RaftLog{K: lastLogKey, V: msg.Content})
	l.agreeMap[lastLogKey] = map[int]struct{}{}
	me.toBottomChan <- pipe.Order{Type: pipe.Store, Msg: pipe.Message{
		Type:       pipe.FileAppend,
		Agree:      false,
		LastLogKey: lastLogKey,
	}}
	me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: pipe.Message{
		Type:             pipe.AppendRaftLog,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		Agree:            false,
		LastLogKey:       lastLogKey,
		SecondLastLogKey: secondLastKey,
		Content:          msg.Content,
	}}
	me.timer.Reset(me.leaderHeartbeat)
	l.index++
	log_plus.Printf(log_plus.DEBUG_LEADER, "Leader: reveive a client's request whose key: %v, log: %v, now I will broadcast it\n", lastLogKey, msg.Content)
	return nil
}

func (l *Leader) processTimeout(me *Me) error {
	me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: pipe.Message{
		Type:             pipe.Heartbeat,
		From:             me.meta.Id,
		To:               me.members,
		Term:             me.meta.Term,
		LastLogKey:       me.raftLogSet.GetLast(),
		SecondLastLogKey: me.raftLogSet.GetSecondLast(),
	}}
	me.timer.Reset(me.leaderHeartbeat)
	log_plus.Println(log_plus.DEBUG_LEADER, "Leader: timeout")
	return nil
}

func (l *Leader) processExpansion(*pipe.Message, *Me) error {
	return nil
}

func (l *Leader) processExpansionReply(*pipe.Message, *Me) error {
	return nil
}

func (l *Leader) ToString() string {
	res := fmt.Sprintf("==== LEADER ====\nindex: %d\nagreedReply:\n", l.index)
	for k, v := range l.agreeMap {
		s := fmt.Sprintf("	key: {%d %d} -> ", k.Term, k.Index)
		for k2 := range v {
			s += fmt.Sprintf("%d ", k2)
		}
		res += s + "\n"
	}
	return res + "====LEADER===="
}
