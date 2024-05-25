package logic

import (
	"RaftDB/kernel/pipe"
	"RaftDB/log_plus"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var candidate Candidate

type Candidate struct {
	agree map[int]bool
	state int // 0：预选举，1：预选举结束，第一次选举，2：选举结束，没有结果
}

func (c *Candidate) init(me *Me) error {
	c.agree = map[int]bool{}
	c.state = 0
	return c.processTimeout(me)
}

/*
在收到同级心跳等leader发出的请求时，说明集群中还有leader存在，立即转变成follower后再处理这些请求。
*/

func (c *Candidate) processHeartbeat(msg *pipe.Message, me *Me) error {
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processAppend(msg *pipe.Message, me *Me) error {
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processCommit(msg *pipe.Message, me *Me) error {
	return me.switchToFollower(msg.Term, true, msg)
}

func (c *Candidate) processAppendReply(*pipe.Message, *Me) error {
	return nil
}

/*
选举期间的candidate不会给同级的candidate选票
*/

func (c *Candidate) processVote(msg *pipe.Message, me *Me) error {
	me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: pipe.Message{
		Type:  pipe.VoteReply,
		From:  me.meta.Id,
		To:    []int{msg.From},
		Term:  me.meta.Term,
		Agree: false,
	}}
	log_plus.Printf(log_plus.DEBUG_CANDIDATE, "Candidate: refuse %d's vote\n", msg.From)
	return nil
}

/*
如果选票同意人数达到quorum，则candidate晋升为leader，如果反对人数达到quorum，则candidate降级为follower。
*/

func (c *Candidate) processVoteReply(msg *pipe.Message, me *Me) error {
	log_plus.Printf(log_plus.DEBUG_CANDIDATE, "Candidate: %d agree my vote: %v\n", msg.From, msg.Agree)
	agreeNum := 0
	disagreeNum := 0
	c.agree[msg.From] = msg.Agree
	if len(c.agree) >= me.quorum { // 统计同意的人数
		for _, v := range c.agree {
			if v {
				agreeNum++
			} else {
				disagreeNum++
			}
		}
		if agreeNum >= me.quorum {
			return me.switchToLeader()
		} else if disagreeNum >= me.quorum {
			return me.switchToFollower(msg.Term, true, msg)
		}
	}
	return nil
}

func (c *Candidate) processPreVote(msg *pipe.Message, me *Me) error {
	me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: pipe.Message{
		Type: pipe.PreVoteReply,
		From: me.meta.Id,
		To:   []int{msg.From},
		Term: me.meta.Term,
	}}
	return nil
}

/*
如果预选举回复数达到quorum，说明集群属于存活态，自己有机会称为leader。
随机一段时间后开始选举。
*/

func (c *Candidate) processPreVoteReply(msg *pipe.Message, me *Me) error {
	if c.state == 0 {
		c.agree[msg.From] = true
		if len(c.agree) >= me.quorum {
			c.agree = map[int]bool{}
			c.state = 1
			log_plus.Println(log_plus.DEBUG_CANDIDATE, "Candidate: begin vote after a random time")
			me.timer.Reset(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}
	return nil
}

func (c *Candidate) processFromClient(msg *pipe.Message, me *Me) error {
	log_plus.Printf(log_plus.DEBUG_CANDIDATE, "Candidate: a msg from client: %v\n", msg)
	if msg.Agree {
		return errors.New("warning: candidate refuses to sync")
	}
	me.toCrownChan <- pipe.Something{ClientId: msg.From, NeedReply: true, Content: msg.Content}
	return nil
}

func (c *Candidate) processClientSync(*pipe.Message, *Me) error {
	return errors.New("warning: candidate can not do sync")
}

/*
三个阶段时间到期：
如果处于预选举状态（0），说明此时集群不满足多数派存活，继续试探。
如果是预选举到选举的随机时间结束到期，则自己开始正式选举。
如果是正式选举到期，说明支持和反对的票都没到达quorum，考虑是否集群不够多数派，回到预选举阶段。
*/

func (c *Candidate) processTimeout(me *Me) error {
	log_plus.Printf(log_plus.DEBUG_CANDIDATE, "Candidate: timeout, state: %v\n", c.state)
	reply := pipe.Message{
		From:       me.meta.Id,
		To:         me.members,
		LastLogKey: me.raftLogSet.GetLast(),
	}
	if c.state == 0 {
		reply.Type = pipe.PreVote
	} else if c.state == 1 {
		me.meta.Term++
		c.state = 2
		if metaTmp, err := json.Marshal(*me.meta); err != nil {
			return err
		} else {
			me.toBottomChan <- pipe.Order{Type: pipe.Store, Msg: pipe.Message{Agree: true, Content: string(metaTmp)}}
		}
		reply.Type = pipe.Vote
		log_plus.Printf(log_plus.DEBUG_CANDIDATE, "Candidate: voting ... , my term is %d\n", me.meta.Term)
	} else {
		c.state = 0
		reply.Type = pipe.PreVote
	}
	reply.Term = me.meta.Term
	me.toBottomChan <- pipe.Order{Type: pipe.NodeReply, Msg: reply}
	me.timer.Reset(me.candidatePreVoteTimeout)
	return nil
}

func (c *Candidate) processExpansion(*pipe.Message, *Me) error {
	return nil
}

func (c *Candidate) processExpansionReply(*pipe.Message, *Me) error {
	return nil
}

func (c *Candidate) ToString() string {
	res := fmt.Sprintf("==== CANDIDATE ====\nstate: %v\nagreeMap:\n", c.state)
	for k, v := range c.agree {
		res += fmt.Sprintf("%d:%v ", k, v)
	}
	return res + "\n==== CANDIDATE ===="
}
