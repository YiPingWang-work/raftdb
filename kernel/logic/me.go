package logic

import (
	"RaftDB/kernel/raft_log"
	"RaftDB/kernel/types/meta"
	"RaftDB/kernel/types/pipe"
	"RaftDB/log_plus"
	"encoding/json"
	"errors"
	"time"
)

type Me struct {
	meta                    *meta.Meta                // 元数据信息指针，用于状态变更，只允许Logic层修改元数据信息
	members                 []int                     // 维护的成员数量
	quorum                  int                       // 最小选举人数
	role                    Role                      // 当前角色
	timer                   *time.Timer               // 计时器
	fromBottomChan          <-chan pipe.BottomMessage // 接收bottom消息的管道
	toBottomChan            chan<- pipe.BottomMessage // 发送消息给bottom的管道
	fromCrownChan           <-chan pipe.CrownMessage  // 上层接口
	toCrownChan             chan<- pipe.CrownMessage  // 上层接口
	mapKeyClient            map[raft_log.RaftKey]int  // 存储写任务的key对应的客户端发送过来的信息
	syncFailedChan          chan int                  // 同步失败，follower返回，me接受并提示失败
	raftLogSet              *raft_log.RaftLogSet      // 日志指针
	leaderHeartbeat         time.Duration             // leader心跳间隔
	followerTimeout         time.Duration             // follower超时时间
	candidatePreVoteTimeout time.Duration             // candidate预选举超时
	candidateVoteTimeout    time.Duration             // candidate选举超时
}

/*
Role接口定义了处理各种消息的函数，Follower、Leader、Candidate角色类实现Role接口（状态机模型）。
在Me中会保存一个Role接口role，这个role代表自己的角色，me直接通过调用role的接口函数间接调用各个角色实现的函数，而不需要判断自己的角色是什么。
*/

type Role interface {
	init(me *Me) error
	processHeartbeat(body *pipe.MessageBody, me *Me) error
	processAppend(body *pipe.MessageBody, me *Me) error
	processAppendReply(body *pipe.MessageBody, me *Me) error
	processCommit(body *pipe.MessageBody, me *Me) error
	processVote(body *pipe.MessageBody, me *Me) error
	processVoteReply(body *pipe.MessageBody, me *Me) error
	processPreVote(body *pipe.MessageBody, me *Me) error
	processPreVoteReply(body *pipe.MessageBody, me *Me) error
	processExpansion(body *pipe.MessageBody, me *Me) error      // 节点变更，未实现
	processExpansionReply(body *pipe.MessageBody, me *Me) error // 节点变更回复，未实现
	processFromClient(body *pipe.MessageBody, me *Me) error
	processClientSync(body *pipe.MessageBody, me *Me) error
	processTimeout(me *Me) error
	ToString() string
}

/*
初始化，设置元数据信息，设置日志信息，设置超时时间，设置通讯管道（包括通向bottom端的和通向crown端的）
*/

func (m *Me) Init(meta *meta.Meta, raftLogSet *raft_log.RaftLogSet,
	fromBottomChan <-chan pipe.BottomMessage, toBottomChan chan<- pipe.BottomMessage,
	fromCrownChan <-chan pipe.CrownMessage, toCrownChan chan<- pipe.CrownMessage) {
	m.meta, m.raftLogSet = meta, raftLogSet
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.fromCrownChan, m.toCrownChan = fromCrownChan, toCrownChan
	m.syncFailedChan = make(chan int, 10000)
	m.members, m.quorum = make([]int, meta.Num), meta.Num/2
	m.mapKeyClient = map[raft_log.RaftKey]int{}
	for i := 0; i < meta.Num; i++ {
		m.members[i] = i
	}
	m.leaderHeartbeat = time.Duration(meta.LeaderHeartbeat) * time.Millisecond
	m.followerTimeout = time.Duration(meta.FollowerTimeout) * time.Millisecond
	m.candidateVoteTimeout = time.Duration(meta.CandidateVoteTimeout) * time.Millisecond
	m.candidatePreVoteTimeout = time.Duration(meta.CandidatePreVoteTimeout) * time.Millisecond
	m.timer = time.NewTimer(m.followerTimeout)
	if err := m.switchToFollower(m.meta.Term, false, &pipe.MessageBody{}); err != nil {
		log_plus.Println(log_plus.DEBUG_LOGIC, "ERROR", err)
	}
}

/*
Logic层的主体函数，不断获取来自bottom的消息和定时器超时的消息，进行相应处理。
收到服务节点的消息后转到process函数。
收到客户端消息，说明自己是leader，直接调用processClient函数。
计时器到期后调用计时器到期处理函数。
在执行过程中发现通讯管道关闭，Panic返回。
*/

func (m *Me) Run() {
	for {
		select {
		case msg, opened := <-m.fromBottomChan:
			if !opened {
				panic("bottom chan is closed")
			}
			if msg.Type == pipe.FromNode {
				if err := m.processFromNode(&msg.Body); err != nil {
					log_plus.Println(log_plus.DEBUG_LOGIC, "ERROR", err)
				}
			}
			if msg.Type == pipe.FromClient {
				if err := m.role.processFromClient(&msg.Body, m); err != nil {
					log_plus.Println(log_plus.DEBUG_LOGIC, "ERROR", err)
					m.toBottomChan <- pipe.BottomMessage{Type: pipe.ClientReply,
						Body: pipe.MessageBody{From: msg.Body.From, Content: "logic refuses to operate"}}
				}
			}
		case <-m.timer.C:
			if err := m.role.processTimeout(m); err != nil {
				log_plus.Println(log_plus.DEBUG_LOGIC, "ERROR", err)
			}
		case sth, opened := <-m.fromCrownChan:
			if !opened {
				panic("crown chan is closed")
			}
			m.toBottomChan <- pipe.BottomMessage{Type: pipe.ClientReply,
				Body: pipe.MessageBody{From: sth.ClientId, Content: sth.Content}}
		case id, opened := <-m.syncFailedChan:
			if !opened {
				panic("syncFailed chan is closed")
			}
			m.toBottomChan <- pipe.BottomMessage{Type: pipe.ClientReply,
				Body: pipe.MessageBody{From: id, Content: "logic sync failed"}}
		}
	}
}

/*
processFromNode方法是处理OrderType为FromNode所有命令中msg的共同逻辑。
首先会进行消息Term判断，如果发现收到了一则比自己Term大的消息，会转成follower之后继续处理这个消息。
如果发现消息的Term比自己小，说明是一个过期的消息，不予处理。
之后会根据消息的Type分类处理。
*/

func (m *Me) processFromNode(body *pipe.MessageBody) error {
	if m.meta.Term > body.Term || m.meta.Id == body.From {
		return nil
	} else if m.meta.Term < body.Term {
		return m.switchToFollower(body.Term, true, body)
	}
	switch body.Type {
	case pipe.Heartbeat:
		return m.role.processHeartbeat(body, m)
	case pipe.AppendRaftLog:
		return m.role.processAppend(body, m)
	case pipe.AppendRaftLogReply:
		return m.role.processAppendReply(body, m)
	case pipe.Commit:
		return m.role.processCommit(body, m)
	case pipe.Vote:
		return m.role.processVote(body, m)
	case pipe.VoteReply:
		return m.role.processVoteReply(body, m)
	case pipe.PreVote:
		return m.role.processPreVote(body, m)
	case pipe.PreVoteReply:
		return m.role.processPreVoteReply(body, m)
	default:
		return errors.New("error: illegal body type")
	}
}

/*
切换为follower，如果还有余下的消息没处理按照follower逻辑处理这些消息。
*/

func (m *Me) switchToFollower(term int, has bool, body *pipe.MessageBody) error {
	log_plus.Printf(log_plus.DEBUG_LOGIC, "==== switch to follower, my term is %d, has remain body to process: %v ====\n", term, has)
	if m.meta.Term < term {
		m.meta.Term = term
		if metaTmp, err := json.Marshal(*m.meta); err != nil {
			return err
		} else {
			m.toBottomChan <- pipe.BottomMessage{Type: pipe.Store, Body: pipe.MessageBody{Agree: true, Content: string(metaTmp)}}
		}
	}
	m.role = &follower
	if err := m.role.init(m); err != nil {
		return err
	}
	if has {
		return m.processFromNode(body)
	}
	return nil
}

/*
切换为leader。
*/

func (m *Me) switchToLeader() error {
	log_plus.Printf(log_plus.DEBUG_LOGIC, "==== switch to leader, my term is %d ====\n", m.meta.Term)
	m.role = &leader
	return m.role.init(m)
}

/*
切换为candidate。
*/

func (m *Me) switchToCandidate() error {
	log_plus.Printf(log_plus.DEBUG_LOGIC, "==== switch to candidate, my term is %d ====\n", m.meta.Term)
	m.role = &candidate
	return m.role.init(m)
}

func (m *Me) ToString() string {
	return m.meta.ToString() + "\n" + m.role.ToString()
}
