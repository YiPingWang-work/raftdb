package logic

import (
	"RaftDB/kernel/meta"
	"RaftDB/kernel/pipe"
	"RaftDB/kernel/raft_log"
	"RaftDB/log_plus"
	"encoding/json"
	"errors"
	"time"
)

type Me struct {
	meta                    *meta.Meta               // 元数据信息指针，用于状态变更，只允许Logic层修改元数据信息
	members                 []int                    // 维护的成员数量
	quorum                  int                      // 最小选举人数
	role                    Role                     // 当前角色
	timer                   *time.Timer              // 计时器
	fromBottomChan          <-chan pipe.Order        // 接收bottom消息的管道
	toBottomChan            chan<- pipe.Order        // 发送消息给bottom的管道
	fromCrownChan           <-chan pipe.Something    // 上层接口
	toCrownChan             chan<- pipe.Something    // 上层接口
	mapKeyClient            map[raft_log.RaftKey]int // 存储写任务的key对应的客户端发送过来的信息
	syncFailedChan          chan int                 // 同步失败，follower返回，me接受并提示失败
	raftLogSet              *raft_log.RaftLogSet     // 日志指针
	leaderHeartbeat         time.Duration            // leader心跳间隔
	followerTimeout         time.Duration            // follower超时时间
	candidatePreVoteTimeout time.Duration            // candidate预选举超时
	candidateVoteTimeout    time.Duration            // candidate选举超时
}

/*
Role接口定义了处理各种消息的函数，Follower、Leader、Candidate角色类实现Role接口（状态机模型）。
在Me中会保存一个Role接口role，这个role代表自己的角色，me直接通过调用role的接口函数间接调用各个角色实现的函数，而不需要判断自己的角色是什么。
*/

type Role interface {
	init(me *Me) error
	processHeartbeat(msg *pipe.Message, me *Me) error
	processAppend(msg *pipe.Message, me *Me) error
	processAppendReply(msg *pipe.Message, me *Me) error
	processCommit(msg *pipe.Message, me *Me) error
	processVote(msg *pipe.Message, me *Me) error
	processVoteReply(msg *pipe.Message, me *Me) error
	processPreVote(msg *pipe.Message, me *Me) error
	processPreVoteReply(msg *pipe.Message, me *Me) error
	processExpansion(msg *pipe.Message, me *Me) error      // 节点变更，未实现
	processExpansionReply(msg *pipe.Message, me *Me) error // 节点变更回复，未实现
	processFromClient(msg *pipe.Message, me *Me) error
	processClientSync(msg *pipe.Message, me *Me) error
	processTimeout(me *Me) error
	ToString() string
}

/*
初始化，设置元数据信息，设置日志信息，设置超时时间，设置通讯管道（包括通向bottom端的和通向crown端的）
*/

func (m *Me) Init(meta *meta.Meta, raftLogSet *raft_log.RaftLogSet,
	fromBottomChan <-chan pipe.Order, toBottomChan chan<- pipe.Order,
	fromCrownChan <-chan pipe.Something, toCrownChan chan<- pipe.Something) {
	m.meta, m.raftLogSet = meta, raftLogSet
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.fromCrownChan, m.toCrownChan = fromCrownChan, toCrownChan
	m.syncFailedChan = make(chan int, 10000)
	m.members, m.quorum = make([]int, meta.Num), meta.Num/2
	m.mapKeyClient = map[raft_log.RaftKey]int{}
	m.timer = time.NewTimer(m.followerTimeout)
	for i := 0; i < meta.Num; i++ {
		m.members[i] = i
	}
	m.leaderHeartbeat = time.Duration(meta.LeaderHeartbeat) * time.Millisecond
	m.followerTimeout = time.Duration(meta.FollowerTimeout) * time.Millisecond
	m.candidateVoteTimeout = time.Duration(meta.CandidateVoteTimeout) * time.Millisecond
	m.candidatePreVoteTimeout = time.Duration(meta.CandidatePreVoteTimeout) * time.Millisecond
	if err := m.switchToFollower(m.meta.Term, false, &pipe.Message{}); err != nil {
		log_plus.Println(log_plus.DEBUG_LOGIC, err)
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
		case order, opened := <-m.fromBottomChan:
			if !opened {
				panic("bottom chan is closed")
			}
			if order.Type == pipe.FromNode {
				if err := m.processFromNode(&order.Msg); err != nil {
					log_plus.Println(log_plus.DEBUG_LOGIC, err)
				}
			}
			if order.Type == pipe.FromClient {
				if err := m.role.processFromClient(&order.Msg, m); err != nil {
					log_plus.Println(log_plus.DEBUG_LOGIC, err)
					m.toBottomChan <- pipe.Order{Type: pipe.ClientReply,
						Msg: pipe.Message{From: order.Msg.From, Content: "logic refuses to operate"}}
				}
			}
		case <-m.timer.C:
			if err := m.role.processTimeout(m); err != nil {
				log_plus.Println(log_plus.DEBUG_LOGIC, err)
			}
		case sth, opened := <-m.fromCrownChan:
			if !opened {
				panic("crown chan is closed")
			}
			m.toBottomChan <- pipe.Order{Type: pipe.ClientReply,
				Msg: pipe.Message{From: sth.ClientId, Content: sth.Content}}
		case id, opened := <-m.syncFailedChan:
			if !opened {
				panic("syncFailed chan is closed")
			}
			m.toBottomChan <- pipe.Order{Type: pipe.ClientReply,
				Msg: pipe.Message{From: id, Content: "logic sync failed"}}
		}
	}
}

/*
processFromNode方法是处理OrderType为FromNode所有命令中msg的共同逻辑。
首先会进行消息Term判断，如果发现收到了一则比自己Term大的消息，会转成follower之后继续处理这个消息。
如果发现消息的Term比自己小，说明是一个过期的消息，不予处理。
之后会根据消息的Type分类处理。
*/

func (m *Me) processFromNode(msg *pipe.Message) error {
	if m.meta.Term > msg.Term || m.meta.Id == msg.From {
		return nil
	} else if m.meta.Term < msg.Term {
		return m.switchToFollower(msg.Term, true, msg)
	}
	switch msg.Type {
	case pipe.Heartbeat:
		return m.role.processHeartbeat(msg, m)
	case pipe.AppendRaftLog:
		return m.role.processAppend(msg, m)
	case pipe.AppendRaftLogReply:
		return m.role.processAppendReply(msg, m)
	case pipe.Commit:
		return m.role.processCommit(msg, m)
	case pipe.Vote:
		return m.role.processVote(msg, m)
	case pipe.VoteReply:
		return m.role.processVoteReply(msg, m)
	case pipe.PreVote:
		return m.role.processPreVote(msg, m)
	case pipe.PreVoteReply:
		return m.role.processPreVoteReply(msg, m)
	default:
		return errors.New("error: illegal msg type")
	}
}

/*
切换为follower，如果还有余下的消息没处理按照follower逻辑处理这些消息。
*/

func (m *Me) switchToFollower(term int, has bool, msg *pipe.Message) error {
	log_plus.Printf(log_plus.DEBUG_LOGIC, "==== switch to follower, my term is %d, has remain msg to process: %v ====\n", term, has)
	if m.meta.Term < term {
		m.meta.Term = term
		if metaTmp, err := json.Marshal(*m.meta); err != nil {
			return err
		} else {
			m.toBottomChan <- pipe.Order{Type: pipe.Store, Msg: pipe.Message{Agree: true, Content: string(metaTmp)}}
		}
	}
	m.role = &follower
	if err := m.role.init(m); err != nil {
		return err
	}
	if has {
		return m.processFromNode(msg)
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
