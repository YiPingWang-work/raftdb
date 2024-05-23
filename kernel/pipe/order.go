package pipe

import (
	"RaftDB/kernel/raft_log"
	"fmt"
)

const (
	NodeReply OrderType = iota
	Store
	FromNode
	FromClient
	ClientReply
	NIL
)

type Order struct {
	Type OrderType // 命令种类
	Msg  Message   //消息正文
}

type MsgType int

type OrderType int

var orderTypes []string = []string{
	"NodeReply",
	"store",
	"FromNode",
	"FromClient",
	"ClientReply",
}

const (
	Heartbeat MsgType = iota
	AppendLog
	Commit
	AppendLogReply
	Vote
	VoteReply
	PreVote
	PreVoteReply
	Expansion
	ExpansionReply
)

var msgTypes []string = []string{
	"Heartbeat",
	"AppendLog",
	"Commit",
	"AppendLogReply",
	"Vote",
	"VoteReply",
	"PreVote",
	"PreVoteReply",
	"Expansion",
	"ExpansionReply",
}

type Message struct {
	Type             MsgType          `json:"type"`                // 消息类型
	From             int              `json:"from"`                // 消息来源
	To               []int            `json:"to"`                  // 消息去向
	Term             int              `json:"term"`                // 消息发送方的任期/客户端设置的超时微秒数
	Agree            bool             `json:"agree"`               // 对消息的回复/客户端读写消息区分/携带的数据是日志还是元数据
	LastLogKey       raft_log.RaftKey `json:"last_log_key"`        // 要commit的消息/要请求的消息/存储日志的最后一条消息
	SecondLastLogKey raft_log.RaftKey `json:"second_last_log_key"` // 要请求消息的前一条消息/存储日志的第一条消息
	Log              string           `json:"log"`                 // 消息正文/客户端请求命令/新日志
}

func (o *Order) ToString() string {
	return fmt.Sprintf("{\n OrderType: %s\n Message:{\n"+
		"  Type: %s\n  From: %d\n  To: %v\n  Term: %d\n  Agree: %v\n  LastLogKey: %v\n  SecondLastLogKey: %v\n  V: %s\n }\n"+
		"}",
		orderTypes[o.Type], msgTypes[o.Msg.Type], o.Msg.From, o.Msg.To, o.Msg.Term,
		o.Msg.Agree, o.Msg.LastLogKey, o.Msg.SecondLastLogKey, o.Msg.Log)
}

func (m *Message) ToString() string {
	return fmt.Sprintf("{\n Type: %s\n From: %d\n To: %v\n Term: %d\n Agree: %v\n LastLogKey: %v\n SecondLastLogKey: %v\n V: %s\n}",
		msgTypes[m.Type], m.From, m.To, m.Term, m.Agree, m.LastLogKey, m.SecondLastLogKey, m.Log)
}
