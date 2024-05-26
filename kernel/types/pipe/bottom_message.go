package pipe

import (
	"RaftDB/kernel/raft_log"
	"fmt"
)

const (
	NodeReply MessageType = iota
	Store
	FromNode
	FromClient
	ClientReply
	NIL
)

type BottomMessage struct {
	Type MessageType // 命令种类
	Body MessageBody //消息正文
}

type BodyType int

type MessageType int

var messageTypes []string = []string{
	"NodeReply",
	"store",
	"FromNode",
	"FromClient",
	"ClientReply",
}

const (
	Heartbeat BodyType = iota
	AppendRaftLog
	Commit
	AppendRaftLogReply
	Vote
	VoteReply
	PreVote
	PreVoteReply
	FileAppend
	FileTruncate
)

var msgTypes []string = []string{
	"Heartbeat",
	"AppendRaftLog",
	"Commit",
	"AppendRaftLogReply",
	"Vote",
	"VoteReply",
	"PreVote",
	"PreVoteReply",
	"FileAppend",
	"FileTruncate",
}

type MessageBody struct {
	Type             BodyType         `json:"type"`                // 消息类型
	From             int              `json:"from"`                // 消息来源
	To               []int            `json:"to"`                  // 消息去向
	Term             int              `json:"term"`                // 消息发送方的任期/客户端设置的超时微秒数
	Agree            bool             `json:"agree"`               // 对消息的回复/客户端读写消息区分/携带的数据是日志还是元数据
	LastLogKey       raft_log.RaftKey `json:"last_log_key"`        // 要commit的消息/要请求的消息/存储日志的最后一条消息
	SecondLastLogKey raft_log.RaftKey `json:"second_last_log_key"` // 要请求消息的前一条消息/存储日志的第一条消息
	Content          string           `json:"content"`             // 消息正文/客户端请求命令/新日志
	Others           interface{}      `json:"-"`                   // 其它
}

func (o *BottomMessage) ToString() string {
	return fmt.Sprintf("{\n MessageType: %s\n MessageBody:{\n"+
		"  Type: %s\n  From: %d\n  To: %v\n  Term: %d\n  Agree: %v\n  LastLogKey: %v\n  SecondLastLogKey: %v\n  V: %s\n }\n"+
		"}",
		messageTypes[o.Type], msgTypes[o.Body.Type], o.Body.From, o.Body.To, o.Body.Term,
		o.Body.Agree, o.Body.LastLogKey, o.Body.SecondLastLogKey, o.Body.Content)
}

func (m *MessageBody) ToString() string {
	return fmt.Sprintf("{\n Type: %s\n From: %d\n To: %v\n Term: %d\n Agree: %v\n LastLogKey: %v\n SecondLastLogKey: %v\n V: %s\n}",
		msgTypes[m.Type], m.From, m.To, m.Term, m.Agree, m.LastLogKey, m.SecondLastLogKey, m.Content)
}
