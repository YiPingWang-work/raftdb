package pipe

type Something struct {
	ClientId  int    // 客户端ID，crown禁止修改
	NeedReply bool   // 是否需要回复，crown禁止修改
	Agree     bool   // 是否执行成功，crown必须修改
	Content   string // 消息正文，crown接受信息并在这里给出回复，传入是命令、传出是结果
}
