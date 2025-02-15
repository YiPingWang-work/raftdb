package bottom

import (
	"RaftDB/kernel/types/pipe"
)

type Communicate struct {
	cable Cable
	dns   []string
	addr  string
}

/*
信道接口，实现的通信信道需要实现初始化、回应客户端信息，回复服务节点信息，模拟网络延迟的功能。
*/

type Cable interface {
	Init(cableParam interface{}, alwaysIp []string) error
	ReplyNode(addr string, body interface{}) error
	Listen(addr string) error
	ReplyClient(body interface{}) error
	ChangeNetworkDelay(delay int, random bool)
}

/*
通讯系统初始化，实例化自己的信道类型，保存本机的addr和所有通讯节点的映射信息；将上层传过来的信道实例初始化。失败报错。
*/

func (c *Communicate) init(cable Cable, addr string, dns []string, cableParam interface{}) error {
	c.cable, c.addr, c.dns = cable, addr, dns
	return c.cable.Init(cableParam, dns)
}

func (c *Communicate) replyNode(body pipe.MessageBody) error {
	for _, v := range body.To {
		if addr := c.dns[v]; addr != c.addr {
			go func() {
				_ = c.cable.ReplyNode(addr, body)
			}()
		}
	}
	return nil
}

/*
开启监听，监听是另一个协程。要求在监听初始化的时候失败会报错，其余情况只提示连接失败。
*/

func (c *Communicate) listen() error {
	return c.cable.Listen(c.addr)
}

/*
回复客户节点，回复不了报错。
*/

func (c *Communicate) ReplyClient(body pipe.MessageBody) error {
	return c.cable.ReplyClient(body)
}

/*
回复服务节点，回复不了报错。
*/

func (c *Communicate) ChangeNetworkDelay(delay int, random bool) {
	c.cable.ChangeNetworkDelay(delay, random)
}
