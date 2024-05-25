package RPC

import (
	"RaftDB/kernel/pipe"
	"RaftDB/log_plus"
	"errors"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

/*
网络实体，本阶段使用RPC进行节点间和节点与客户端之间的通讯
*/

/*
	type Cable interface {
		Init(cableParam interface{}) error
		ReplyNode(addr string, msg interface{}) error
		Listen(addr string) error
		ReplyClient(msg interface{}) error
		ChangeNetworkDelay(delay int, random bool)
	}
*/
/*
要求rpc将信息推送到Logic层的时候，必须为消息打上全局唯一的标识。
*/

type RPC struct {
	clientChans     sync.Map
	replyChan       chan<- pipe.Order
	delay           int
	random          bool
	num             atomic.Int32
	alwaysConnPools map[string]*sync.Pool
}

func (r *RPC) Init(replyChan interface{}, alwaysIp []string) error {
	if x, ok := replyChan.(chan pipe.Order); !ok {
		return errors.New("RPC: Init need a reply chan")
	} else {
		r.replyChan = x
	}
	r.clientChans = sync.Map{}
	r.ChangeNetworkDelay(0, false)
	if err := rpc.RegisterName("RPC", r); err != nil {
		return err
	}
	r.alwaysConnPools = map[string]*sync.Pool{}
	for _, v := range alwaysIp {
		ip := v
		r.alwaysConnPools[v] = &sync.Pool{
			New: func() interface{} {
				client, err := rpc.Dial("tcp", ip)
				if err != nil {
					return err
				} else {
					return client
				}
			},
		}
	}
	return nil
}

func (r *RPC) ReplyNode_old_version(addr string, msg interface{}) error {
	if x, ok := msg.(pipe.Message); !ok {
		return errors.New("RPC: ReplyNode need a order.Message")
	} else {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return err
		}
		defer client.Close()
		if r.disturb() {
			return nil
		}
		if err = client.Call("RPC.Push", x, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *RPC) ReplyNode(addr string, msg interface{}) error {
	if x, ok := msg.(pipe.Message); !ok {
		return errors.New("RPC: ReplyNode need a order.Message")
	} else {
		if pool, has := r.alwaysConnPools[addr]; has {
			if client, ok := pool.Get().(*rpc.Client); !ok {
				return errors.New("lose connect")
			} else {
				if r.disturb() {
					return nil
				}
				if err := client.Call("RPC.Push", x, nil); err != nil {
					return err
				}
				pool.Put(client)
			}
		} else {
			panic("error a new node ip")
		}
	}
	return nil
}

func (r *RPC) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		if conn, err := listener.Accept(); err != nil {
			log_plus.Println(log_plus.DEBUG_COMMUNICATE, err)
		} else {
			go rpc.ServeConn(conn)
		}
	}
}

func (r *RPC) ChangeNetworkDelay(delay int, random bool) {
	r.delay = delay
	r.random = random
}

func (r *RPC) ReplyClient(msg interface{}) error {
	if x, ok := msg.(pipe.Message); !ok {
		return errors.New("RPC: ReplyClient need a order.Message")
	} else {
		ch, ok := r.clientChans.Load(x.From)
		if ok {
			select {
			case ch.(chan pipe.Message) <- x:
			default:
			}
		}
	}
	return nil
}

func (r *RPC) Push(rec pipe.Message, _ *string) error {
	if r.disturb() {
		return nil
	}
	r.replyChan <- pipe.Order{Type: pipe.FromNode, Msg: rec}
	return nil
}

func (r *RPC) Write(rec pipe.Message, rep *string) error {
	rec.From = int(r.num.Add(1))
	ch := make(chan pipe.Message, 1)
	r.clientChans.Store(rec.From, ch)
	r.replyChan <- pipe.Order{Type: pipe.FromClient, Msg: rec}
	timer := time.After(time.Duration(rec.Term) * time.Millisecond)
	select {
	case msg := <-ch:
		*rep = msg.Content
	case <-timer:
		*rep = "timeout"
	}
	close(ch)
	r.clientChans.Delete(rec.From)
	return nil
}

func (r *RPC) disturb() bool {
	if r.delay != 0 {
		if r.random {
			time.Sleep(time.Duration(r.delay/5+rand.Intn(r.delay/5*4)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(r.delay) * time.Millisecond)
		}
		if r.random && rand.Intn(100) == 0 {
			return true
		}
	}
	return false
}
