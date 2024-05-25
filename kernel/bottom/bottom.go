package bottom

import (
	"RaftDB/kernel/meta"
	"RaftDB/kernel/pipe"
	"RaftDB/kernel/raft_log"
	"RaftDB/log_plus"
)

type Bottom struct {
	communicate   Communicate
	store         Store
	raftLogSet    *raft_log.RaftLogSet
	fromLogicChan <-chan pipe.Order // 接收me消息的管道
	toLogicChan   chan<- pipe.Order // 发送消息给me的管道
}

/*
bottom初始化，它需要完成：
	1.根据指定的配置文件位置和日志持计划位置将配置和日志读出放在内存，同时将二者作为传出参数返回上层。
	2.使用给定的存储介质实体和网线实体初始化自己的存储系统和通信系统。
	3.保存自己和Logic层的通讯管道。
	4.初始化和保存日志系统，注意bottom层和Logic层都有对日志系统的读写权限。
一旦初始化为正确执行，Panic结束。
*/

func (b *Bottom) Init(confPath string, filePath string, meta *meta.Meta, raftLogSet *raft_log.RaftLogSet,
	medium Medium, cable Cable, fromLogicChan <-chan pipe.Order, toLogicChan chan<- pipe.Order,
	mediumParam interface{}, cableParam interface{}) {

	b.store, b.raftLogSet = Store{}, raftLogSet
	b.fromLogicChan, b.toLogicChan = fromLogicChan, toLogicChan
	if err := b.store.initAndLoad(confPath, filePath, meta, raftLogSet, medium, mediumParam); err != nil {
		panic(err)
	}
	if err := b.communicate.init(cable, meta.Dns[meta.Id], meta.Dns[0:meta.Num], cableParam); err != nil {
		panic(err)
	}
	raftLogSet.Init(meta.CommittedKeyTerm, meta.CommittedKeyIndex)
}

/*
运行期间不断收取Logic层传过来的信息，进行处理。
如果一开始连接不可用说明系统无法启动，Panic处理。
在执行过程中发现通讯管道关闭，Panic返回。
communicate.listen()函数具有往toLogicChan里写入数据的权限。
*/

func (b *Bottom) Run() {
	go func() {
		err := b.communicate.listen()
		if err != nil {
			panic(err)
		}
	}()
	for {
		select {
		case order, opened := <-b.fromLogicChan:
			if !opened {
				panic("logic chan is closed")
				return
			}
			if order.Type == pipe.Store {
				if order.Msg.Agree {
					if err := b.store.updateMeta(order.Msg.Content); err != nil {
						log_plus.Println(log_plus.DEBUG_BOTTOM, err)
					}
					log_plus.Println(log_plus.DEBUG_BOTTOM, "bottom: update meta")
				} else if order.Msg.Type == pipe.FileAppend {
					raftLog, _ := b.raftLogSet.GetLogByK(order.Msg.LastLogKey)
					if err := b.store.appendLog(raftLog); err != nil {
						log_plus.Println(log_plus.DEBUG_BOTTOM, err)
					}
					log_plus.Printf(log_plus.DEBUG_BOTTOM, "bottom: write log from %d to %d\n", order.Msg.SecondLastLogKey, order.Msg.LastLogKey)
				} else if order.Msg.Type == pipe.FileTruncate {
					raftLogs, _ := order.Msg.Others.([]raft_log.RaftLog)
					if err := b.store.truncateLog(raftLogs); err != nil {
						log_plus.Println(log_plus.DEBUG_BOTTOM, err)
					}
				}
			}
			if order.Type == pipe.NodeReply {
				if err := b.communicate.replyNode(order.Msg); err != nil {
					log_plus.Println(log_plus.DEBUG_BOTTOM, err)
				}
			}
			if order.Type == pipe.ClientReply {
				if err := b.communicate.ReplyClient(order.Msg); err != nil {
					log_plus.Println(log_plus.DEBUG_BOTTOM, err)
				}
			}
		}
	}
}

func (b *Bottom) ChangeNetworkDelay(delay int, random bool) {
	b.communicate.ChangeNetworkDelay(delay, random)
}
