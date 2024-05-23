package main

import (
	"RaftDB/custom/communicate/RPC"
	"RaftDB/custom/db/kvdb"
	"RaftDB/custom/store/Commenfile"
	"RaftDB/kernel/bottom"
	"RaftDB/kernel/crown"
	"RaftDB/kernel/logic"
	"RaftDB/kernel/meta"
	"RaftDB/kernel/pipe"
	"RaftDB/kernel/raft_log"
	"RaftDB/monitor"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// 你可以修改Gogo函数使其完成你的定制化功能

func Gogo(confPath string, logPath string, medium bottom.Medium, cable bottom.Cable, app crown.App) {
	var b bottom.Bottom                               // 声明通信和存储底座，内部数据结构线程安全
	var m meta.Meta                                   // 新建元数据，元数据线程不安全，但只允许Logic层访问
	var logSet raft_log.RaftLogSet                    // 新建日志系统，日志系统线程安全
	var me logic.Me                                   // 新建Raft层
	var c crown.Crown                                 // 新建上层应用服务
	fromBottomChan := make(chan pipe.Order, 10000)    // 创建下层通讯管道，管道线程安全
	toBottomChan := make(chan pipe.Order, 10000)      // 创建下层通讯管道，管道线程安全
	toCrownChan := make(chan pipe.Something, 10000)   // 创建上层管道
	fromCrownChan := make(chan pipe.Something, 10000) // 创建上层通讯管道
	b.Init(confPath, logPath, &m, &logSet, medium, cable,
		toBottomChan, fromBottomChan, nil, fromBottomChan) // 初始化系统底座，初始化meta和logs（传入传出参数）
	rand.Seed(time.Now().UnixNano() + int64(m.Id%m.Num))                           // 设置随机因子
	log.Printf("\n%s\n", m.ToString())                                             // 输出元数据信息
	log.Printf("\n%s\n", logSet.ToString())                                        // 输出日志信息
	me.Init(&m, &logSet, fromBottomChan, toBottomChan, fromCrownChan, toCrownChan) // 初始化Raft层，raft层和bottom可以共享访问log，但是meta只有Raft层可以访问
	c.Init(&logSet, app, toCrownChan, fromCrownChan)
	go b.Run() // 运行底座，运行网络监听，开始对接端口Msg.ToLogicChan, order.ReplyChan监听
	go c.Run()
	go me.Run()
	monitor.Monitor(&me, &logSet, &b, &c)
}

func main() {
	if len(os.Args) != 3 {
		log.Println("need config file and history log file")
		return
	}
	confPath, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Println("input error: illegal file")
		return
	}
	filePath, err := filepath.Abs(os.Args[2])
	if err != nil {
		log.Println("input error: illegal file")
		return
	}
	log.Printf("config: %s, datafile: %s\n", confPath, filePath)
	Gogo(confPath, filePath, &Commenfile.CommonFile{}, &RPC.RPC{}, &kvdb.KVDB{}) // 原神，启动！

}
