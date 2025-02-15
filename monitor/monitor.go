package monitor

import (
	"RaftDB/kernel/bottom"
	"RaftDB/kernel/crown"
	"RaftDB/kernel/logic"
	"RaftDB/kernel/raft_log"
	"fmt"
	"strconv"
	"strings"
)

func Monitor(me *logic.Me, raftLogSet *raft_log.RaftLogSet, bottom *bottom.Bottom, crown *crown.Crown) { // 禁止修改参数
	for {
		var x string
		fmt.Scanln(&x)
		if x == "me" {
			fmt.Println(me.ToString())
			continue
		} else if x == "log" {
			fmt.Println(raftLogSet.ToString())
			continue
		} else if x == "app" {
			fmt.Println(crown.ToString())
			continue
		} else if x == "delay" {
			bottom.ChangeNetworkDelay(10000, true)
			crown.ChangeProcessDelay(5000, true)
			fmt.Println("all delay changed")
			continue
		} else {
			tmp := strings.Split(x, ",")
			if len(tmp) == 3 && tmp[0] == "netdelay" {
				delay, err := strconv.Atoi(tmp[1])
				if err == nil {
					random, err := strconv.Atoi(tmp[2])
					if err == nil {
						bottom.ChangeNetworkDelay(delay, random != 0)
						fmt.Println("network delay changed")
						continue
					}
				}
			} else if len(tmp) == 3 && tmp[0] == "appdelay" {
				delay, err := strconv.Atoi(tmp[1])
				if err == nil {
					random, err := strconv.Atoi(tmp[2])
					if err == nil {
						crown.ChangeProcessDelay(delay, random != 0)
						fmt.Println("app delay changed")
						continue
					}
				}
			}
		}
		fmt.Println("use 'me' to get node info, " +
			"use 'log' to get log info, " +
			"use 'netdelay,[ms],[randn]' to imitate network delay, " +
			"use 'appdelay,[ms],[randn]' to imitate app's process delay, " +
			"use app to get app info")
	}
}
