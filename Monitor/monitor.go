package Monitor

import (
	"RaftDB/Kernel/Bottom"
	"RaftDB/Kernel/Crown"
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Logic"
	"fmt"
	"strconv"
	"strings"
)

func Monitor(me *Logic.Me, logs *Log.LogSet, bottom *Bottom.Bottom, crown *Crown.Crown) { // 禁止修改参数
	for {
		var x string
		fmt.Scanln(&x)
		if x == "me" {
			fmt.Println(me.ToString())
			continue
		} else if x == "log" {
			fmt.Println(logs.ToString())
			continue
		} else if x == "crown" {
			fmt.Println(crown.ToString())
			continue
		} else {
			tmp := strings.Split(x, ",")
			if len(tmp) == 3 && tmp[0] == "delay" {
				delay, err := strconv.Atoi(tmp[1])
				if err == nil {
					random, err := strconv.Atoi(tmp[2])
					if err == nil {
						bottom.ChangeNetworkDelay(delay, random != 0)
						fmt.Println("network delay changed")
						continue
					}
				}
			}
		}
		fmt.Println("use 'me' to get node info, use 'log' to get log info, use 'delay,[ms],[randn]' to imitate network delay")
	}
}
