package main

import (
	"RaftDB/RaftDB_Client/db/kvdb_client"
	"RaftDB/kernel/types/pipe"
	"bufio"
	"fmt"
	"net/rpc"
	"os"
)

func main() {
	db := kvdb_client.KVDBClient{}
	if len(os.Args) != 2 {
		fmt.Println("input server's addr like [ip]:[host]")
		return
	}
	addr := os.Args[1]
	for {
		fmt.Printf("> ")
		order, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		content, ok := db.Parser(order)
		if !ok {
			fmt.Println("illegal operation")
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println(err)
		}
		req := pipe.MessageBody{Content: content, Term: 40000, Agree: content[1] == 'r'}
		rep := ""
		if err := client.Call("RPC.Write", req, &rep); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(rep)
	}
}
