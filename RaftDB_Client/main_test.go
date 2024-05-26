package main

import (
	"RaftDB/RaftDB_Client/db/kvdb_client"
	"RaftDB/kernel/types/pipe"
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"testing"
)

func Test(t *testing.T) {
	db := kvdb_client.KVDBClient{}
	addr := "localhost:18002"
	pool := sync.Pool{New: func() interface{} {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return nil
		} else {
			return client
		}
	}}
	for i := 11020; i < 11121; i++ {
		i := i
		//go func() {
		content, _ := db.Parser(fmt.Sprintf("write %d %d", i, i+1))
		client, ok := pool.Get().(*rpc.Client)
		if !ok {
			log.Println("fuck")
			break
		}
		req := pipe.MessageBody{Content: content, Term: 35000, Agree: content[1] == 'r'}
		rep := ""
		if err := client.Call("RPC.Write", req, &rep); err != nil {
			fmt.Println(err)
			return
		}
		pool.Put(client)
		log.Println(rep)

		//}()
	}
	//time.Sleep(time.Second * 1000)
}
