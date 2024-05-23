package raft_log

import (
	"fmt"
	"testing"
)

func TestLogToString(t *testing.T) {
	x := RaftLog{
		K: RaftKey{-1, -1},
		V: "cbuiabva b  boabobvab    ubaacbai  jadbabdaibtim",
	}
	fmt.Println(LogToString(x))
	fmt.Println(StringToLog(LogToString(x)))
}
