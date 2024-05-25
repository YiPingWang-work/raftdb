package Commenfile

import "testing"

func TestCommonFile(t *testing.T) {
	f := CommonFile{}
	f.Init(nil)
	f.Write("a.txt", "123456\n789\n")
	x := len("789\n")
	f.Truncate("a.txt", int64(x))
}
