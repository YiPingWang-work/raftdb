package Commenfile

import (
	"RaftDB/log_plus"
	"fmt"
	"os"
)

/*
type Medium interface {
	Init(mediumParam interface{}) error
	Read(path string, content *string) error
	Write(path string, content string) error
	Append(path string, content string) error
}
*/

type CommonFile struct {
	fs map[string]*os.File
}

func (c *CommonFile) Init(interface{}) error {
	c.fs = map[string]*os.File{}
	return nil
}

func (c *CommonFile) Read(path string, content *string) error {
	f, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	*content = string(f)
	return nil
}

func (c *CommonFile) Write(path string, content string) error {
	return os.WriteFile(path, []byte(content), 0666)
}

func (c *CommonFile) Append(path string, content string) error {
	log_plus.Println(log_plus.DEBUG_STORE, "append file")
	if f, err := c.open(path); err != nil {
		return err
	} else {
		_, err = fmt.Fprint(f, content)
		return err
	}
}

func (c *CommonFile) Truncate(path string, cnt int64) error {
	log_plus.Println(log_plus.DEBUG_STORE, "truncate file")
	if f, err := c.open(path); err != nil {
		return err
	} else {
		stat, _ := f.Stat()
		return f.Truncate(stat.Size() - cnt)
	}
}

func (c *CommonFile) open(path string) (f *os.File, err error) {
	f, has := c.fs[path]
	if !has {
		log_plus.Println(log_plus.DEBUG_STORE, "open file")
		f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			return
		}
		c.fs[path] = f
	}
	return
}
