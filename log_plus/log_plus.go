package log_plus

import (
	"log"
	"os"
	"sync"
)

var Grade uint64 = 0
var LogFile string

var (
	logger     *log.Logger
	loggerOnce sync.Once
)

func getLogger() *log.Logger {
	loggerOnce.Do(func() {
		var fd *os.File
		if LogFile != "" {
			if _fd, err := os.OpenFile(LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
				panic(err)
			} else {
				fd = _fd
			}
		} else {
			fd = os.Stdout
		}
		logger = log.New(fd, "", log.Ldate|log.Ltime)
	})
	return logger
}

const (
	DEBUG_LEADER      = 1 << 1
	DEBUG_FOLLOWER    = 1 << 2
	DEBUG_CANDIDATE   = 1 << 3
	DEBUG_DB          = 1 << 4
	DEBUG_CROWN       = 1 << 5
	DEBUG_BOTTOM      = 1 << 6
	DEBUG_LOGIC       = DEBUG_LEADER | DEBUG_FOLLOWER | DEBUG_CANDIDATE
	DEBUG_STORE       = 1 << 7
	DEBUG_COMMUNICATE = 1 << 8
	DEBUG_OTHER       = 1 << 9
	DEBUG_ALL         = (1 << 10) - 1
)

func Printf(grade uint64, format string, v ...any) {
	if (grade & Grade) > 0 {
		getLogger().Printf(format, v...)
	}
}

func Print(grade uint64, v ...any) {
	if (grade & Grade) > 0 {
		getLogger().Print(v...)
	}
}

func Println(grade uint64, v ...any) {
	if (grade & Grade) > 0 {
		getLogger().Println(v...)
	}
}
