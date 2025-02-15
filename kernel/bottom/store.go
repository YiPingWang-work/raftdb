package bottom

import (
	"RaftDB/kernel/raft_log"
	"RaftDB/kernel/types/meta"
	"encoding/json"
	"strings"
)

type Store struct {
	medium   Medium
	confPath string
	filePath string
}

/*
存储介质接口需要实现初始化，读写和追加写功能
*/

type Medium interface {
	Init(mediumParam interface{}) error
	Read(path string, content *string) error
	Write(path string, content string) error
	Append(path string, content string) error
	Truncate(path string, cnt int64) error // 从后面截断cnt个字节
}

/*
存储系统初始化，需要初始化存储介质，之后通过该介质读取磁盘中的配置信息和日志，并将其通过传出参数返回上层。
*/

func (s *Store) initAndLoad(confPath string, filePath string, meta *meta.Meta, raftLogSet *raft_log.RaftLogSet,
	m Medium,
	mediumParam interface{}) error {
	s.medium, s.confPath, s.filePath = m, confPath, filePath
	if err := s.medium.Init(mediumParam); err != nil {
		return err
	}
	if err := s.getMeta(confPath, meta); err != nil {
		return err
	}
	if err := s.loadFrom0(filePath, raftLogSet); err != nil {
		return err
	}
	return nil
}

/*
给出一组内存中的日志，将这一组日志按照顺序追加写入磁盘文件。为保证系统持续运行，如果追加失败，提示错误不会Panic。
*/

func (s *Store) appendLog(raftLog raft_log.RaftLog) error {
	return s.medium.Append(s.filePath, raft_log.LogToString(raftLog))
}

func (s *Store) truncateLog(raftLogs []raft_log.RaftLog) error {
	total := int64(0)
	for _, v := range raftLogs {
		total += int64(len(raft_log.LogToString(v)))
	}
	return s.medium.Truncate(s.filePath, total)
}

/*
更新磁盘中的配置信息，写入失败报错。
*/

func (s *Store) updateMeta(meta string) error {
	return s.medium.Write(s.confPath, meta)
}

/*
获取磁盘中的配置信息，只有系统初始化的时候使用，获取不到报错。
*/

func (s *Store) getMeta(metaPath string, meta *meta.Meta) error {
	var str string
	if err := s.medium.Read(metaPath, &str); err != nil {
		return err
	}
	return json.Unmarshal([]byte(str), meta)
}

/*
加载磁盘中的历史记录，只有系统初始化的时候使用，获取不到报错。
*/

func (s *Store) loadFrom0(logPath string, raftLogs *raft_log.RaftLogSet) error {
	var str string
	if err := s.medium.Read(logPath, &str); err != nil {
		return err
	}
	tmp := strings.Split(str, "\n")
	for _, v := range tmp {
		if res, err := raft_log.StringToLog(v); err == nil {
			raftLogs.Append(res)
		}
	}
	return nil
}
