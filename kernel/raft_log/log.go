package raft_log

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// 必须保证并发安全

type RaftKey struct {
	Term  int
	Index int
}

type RaftLog struct {
	K RaftKey
	V string
}

type RaftLogSet struct {
	raftLogs     []RaftLog
	committedKey RaftKey
	m            sync.RWMutex
}

func (k RaftKey) Greater(key RaftKey) bool {
	if k.Term == key.Term {
		return k.Index > key.Index
	}
	return k.Term > key.Term
}

func (k RaftKey) Less(key RaftKey) bool {
	if k.Term == key.Term {
		return k.Index < key.Index
	}
	return k.Term < key.Term
}

func (k RaftKey) Equals(key RaftKey) bool {
	return k.Term == key.Term && k.Index == key.Index
}

func LogToString(content RaftLog) string {
	return fmt.Sprintf("%d$%d^%s\n", content.K.Term, content.K.Index, content.V)
}

func StringToLog(v string) (content RaftLog, err error) {
	err = errors.New("error: illegal log.(string)")
	res := strings.SplitN(v, "^", 2)
	if len(res) < 2 {
		return
	}
	str := res[1]
	res = strings.SplitN(res[0], "$", 2)
	if len(res) < 2 {
		return
	}
	term, err := strconv.Atoi(res[0])
	if err != nil {
		return
	}
	index, err := strconv.Atoi(res[1])
	if err != nil {
		return
	}
	return RaftLog{RaftKey{Term: term, Index: index}, str}, nil
}

func (l *RaftLogSet) Init(committedKeyTerm int, committedKeyIndex int) {
	l.committedKey = RaftKey{Term: committedKeyTerm, Index: committedKeyIndex}
}

func (l *RaftLogSet) GetLast() RaftKey {
	res := RaftKey{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.raftLogs) >= 1 {
		res = l.raftLogs[len(l.raftLogs)-1].K
	}
	l.m.RUnlock()
	return res
}

func (l *RaftLogSet) GetSecondLast() RaftKey {
	res := RaftKey{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.raftLogs) >= 2 {
		res = l.raftLogs[len(l.raftLogs)-2].K
	}
	l.m.RUnlock()
	return res
}

func (l *RaftLogSet) GetCommitted() RaftKey {
	return l.committedKey
}

func (l *RaftLogSet) Append(content RaftLog) { // 幂等的增加日志
	l.m.Lock()
	if len(l.raftLogs) == 0 || l.raftLogs[len(l.raftLogs)-1].K.Less(content.K) {
		l.raftLogs = append(l.raftLogs, content)
	}
	l.m.Unlock()
}

func (l *RaftLogSet) GetPrevious(key RaftKey) (RaftKey, error) { // 如果key不存在，报错，如果不存在上一个返回-1-1
	l.m.RLock()
	res := RaftKey{Term: -1, Index: -1}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res, errors.New("there is no your key")
	}
	left, right := 0, len(l.raftLogs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if !l.raftLogs[mid].K.Less(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if l.raftLogs[left].K.Less(key) {
		res = l.raftLogs[left].K
	}
	l.m.RUnlock()
	return res, nil
}

func (l *RaftLogSet) GetNext(key RaftKey) (RaftKey, error) { // 如果key不存在，报错，如果key没有下一个返回-1-1
	l.m.RLock()
	res := RaftKey{Term: -1, Index: -1}
	if key.Equals(RaftKey{-1, -1}) && len(l.raftLogs) > 0 {
		l.m.RUnlock()
		return l.raftLogs[0].K, nil
	}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res, errors.New("there is no your key")
	}
	left, right := 0, len(l.raftLogs)-1
	for left < right {
		mid := (left + right) / 2
		if l.raftLogs[mid].K.Greater(key) {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if l.raftLogs[left].K.Greater(key) {
		res = l.raftLogs[left].K
	}
	l.m.RUnlock()
	return res, nil
}

func (l *RaftLogSet) GetVByK(key RaftKey) (string, error) { // 通过Key寻找指定日志，找不到返回空
	var res string
	err := errors.New("error: can not find this log by key")
	l.m.RLock()
	left, right := 0, len(l.raftLogs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.raftLogs[mid].K.Equals(key) {
			err = nil
			res = l.raftLogs[mid].V
			break
		} else if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	l.m.RUnlock()
	if err == nil {
		return res, nil
	} else {
		return "", err
	}
}

func (l *RaftLogSet) GetLogByK(key RaftKey) (RaftLog, error) { // 通过Key寻找指定日志，找不到返回空
	var res RaftLog
	err := errors.New("error: can not find this log by key")
	l.m.RLock()
	left, right := 0, len(l.raftLogs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.raftLogs[mid].K.Equals(key) {
			err = nil
			res = l.raftLogs[mid]
			break
		} else if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	l.m.RUnlock()
	if err == nil {
		return res, nil
	} else {
		return RaftLog{}, err
	}
}

func (l *RaftLogSet) Exist(key RaftKey) bool {
	exist := false
	l.m.RLock()
	left, right := 0, len(l.raftLogs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.raftLogs[mid].K.Equals(key) {
			exist = true
			break
		} else if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	l.m.RUnlock()
	return exist
}

func (l *RaftLogSet) Commit(key RaftKey) (previousCommitted RaftKey) { // 提交所有小于等于key的日志，幂等的提交日志
	l.m.Lock()
	if len(l.raftLogs) == 0 {
		l.m.Unlock()
		return RaftKey{-1, -1}
	}
	previousCommitted = l.committedKey
	left, right := 0, len(l.raftLogs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.raftLogs[left].K.Greater(key) && previousCommitted.Less(l.raftLogs[left].K) {
		l.committedKey = l.raftLogs[left].K
	}
	l.m.Unlock()
	return
}

func (l *RaftLogSet) Remove(key RaftKey) ([]RaftLog, error) { // 删除日志直到自己的日志Key不大于key
	l.m.Lock()
	if len(l.raftLogs) == 0 {
		l.m.Unlock()
		return []RaftLog{}, nil
	}
	var ret []RaftLog
	err := errors.New("error: remove committed log")
	left, right := 0, len(l.raftLogs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.raftLogs[left].K.Greater(key) {
		if l.committedKey.Greater(l.raftLogs[left].K) {
			l.m.Unlock()
			return ret, err
		}
		ret = make([]RaftLog, len(l.raftLogs)-left-1)
		copy(ret, l.raftLogs[left+1:len(l.raftLogs)])
		l.raftLogs = l.raftLogs[0 : left+1]
	} else {
		if !l.committedKey.Equals(RaftKey{-1, -1}) {
			l.m.Unlock()
			return ret, err
		}
		ret = l.raftLogs
		l.raftLogs = []RaftLog{}
	}
	l.m.Unlock()
	return ret, nil
}

func (l *RaftLogSet) Iterator(key RaftKey) int { // 根据Key返回迭代器，没找到返回-1，线程不安全
	if len(l.raftLogs) == 0 {
		return -1
	}
	left, right := 0, len(l.raftLogs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.raftLogs[mid].K.Equals(key) {
			return mid
		} else if l.raftLogs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return -1
}

func (l *RaftLogSet) GetLogsByRange(begin RaftKey, end RaftKey) []RaftLog { // 返回 [begin, end]闭区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []RaftLog{}
	} else {
		tmp := make([]RaftLog, endIter-beginIter+1)
		copy(tmp, l.raftLogs[beginIter:endIter+1])
		l.m.RUnlock()
		return tmp
	}
}

func (l *RaftLogSet) GetKsByRange(begin RaftKey, end RaftKey) []RaftKey { // 返回 [begin, end]区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []RaftKey{}
	} else {
		tmp := make([]RaftKey, endIter-beginIter+1)
		for i := beginIter; i <= endIter; i++ {
			tmp[i-beginIter] = l.raftLogs[i].K
		}
		l.m.RUnlock()
		return tmp
	}
}

func (l *RaftLogSet) GetAll() []RaftLog { // 线程不安全
	return l.raftLogs
}

func (l *RaftLogSet) ToString() string {
	l.m.RLock()
	res := fmt.Sprintf("==== raftLogs %d ====\ncontents: %v\ncommittedKey: %v\n==== raftLogs ====", len(l.raftLogs), l.raftLogs, l.committedKey)
	l.m.RUnlock()
	return res
}
