package persistence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type Persistence interface {
	Append(logs ...*rafttypes.AppendLog) (lastIndex int64, err error)
	Truncate(fromIndex int64) error
	ReadLogIndex(index int64) (*rafttypes.AppendLog, error)
	ReadLogs(startIndex int64, endIndex int64) ([]*rafttypes.AppendLog, error)
	GetRaftClusterConfigLogs() ([]rafttypes.RaftClusterState, error)
	LastReadVoteState() (currentTerm int64, votedFor string, err error)
	SaveVoteState(currentTerm int64, votedFor string) error
}

type filePersistence struct {
	mu        sync.Mutex
	basePath  string
	logDir    string
	lastIndex int64
	cache     logCache
}

var _ Persistence = (*filePersistence)(nil)

func NewFilePersistence(basePath string) (*filePersistence, error) {
	f := &filePersistence{basePath: basePath, logDir: filepath.Join(basePath, logDirName)}
	if err := os.MkdirAll(f.logDir, 0o755); err != nil {
		return nil, err
	}

	last := int64(-1)
	for {
		if _, err := os.Stat(f.segmentPath(last + 1)); err != nil {
			break
		}
		last++
	}
	if last < 0 {
		return f, nil
	}

	logs, torn, err := f.readSegment(last)
	if err != nil {
		return nil, err
	}
	f.lastIndex = last*segmentEntries + int64(len(logs))
	f.cache.put(logs...)
	if torn {
		if err := f.cutSegment(last, f.lastIndex); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func (f *filePersistence) segmentPath(segment int64) string {
	return filepath.Join(f.logDir, fmt.Sprintf("%d.seg", segment))
}

func segmentOf(index int64) int64 {
	return (index - 1) / segmentEntries
}

func (f *filePersistence) readSegment(segment int64) ([]*rafttypes.AppendLog, bool, error) {
	data, err := os.ReadFile(f.segmentPath(segment))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var logs []*rafttypes.AppendLog
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		log, err := decodeLog(r)
		if err != nil || log.Index != segment*segmentEntries+int64(len(logs))+1 {
			return logs, true, nil
		}
		logs = append(logs, log)
	}
	return logs, false, nil
}

func (f *filePersistence) Append(logs ...*rafttypes.AppendLog) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for len(logs) > 0 && logs[0].Index <= f.lastIndex {
		existing, err := f.read(logs[0].Index)
		if err != nil {
			return f.lastIndex, err
		}
		if existing.Term != logs[0].Term {
			if err := f.truncate(logs[0].Index); err != nil {
				return f.lastIndex, err
			}
			break
		}
		logs = logs[1:]
	}
	if len(logs) == 0 {
		return f.lastIndex, nil
	}
	if logs[0].Index != f.lastIndex+1 {
		return f.lastIndex, fmt.Errorf("append at %d leaves a gap, log ends at %d", logs[0].Index, f.lastIndex)
	}

	for len(logs) > 0 {
		segment := segmentOf(logs[0].Index)
		n := 1
		for n < len(logs) && segmentOf(logs[n].Index) == segment {
			n++
		}
		if err := f.appendToSegment(segment, logs[:n]); err != nil {
			f.cutSegment(segment, f.lastIndex)
			return f.lastIndex, err
		}
		f.cache.put(logs[:n]...)
		f.lastIndex = logs[n-1].Index
		logs = logs[n:]
	}
	return f.lastIndex, nil
}

func (f *filePersistence) appendToSegment(segment int64, logs []*rafttypes.AppendLog) error {
	path := f.segmentPath(segment)
	_, err := os.Stat(path)
	created := errors.Is(err, os.ErrNotExist)

	var buf []byte
	for _, log := range logs {
		buf = encodeLog(buf, log)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(buf); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if created {
		return syncDir(f.logDir)
	}
	return nil
}

func (f *filePersistence) cutSegment(segment int64, lastIndex int64) error {
	path := f.segmentPath(segment)
	logs, _, err := f.readSegment(segment)
	if err != nil {
		return err
	}
	keep := max(lastIndex-segment*segmentEntries, 0)
	logs = logs[:min(keep, int64(len(logs)))]

	if len(logs) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return syncDir(f.logDir)
	}

	var buf []byte
	for _, log := range logs {
		buf = encodeLog(buf, log)
	}
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(buf); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDir(f.logDir)
}

func (f *filePersistence) Truncate(fromIndex int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.truncate(fromIndex)
}

func (f *filePersistence) truncate(fromIndex int64) error {
	if fromIndex > f.lastIndex {
		return nil
	}
	for segment := segmentOf(f.lastIndex); segment > segmentOf(fromIndex); segment-- {
		if err := os.Remove(f.segmentPath(segment)); err != nil {
			return err
		}
	}
	if err := syncDir(f.logDir); err != nil {
		return err
	}
	if err := f.cutSegment(segmentOf(fromIndex), fromIndex-1); err != nil {
		return err
	}
	f.cache.evictFrom(fromIndex)
	f.lastIndex = fromIndex - 1
	return nil
}

func (f *filePersistence) read(index int64) (*rafttypes.AppendLog, error) {
	if index < 1 || index > f.lastIndex {
		return nil, io.EOF
	}
	if log := f.cache.get(index); log != nil {
		return log, nil
	}
	segment := segmentOf(index)
	logs, _, err := f.readSegment(segment)
	if err != nil {
		return nil, err
	}
	i := index - segment*segmentEntries - 1
	if i >= int64(len(logs)) {
		return nil, fmt.Errorf("log segment %d is corrupted", segment)
	}
	return logs[i], nil
}

func (f *filePersistence) ReadLogIndex(index int64) (*rafttypes.AppendLog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.read(index)
}

func (f *filePersistence) ReadLogs(startIndex int64, endIndex int64) ([]*rafttypes.AppendLog, error) {
	if endIndex <= startIndex {
		return nil, errors.New("end index cannot be smaller than or equal to start index")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var result []*rafttypes.AppendLog
	for index := max(startIndex, 1); index <= min(endIndex, f.lastIndex); index++ {
		log, err := f.read(index)
		if err != nil {
			return result, err
		}
		result = append(result, log)
	}
	if endIndex > f.lastIndex {
		return result, io.EOF
	}
	return result, nil
}

func (f *filePersistence) GetRaftClusterConfigLogs() ([]rafttypes.RaftClusterState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	stateLogs := []rafttypes.RaftClusterState{}
	for segment := int64(0); segment <= segmentOf(f.lastIndex) && f.lastIndex > 0; segment++ {
		logs, _, err := f.readSegment(segment)
		if err != nil {
			return nil, err
		}
		for _, log := range logs {
			if log.Type != rafttypes.LogTypeRaftCluster {
				continue
			}
			var raftClusterChange rafttypes.RaftClusterState
			if err := json.Unmarshal(log.Data, &raftClusterChange); err != nil {
				return nil, err
			}
			stateLogs = append(stateLogs, raftClusterChange)
		}
	}
	return stateLogs, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
