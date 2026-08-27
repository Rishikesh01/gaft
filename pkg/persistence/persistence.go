package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type Persistence interface {
	Truncate(fromIndex int64) error
	LastReadVoteState() (currentTerm int64, votedFor string, err error)
	SaveVoteState(currentTerm int64, votedFor string) error
	ReadLogs(startIndex int64, endIndex int64) ([]*rafttypes.AppendLog, error)
	Append(logs ...rafttypes.AppendLog) error
}

type filePersistence struct {
	basePath  string
	EndoffSet int64
	logCache  *logCache
}

var _ Persistence = (*filePersistence)(nil)

func NewFilePersistence(basePath string) *filePersistence {
	return &filePersistence{
		basePath: basePath,
		logCache: &logCache{
			logs:         [256]rafttypes.AppendLog{},
			currentIndex: 0,
		},
	}
}

// Append implements [Persistence].
func (f *filePersistence) Append(logs ...rafttypes.AppendLog) error {
	raftLogFile, err := os.OpenFile(filepath.Join(f.basePath, fileRaftLog), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer raftLogFile.Close()

	var buf bytes.Buffer
	for _, log := range logs {
		crcValue, err := crcAppendLog(&log)
		if err != nil {
			return err
		}
		raftLog := fileFormatRaftLogs{
			crc: crcValue,
			log: &log,
		}

		if err := raftLog.Store(&buf); err != nil {
			return err
		}
	}
	if _, err := buf.WriteTo(raftLogFile); err != nil {
		return err
	}
	if err := raftLogFile.Sync(); err != nil {
		return err
	}

	f.logCache.appendToCache(logs...)

	return nil
}

func (f *filePersistence) LastReadVoteState() (currentTerm int64, votedFor string, err error) {
	data, err := os.ReadFile(f.basePath + fileVoteState)
	if err != nil {
		return 0, "", err
	}
	currentTerm = int64(binary.BigEndian.Uint64(data[0:8]))
	stringLen := int(binary.BigEndian.Uint64(data[8:16]))
	votedFor = string(data[16:stringLen])

	return
}

// ReadLogs implements [Persistence].
func (f *filePersistence) ReadLogs(startIndex int64, endIndex int64) ([]*rafttypes.AppendLog, error) {
	if endIndex <= startIndex {
		return nil, errors.New("end index cannot be smaller than  or equal to start index")
	}
	logs := make([]*rafttypes.AppendLog, 0, endIndex-startIndex)
	file, err := os.Open(f.basePath + fileRaftLog)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	for {
		logFile, err := readRaftLog(file)
		if err != nil {
			return nil, err
		}

		if logFile.log.Index == uint64(endIndex) {
			logs = append(logs, logFile.log)
			break
		}

		if logFile.log.Index >= uint64(startIndex) {
			logs = append(logs, logFile.log)
		}
	}

	return logs, nil
}

// SaveVoteState implements [Persistence].
func (f *filePersistence) SaveVoteState(currentTerm int64, votedFor string) error {
	voteStateFile, err := os.OpenFile(filepath.Join(f.basePath, fileVoteState), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer voteStateFile.Close()

	buf := make([]byte, 0, 16+len(votedFor))

	buf = binary.BigEndian.AppendUint64(buf, uint64(currentTerm))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(votedFor)))
	buf = append(buf, votedFor...)

	if _, err := voteStateFile.Write(buf); err != nil {
		return err
	}

	return voteStateFile.Sync()
}

// Truncate implements [Persistence].
func (f *filePersistence) Truncate(fromIndex int64) error {
	panic("unimplemented")
}

func crcAppendLog(log *rafttypes.AppendLog) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if err := writeLog(h, log); err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}
