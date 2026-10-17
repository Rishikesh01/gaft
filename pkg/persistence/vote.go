package persistence

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

func (f *filePersistence) SaveVoteState(currentTerm int64, votedFor string) error {
	buf := binary.BigEndian.AppendUint64(nil, uint64(currentTerm))
	buf = append(buf, votedFor...)

	path := filepath.Join(f.basePath, fileVoteState)
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
	return syncDir(f.basePath)
}

func (f *filePersistence) LastReadVoteState() (currentTerm int64, votedFor string, err error) {
	data, err := os.ReadFile(filepath.Join(f.basePath, fileVoteState))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	if len(data) < 8 {
		return 0, "", errors.New("vote state on disk is corrupted")
	}
	return int64(binary.BigEndian.Uint64(data[:8])), string(data[8:]), nil
}
