package persistence

import (
	"encoding/binary"
	"io"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type fileFormatRaftLog struct {
	StartOffset int64
	CRC         uint32
	Log         *rafttypes.AppendLog
}

func (f *fileFormatRaftLog) Store(w io.Writer) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(f.StartOffset))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	binary.BigEndian.PutUint32(buf[:4], f.CRC)
	if _, err := w.Write(buf[:4]); err != nil {
		return err
	}

	writeLog(w, f.Log)

	return nil
}

func writeLog(w io.Writer, log *rafttypes.AppendLog) error {
	var buf [24]byte

	binary.BigEndian.PutUint64(buf[0:8], uint64(log.Index))
	binary.BigEndian.PutUint64(buf[8:16], uint64(log.Term))
	binary.BigEndian.PutUint64(buf[16:24], uint64(len(log.Data)))

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	_, err := w.Write(log.Data)
	return err
}
