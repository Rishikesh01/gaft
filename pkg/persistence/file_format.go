package persistence

import (
	"encoding/binary"
	"io"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type fileFormatRaftLog struct {
	crc uint32
	log *rafttypes.AppendLog
}

type fileFormatIndex struct {
	header indexHeader
	indexs [256]indexs
}

type indexs struct {
	startOffSet uint64
	fileName    string
}

type indexHeader struct {
	crc        uint32
	startIndex uint64
	endIndex   uint64
}

func (f *fileFormatRaftLog) Store(w io.Writer) error {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], f.crc)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	writeLog(w, f.log)

	return nil
}

func writeLog(w io.Writer, log *rafttypes.AppendLog) error {
	var buf [24]byte

	binary.BigEndian.PutUint64(buf[0:8], log.Index)
	binary.BigEndian.PutUint64(buf[8:16], log.Term)
	binary.BigEndian.PutUint64(buf[16:24], uint64(len(log.Data)))

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	_, err := w.Write(log.Data)
	return err
}

func (f *fileFormatIndex) Store(w io.Writer) error {
	var buf [24]byte

	binary.BigEndian.PutUint64(buf[0:8], f.header.startIndex)
	binary.BigEndian.PutUint64(buf[8:16], f.header.endIndex)
	binary.BigEndian.PutUint32(buf[16:20], f.header.crc)

	return nil
}
