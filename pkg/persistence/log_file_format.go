package persistence

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type fileFormatRaftLogs struct {
	crc uint32
	log *rafttypes.AppendLog
}

func (f *fileFormatRaftLogs) Store(w io.Writer) error {
	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], f.crc)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	if err := writeLog(w, f.log); err != nil {
		return err
	}

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

func readRaftLog(r io.Reader) (fileFormatRaftLogs, error) {
	var crc uint32
	if err := binary.Read(r, binary.BigEndian, &crc); err != nil {
		return fileFormatRaftLogs{}, err
	}

	var hdr [24]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fileFormatRaftLogs{}, err
	}

	index := binary.BigEndian.Uint64(hdr[0:8])
	term := binary.BigEndian.Uint64(hdr[8:16])
	dataLen := binary.BigEndian.Uint64(hdr[16:24])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return fileFormatRaftLogs{}, err
	}
	log := &rafttypes.AppendLog{
		Index: index,
		Term:  term,
		Data:  data,
	}

	verificationCRC, err := crcAppendLog(log)
	if err != nil {
		return fileFormatRaftLogs{}, err
	}
	if crc != verificationCRC {
		return fileFormatRaftLogs{}, errors.New("raft log on disk is corrupted")
	}

	return fileFormatRaftLogs{
		crc: crc,
		log: log,
	}, nil
}
