package persistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

const recordHeaderSize = 4 + 32

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func encodeLog(buf []byte, log *rafttypes.AppendLog) []byte {
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint64(buf, uint64(log.Index))
	buf = binary.BigEndian.AppendUint64(buf, uint64(log.Term))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(log.Type)))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(log.Data)))
	buf = append(buf, log.Type...)
	buf = append(buf, log.Data...)
	binary.BigEndian.PutUint32(buf[start:], crc32.Checksum(buf[start+4:], castagnoli))
	return buf
}

func decodeLog(r *bytes.Reader) (*rafttypes.AppendLog, error) {
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	typeLen := binary.BigEndian.Uint64(hdr[20:28])
	dataLen := binary.BigEndian.Uint64(hdr[28:36])
	if typeLen > uint64(r.Len()) || dataLen > uint64(r.Len())-typeLen {
		return nil, errors.New("raft log record is corrupted")
	}

	payload := make([]byte, typeLen+dataLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	h := crc32.New(castagnoli)
	h.Write(hdr[4:])
	h.Write(payload)
	if h.Sum32() != binary.BigEndian.Uint32(hdr[:4]) {
		return nil, errors.New("raft log record is corrupted")
	}

	return &rafttypes.AppendLog{
		Index: int64(binary.BigEndian.Uint64(hdr[4:12])),
		Term:  int64(binary.BigEndian.Uint64(hdr[12:20])),
		Type:  string(payload[:typeLen]),
		Data:  payload[typeLen:],
	}, nil
}
