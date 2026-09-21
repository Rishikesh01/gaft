package persistence

import (
	"io"
	"sync"

	"github.com/Rishikesh01/gaft/pkg/rafttypes"
)

type logCache struct {
	mu           sync.Mutex
	logs         [256]rafttypes.AppendLog
	currentIndex uint64
}

// circular buffer
func (l *logCache) appendToCache(logs ...rafttypes.AppendLog) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.currentIndex == uint64(len(l.logs)) {
		l.currentIndex = 0
	}

	for _, log := range logs {
		l.logs[l.currentIndex] = log
		l.currentIndex++
		if l.currentIndex == uint64(len(l.logs)) {
			l.currentIndex = 0
		}
	}
}

func (l *logCache) getLog(index int64) (rafttypes.AppendLog, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, log := range l.logs {
		if log.Index == index {
			return log, nil
		}
	}

	return rafttypes.AppendLog{}, io.EOF
}
