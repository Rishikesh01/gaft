package persistence

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type logCache struct {
	logs         [256]rafttypes.AppendLog
	currentIndex uint64
}

// circular buffer
func (l *logCache) appendToCache(log rafttypes.AppendLog) {
	if l.currentIndex == uint64(len(l.logs)-1) {
		l.currentIndex = 0
	}

	l.logs[l.currentIndex] = log
	l.currentIndex++
}
