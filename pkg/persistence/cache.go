package persistence

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

type logCache struct {
	logs         [256]rafttypes.AppendLog
	currentIndex uint64
}

// circular buffer
func (l *logCache) appendToCache(logs ...rafttypes.AppendLog) {
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
