package persistence

import "github.com/Rishikesh01/gaft/pkg/rafttypes"

const cacheEntries = 1024

type logCache struct {
	logs [cacheEntries]*rafttypes.AppendLog
}

func (c *logCache) put(logs ...*rafttypes.AppendLog) {
	for _, log := range logs {
		c.logs[log.Index%cacheEntries] = log
	}
}

func (c *logCache) evictFrom(index int64) {
	for i, log := range c.logs {
		if log != nil && log.Index >= index {
			c.logs[i] = nil
		}
	}
}

func (c *logCache) get(index int64) *rafttypes.AppendLog {
	log := c.logs[index%cacheEntries]
	if log == nil || log.Index != index {
		return nil
	}
	return log
}
