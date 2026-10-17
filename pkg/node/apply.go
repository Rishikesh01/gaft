package node

import (
	"github.com/Rishikesh01/gaft/pkg/rafttypes"
	"go.uber.org/zap"
)

type Application interface {
	Apply(log *rafttypes.AppendLog)
}

func (c *ClusterNode) applyLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.commitIndexAdvanced:
		}
		commit := c.lastCommittedIndex.Load()
		for c.lastAppliedIndex < commit {
			log, err := c.persist.ReadLogIndex(c.lastAppliedIndex + 1)
			if err != nil {
				c.log.Panic("apply failed", zap.Error(err))
			}
			switch log.Type {
			case rafttypes.LogTypeApplication:
				c.app.Apply(log)
			case rafttypes.LogTypeRaftCluster:
			}
			c.lastAppliedIndex = log.Index
		}
	}
}
