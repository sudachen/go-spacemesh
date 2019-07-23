package sync

import (
	"errors"
	"github.com/spacemeshos/go-spacemesh/log"
	"github.com/spacemeshos/go-spacemesh/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestValidationQueue_inQueue(t *testing.T) {
	r := require.New(t)
	vq := NewValidationQueue(log.NewDefault(t.Name()))

	// does not exist
	bid := types.BlockID(1)
	res := vq.inQueue(bid)
	r.False(res)

	// exist reverse
	vq.reverseDepMap[bid] = []types.BlockID{}
	res = vq.inQueue(bid)
	r.True(res)

	// exist visited
	vq.visited[types.BlockID(2)] = struct{}{}
	res = vq.inQueue(bid)
	r.True(res)
}

func TestValidationQueue_addToDatabase(t *testing.T) {
	r := require.New(t)
	vq := NewValidationQueue(log.NewDefault(t.Name()))

	vq.callbacks[types.BlockID(1)] = func() error {
		return nil
	}
	e := vq.addToDatabase(types.BlockID(1))
	r.Nil(e)

	vq.callbacks[types.BlockID(2)] = func() error {
		return errors.New("my err")
	}
	e = vq.addToDatabase(types.BlockID(2))
	r.Error(e)
}

func TestValidationQueue_getMissingBlocks(t *testing.T) {
	r := require.New(t)
	vq := NewValidationQueue(log.NewDefault(t.Name()))
	exp := make(map[types.BlockID]struct{})
	exp[1] = struct{}{}
	exp[2] = struct{}{}
	exp[3] = struct{}{}
	exp[4] = struct{}{}

	vq.reverseDepMap[types.BlockID(1)] = []types.BlockID{}
	vq.reverseDepMap[types.BlockID(2)] = []types.BlockID{5, 6, 7}
	vq.reverseDepMap[types.BlockID(3)] = []types.BlockID{1, 2}
	vq.reverseDepMap[types.BlockID(4)] = []types.BlockID{4, 5}
	missing := vq.getMissingBlocks()
	r.Equal(len(exp), len(missing))
	for bid, _ := range vq.reverseDepMap {
		_, ok := exp[bid]
		r.True(ok)
	}
}
