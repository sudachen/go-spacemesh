package monitoring

import (
	"github.com/spacemeshos/go-spacemesh/log"
	"math"
	"sync"
)

type Tracker struct {
	data []uint64
	max  uint64
	min  uint64
	avg  float64
}

func NewTracker() *Tracker {
	return &Tracker{
		data: make([]uint64, 0),
		max:  0,
		min:  math.MaxUint64,
		avg:  0,
	}
}

func (t *Tracker) Track(value uint64) {
	if value > t.max {
		t.max = value
	}

	if value < t.min {
		t.min = value
	}

	count := uint64(len(t.data))
	t.avg = (float64)(count*uint64(t.avg)+value) / (float64)(count+1)

	t.data = append(t.data, value)
}

func (t *Tracker) Max() uint64 {
	return t.max
}

func (t *Tracker) Min() uint64 {
	return t.min
}

func (t *Tracker) Avg() float64 {
	return t.avg
}

func (t *Tracker) IsEmpty() bool {
	return len(t.data) == 0
}

type Controller struct {
	trackers map[string]*Tracker
	mutex    sync.Mutex
	l        log.Log
}

func NewController(l log.Log) *Controller {
	return &Controller{
		trackers: make(map[string]*Tracker),
		l:        l,
	}
}

func (c *Controller) Update(name string, sample uint64) {
	c.mutex.Lock()
	if _, exist := c.trackers[name]; !exist {
		c.trackers[name] = NewTracker()
	}
	c.trackers[name].Track(sample)
	c.mutex.Unlock()
}

func (c *Controller) Report() {
	c.mutex.Lock()

	for name, t := range c.trackers {
		c.l.With().Info("controller report", log.String("component", name), log.Float64("avg", t.Avg()), log.Uint64("max", t.Max()), log.Uint64("min", t.Min()))
	}

	c.mutex.Unlock()
}
