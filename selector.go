package chdistr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

var (
	_ HostSelector[HostInfo]       = &roundRobinSelector{}
	_ HostSelector[WeightHostInfo] = &wRoundRobinSelector{}
)

type HostStateController[T Node] interface {
	AddHost(h T) error
	RemoveHost(h T) error
}

type HostSelector[T Node] interface {
	HostStateController[T]
	Pick() HostInfo
}

func ListenStates[T Node](ctx context.Context, controller HostStateController[T], stch <-chan T) error {
	for {
		select {
		case h := <-stch:
			hi := h.Info()
			switch hi.State() {
			case NodeUp:
				if err := controller.AddHost(h); err != nil {
					return fmt.Errorf("add host: %s", err)
				}
			case NodeDown:
				if err := controller.RemoveHost(h); err != nil {
					return fmt.Errorf("remove host: %s", err)
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type roundRobinSelector struct {
	mu sync.Mutex

	keys       []string
	keysPos    map[string]int
	currentIdx uint64

	hosts map[string]HostInfo
}

func (s *roundRobinSelector) AddHost(h HostInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.addHost(h)
}

func (s *roundRobinSelector) RemoveHost(h HostInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.removeHost(h)
}

func (s *roundRobinSelector) Pick() HostInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pick()
}

func (s *roundRobinSelector) pick() HostInfo {
	idx := s.currentIdx % uint64(len(s.keys))

	s.currentIdx = (s.currentIdx + 1) % math.MaxUint64
	return s.hosts[s.keys[idx]]
}

func (s *roundRobinSelector) addHost(h HostInfo) error {
	hst := h.Hostname()
	if st := h.State(); st == NodeDown {
		return fmt.Errorf("host %s must have %s state, but got %s", hst, NodeUp, st)
	}

	if _, ok := s.hosts[hst]; !ok {
		s.hosts[hst] = h
		s.keys = append(s.keys, hst)
		s.keysPos[hst] = len(s.keys) - 1
	}

	return nil
}

func (s *roundRobinSelector) removeHost(h HostInfo) error {
	hst := h.Hostname()
	if st := h.State(); st == NodeUp {
		return fmt.Errorf("host %s must have %s state, but got %s", hst, NodeDown, st)
	}

	if _, ok := s.keysPos[hst]; !ok {
		return nil
	}

	keyIdx := s.keysPos[hst]
	s.keys = append(s.keys[:keyIdx], s.keys[keyIdx+1:]...)

	delete(s.hosts, hst)
	delete(s.keysPos, hst)

	for hst, pos := range s.keysPos {
		if pos >= keyIdx+1 {
			s.keysPos[hst] = pos - 1
		}
	}

	return nil
}

func RoundRobinSelector() *roundRobinSelector {
	return &roundRobinSelector{
		keysPos: map[string]int{},
		hosts:   map[string]HostInfo{},
	}
}

type wRoundRobinSelector struct {
	mu sync.RWMutex

	// own range in [begin, end)
	ranges []struct {
		begin uint32
		end   uint32
	}
	rangePos map[string]int

	currentIdx uint32

	owns  []*WeightHostInfo
	hosts map[string]*WeightHostInfo
}

// Adds host. New weight will affect the distribution.
// State must have NodeUp.
func (s *wRoundRobinSelector) AddHost(h WeightHostInfo) error {
	if h.Weight() == 0 {
		return errors.New("weight must be non zero")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.addHost(h)
}

// Removes host. New weight has no effect.
// State must have NodeDown.
func (s *wRoundRobinSelector) RemoveHost(h WeightHostInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.removeHost(h)
}

func (s *wRoundRobinSelector) Pick() HostInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pick()
}

func (s *wRoundRobinSelector) addHost(h WeightHostInfo) error {
	hst := h.Hostname()

	hinfo, ok := s.hosts[hst]
	if !ok {
		newHost := NewWeightHostInfo(hst, h.Weight())
		hinfo = &newHost
		s.hosts[hst] = hinfo

		newRange := struct {
			begin uint32
			end   uint32
		}{0, hinfo.Weight()}

		if len(s.ranges) != 0 {
			lastRange := s.ranges[len(s.ranges)-1]

			// add offset
			newRange.begin += lastRange.end
			newRange.end += lastRange.end
		}

		s.ranges = append(s.ranges, newRange)
		s.rangePos[hst] = len(s.ranges) - 1

		for i := uint32(0); i < hinfo.Weight(); i++ {
			s.owns = append(s.owns, hinfo)
		}
	}

	if st := h.State(); st == NodeDown {
		return fmt.Errorf("host %s must have %s state, but got %s", hst, NodeUp, st)
	} else {
		hinfo.SetState(st)
	}

	if hinfo.Weight() == h.Weight() {
		return nil
	}

	diffWeight := h.Weight() - hinfo.Weight()
	// simple case: if host is last then append to owns.
	if pos := s.rangePos[hst]; pos == len(s.rangePos)-1 {
		s.owns = s.rebalance(s.owns, hinfo, h.Weight())
		s.ranges[pos].end += diffWeight
	} else {
		// need shift of subslice
		curRange := s.ranges[pos]

		sub1 := s.owns[:curRange.begin]
		sub2 := s.owns[curRange.end:]

		var curOwns []*WeightHostInfo
		curOwns = append(curOwns, s.owns[curRange.begin:curRange.end]...)
		curOwns = s.rebalance(curOwns, hinfo, h.Weight())

		var owns []*WeightHostInfo
		owns = append(owns, sub1...)
		owns = append(owns, curOwns...)
		owns = append(owns, sub2...)
		s.owns = owns

		// update ranges
		s.ranges[pos].end += diffWeight
		for i := pos + 1; i < len(s.ranges); i++ {
			s.ranges[i].begin += diffWeight
			s.ranges[i].end += diffWeight
		}
	}

	hinfo.weight = h.Weight()
	return nil
}

func (s *wRoundRobinSelector) rebalance(owns []*WeightHostInfo, h *WeightHostInfo, newWeight uint32) []*WeightHostInfo {
	diffWeight := int32(newWeight - h.Weight())
	if diffWeight > 0 {
		for i := int32(0); i < diffWeight; i++ {
			owns = append(owns, h)
		}
	} else {
		owns = owns[:int32(len(owns))+diffWeight]
	}

	return owns
}

func (s *wRoundRobinSelector) removeHost(h WeightHostInfo) error {
	hst := h.Hostname()
	if st := h.State(); st == NodeUp {
		return fmt.Errorf("host %s must have %s state, but got %s", hst, NodeDown, st)
	}

	hinfo, ok := s.hosts[hst]
	if !ok {
		return nil
	}

	hinfo.SetState(h.State())

	return nil
}

func (s *wRoundRobinSelector) pick() HostInfo {
	var (
		downCnt int
		h       *WeightHostInfo
	)
	for {
		if downCnt == len(s.hosts) {
			for _, rndHost := range s.hosts {
				h = rndHost
				break
			}

			break
		}

		idx := s.currentIdx % uint32(len(s.owns))

		s.currentIdx = (s.currentIdx + 1) % math.MaxUint32
		h = s.owns[idx]

		if h.State() == NodeDown {
			downCnt++

			pos := s.rangePos[h.Hostname()]
			s.currentIdx = s.ranges[pos].end
		} else {
			break
		}
	}

	return h.Info()
}

func WeightRoundRobinSelector() *wRoundRobinSelector {
	return &wRoundRobinSelector{
		rangePos: map[string]int{},
		hosts:    map[string]*WeightHostInfo{},
	}
}
