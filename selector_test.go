package chdistr

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWRR_AddZeroWeightHost(t *testing.T) {
	s := WeightRoundRobinSelector()
	assert.Error(t, s.AddHost(NewWeightHostInfo("host1", 0)))
}

func TestWRR_AddHostWithInvalidState(t *testing.T) {
	s := WeightRoundRobinSelector()
	h := NewWeightHostInfo("host1", 1)
	h.SetState(NodeDown)

	assert.Error(t, s.AddHost(h))
}

func TestWRR_AddNonZeroWeightHost(t *testing.T) {
	r := assert.New(t)

	cases := []struct {
		host   string
		weight uint32
	}{}
	for i := uint32(1); i <= 10; i++ {
		cases = append(cases, struct {
			host   string
			weight uint32
		}{"host", i})
	}

	for _, testCase := range cases {
		s := WeightRoundRobinSelector()
		r.NoError(s.AddHost(NewWeightHostInfo(testCase.host, testCase.weight)))

		r.Equal(len(s.hosts), 1)
		h := s.hosts[testCase.host]
		r.Equal(*h, NewWeightHostInfo(testCase.host, testCase.weight))

		r.Equal(len(s.rangePos), 1)
		rangePos := s.rangePos[testCase.host]
		r.Equal(rangePos, 0)

		r.Equal(len(s.ranges), 1)
		rang := s.ranges[rangePos]
		r.Equal(rang.begin, uint32(0))
		r.Equal(rang.end, testCase.weight)

		var expectedOwns []*WeightHostInfo
		for i := uint32(0); i < testCase.weight; i++ {
			expectedOwns = append(expectedOwns, h)
		}
		r.Equal(expectedOwns, s.owns)
	}
}

func TestWRR_AddSecondHost(t *testing.T) {
	r := assert.New(t)

	cases := []struct {
		host1        WeightHostInfo
		host2        WeightHostInfo
		expectedOwns []int
		ranges       []struct {
			begin uint32
			end   uint32
		}
	}{
		{
			host1:        NewWeightHostInfo("host1", 1),
			host2:        NewWeightHostInfo("host2", 1),
			expectedOwns: []int{0, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 1},
				{1, 2},
			},
		},
		{
			host1:        NewWeightHostInfo("host1", 1),
			host2:        NewWeightHostInfo("host2", 2),
			expectedOwns: []int{0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 1},
				{1, 3},
			},
		},
		{
			host1:        NewWeightHostInfo("host1", 2),
			host2:        NewWeightHostInfo("host2", 1),
			expectedOwns: []int{0, 0, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 3},
			},
		},
		{
			host1:        NewWeightHostInfo("host1", 2),
			host2:        NewWeightHostInfo("host2", 2),
			expectedOwns: []int{0, 0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 4},
			},
		},
		{
			host1:        NewWeightHostInfo("host1", 2),
			host2:        NewWeightHostInfo("host2", 4),
			expectedOwns: []int{0, 0, 1, 1, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 6},
			},
		},
		{
			host1:        NewWeightHostInfo("host1", 4),
			host2:        NewWeightHostInfo("host2", 2),
			expectedOwns: []int{0, 0, 0, 0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 4},
				{4, 6},
			},
		},
	}

	for _, testCase := range cases {
		s := WeightRoundRobinSelector()

		r.NoError(s.AddHost(testCase.host1))
		r.NoError(s.AddHost(testCase.host2))
		r.Equal(testCase.ranges, s.ranges)

		h1 := s.hosts[testCase.host1.Hostname()]
		h2 := s.hosts[testCase.host2.Hostname()]

		r.Equal(map[string]int{
			testCase.host1.Hostname(): 0,
			testCase.host2.Hostname(): 1,
		}, s.rangePos)

		var expectedOwns []*WeightHostInfo
		for _, idx := range testCase.expectedOwns {
			switch idx {
			case 0:
				expectedOwns = append(expectedOwns, h1)
			case 1:
				expectedOwns = append(expectedOwns, h2)
			default:
				t.Fatal("check test cases because expected 0 or 1 in owns, but got ", idx)
			}
		}

		r.Equal(expectedOwns, s.owns)
	}
}

func TestWRR_LastHostWeightChanged(t *testing.T) {
	r := assert.New(t)

	hostname1 := "host1"
	hostname2 := "host2"
	newSelectorWithHosts := func() *wRoundRobinSelector {
		s := WeightRoundRobinSelector()
		s.AddHost(NewWeightHostInfo(hostname1, 2))
		s.AddHost(NewWeightHostInfo(hostname2, 1))

		return s
	}

	cases := []struct {
		host         WeightHostInfo
		expectedOwns []int
		ranges       []struct {
			begin uint32
			end   uint32
		}
	}{
		{
			host:         NewWeightHostInfo(hostname2, 1),
			expectedOwns: []int{0, 0, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 3},
			},
		},
		{
			host:         NewWeightHostInfo(hostname2, 3),
			expectedOwns: []int{0, 0, 1, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 5},
			},
		},
		{
			host:         NewWeightHostInfo(hostname2, 5),
			expectedOwns: []int{0, 0, 1, 1, 1, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 2},
				{2, 7},
			},
		},
	}

	for _, testCase := range cases {
		s := newSelectorWithHosts()
		r.NoError(s.AddHost(testCase.host))

		r.Equal(testCase.ranges, s.ranges)

		h1 := s.hosts[hostname1]
		h2 := s.hosts[hostname2]

		r.Equal(map[string]int{
			hostname1: 0,
			hostname2: 1,
		}, s.rangePos)

		var expectedOwns []*WeightHostInfo
		for _, idx := range testCase.expectedOwns {
			switch idx {
			case 0:
				expectedOwns = append(expectedOwns, h1)
			case 1:
				expectedOwns = append(expectedOwns, h2)
			default:
				t.Fatal("check test cases because expected 0 or 1 in owns, but got ", idx)
			}
		}

		r.Equal(expectedOwns, s.owns)
	}
}

func TestWRR_FirstHostWeightChanged(t *testing.T) {
	r := assert.New(t)

	hostname1 := "host1"
	hostname2 := "host2"
	newSelectorWithHosts := func() *wRoundRobinSelector {
		s := WeightRoundRobinSelector()
		s.AddHost(NewWeightHostInfo(hostname1, 2))
		s.AddHost(NewWeightHostInfo(hostname2, 2))

		return s
	}

	cases := []struct {
		host         WeightHostInfo
		expectedOwns []int
		ranges       []struct {
			begin uint32
			end   uint32
		}
	}{
		{
			host:         NewWeightHostInfo(hostname1, 1),
			expectedOwns: []int{0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 1},
				{1, 3},
			},
		},
		{
			host:         NewWeightHostInfo(hostname1, 3),
			expectedOwns: []int{0, 0, 0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 3},
				{3, 5},
			},
		},
		{
			host:         NewWeightHostInfo(hostname1, 5),
			expectedOwns: []int{0, 0, 0, 0, 0, 1, 1},
			ranges: []struct {
				begin uint32
				end   uint32
			}{
				{0, 5},
				{5, 7},
			},
		},
	}

	for _, testCase := range cases {
		s := newSelectorWithHosts()
		r.NoError(s.AddHost(testCase.host))

		r.Equal(testCase.ranges, s.ranges)

		h1 := s.hosts[hostname1]
		h2 := s.hosts[hostname2]

		r.Equal(map[string]int{
			hostname1: 0,
			hostname2: 1,
		}, s.rangePos)

		var expectedOwns []*WeightHostInfo
		for _, idx := range testCase.expectedOwns {
			switch idx {
			case 0:
				expectedOwns = append(expectedOwns, h1)
			case 1:
				expectedOwns = append(expectedOwns, h2)
			default:
				t.Fatal("check test cases because expected 0 or 1 in owns, but got ", idx)
			}
		}

		r.Equal(expectedOwns, s.owns, testCase.expectedOwns)
	}
}

func TestWRR_RemoveHostWithInvalidState(t *testing.T) {
	s := WeightRoundRobinSelector()
	h := NewWeightHostInfo("host1", 1)

	assert.Error(t, s.RemoveHost(h))
}

func TestWRR_RemoveHostThatDoesntExist(t *testing.T) {
	s := WeightRoundRobinSelector()
	h := NewWeightHostInfo("host1", 1)
	h.SetState(NodeDown)

	assert.NoError(t, s.RemoveHost(h))
}

func TestWRR_RemoveHost(t *testing.T) {
	r := assert.New(t)

	newDownWeight := func(name string) WeightHostInfo {
		h := NewWeightHostInfo(name, 1)
		h.SetState(NodeDown)
		return h
	}

	cases := []struct {
		addHosts    []WeightHostInfo
		removeHosts []WeightHostInfo

		expectedStates map[string]NodeState
	}{
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("1"),
			},
			expectedStates: map[string]NodeState{
				"1": NodeDown,
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 1),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("1"),
			},
			expectedStates: map[string]NodeState{
				"1": NodeDown,
				"2": NodeUp,
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 1),
				NewWeightHostInfo("3", 1),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("1"),
				newDownWeight("2"),
			},
			expectedStates: map[string]NodeState{
				"1": NodeDown,
				"2": NodeDown,
				"3": NodeUp,
			},
		},
	}

	for _, testCase := range cases {
		s := WeightRoundRobinSelector()

		for _, h := range testCase.addHosts {
			r.NoError(s.AddHost(h))
		}

		for _, h := range testCase.removeHosts {
			r.NoError(s.RemoveHost(h))
		}

		states := map[string]NodeState{}
		for k, h := range s.hosts {
			states[k] = h.State()
		}

		r.Equal(testCase.expectedStates, states)
	}
}

func TestWRR_Pick(t *testing.T) {
	r := assert.New(t)

	newDownWeight := func(name string) WeightHostInfo {
		h := NewWeightHostInfo(name, 1)
		h.SetState(NodeDown)
		return h
	}

	cases := []struct {
		addHosts    []WeightHostInfo
		removeHosts []WeightHostInfo

		expectedPicks []HostInfo
	}{
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"1", NodeUp},
				{"1", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"1", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"1", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("1"),
			},

			expectedPicks: []HostInfo{
				{"2", NodeUp},
				{"2", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"2", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("2"),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"1", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
				{"3", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("3"),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"1", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("1"),
				newDownWeight("3"),
			},

			expectedPicks: []HostInfo{
				{"2", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", 1),
				NewWeightHostInfo("2", 2),
				NewWeightHostInfo("3", 3),
				NewWeightHostInfo("1", 3),
			},
			removeHosts: []WeightHostInfo{
				newDownWeight("3"),
			},

			expectedPicks: []HostInfo{
				{"1", NodeUp},
				{"1", NodeUp},
				{"1", NodeUp},
				{"2", NodeUp},
				{"2", NodeUp},
			},
		},
	}

	for _, testCase := range cases {
		s := WeightRoundRobinSelector()

		for _, h := range testCase.addHosts {
			r.NoError(s.AddHost(h))
		}

		for _, h := range testCase.removeHosts {
			r.NoError(s.RemoveHost(h))
		}

		for _, expectedPick := range testCase.expectedPicks {
			r.Equal(expectedPick, s.Pick())
		}
	}
}

func TestWRR_PickWhenAllHostsIsDown(t *testing.T) {
	r := assert.New(t)
	s := WeightRoundRobinSelector()

	hosts := map[HostInfo]struct{}{}
	for i := 0; i < 100; i++ {
		h := NewWeightHostInfo(strconv.Itoa(i), 1)
		r.NoError(s.AddHost(h))
		h.SetState(NodeDown)
		r.NoError(s.RemoveHost(h))

		hosts[h.Info()] = struct{}{}
	}

	for i := 0; i < 10000; i++ {
		h := s.Pick()
		_, ok := hosts[h]
		r.True(ok)
	}
}
