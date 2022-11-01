package chdistr

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRRobin_AddHostWithInvalidState(t *testing.T) {
	s := RoundRobinSelector()
	h := NewHostInfoWithState("host1", "default", HostDown)

	assert.Error(t, s.AddHost(h))
}

func TestRRobin_AddHosts(t *testing.T) {
	r := assert.New(t)
	s := RoundRobinSelector()
	db := "default"
	withDB := func(v string) string { return v + db }

	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("host%d", i+1)
		hst := NewHostInfo(name, db)
		r.NoError(s.AddHost(hst))
	}

	r.Equal(map[string]HostInfo{
		withDB("host1"): NewHostInfo("host1", db),
		withDB("host2"): NewHostInfo("host2", db),
		withDB("host3"): NewHostInfo("host3", db),
	}, s.hosts)

	r.Equal(map[string]int{
		withDB("host1"): 0,
		withDB("host2"): 1,
		withDB("host3"): 2,
	}, s.keysPos)

	r.Equal([]string{withDB("host1"), withDB("host2"), withDB("host3")}, s.keys)
}

func TestRRobin_RemoveHostWithInvalidState(t *testing.T) {
	s := RoundRobinSelector()
	h := NewHostInfo("host1", "default")
	assert.NoError(t, s.AddHost(h))
	assert.Error(t, s.RemoveHost(h))
}

func TestRRobin_RemoveNonexistentHost(t *testing.T) {
	s := RoundRobinSelector()
	h := NewHostInfoWithState("host1", "default", HostDown)
	assert.NoError(t, s.RemoveHost(h))
}

func TestRRobin_RemoveLastHost(t *testing.T) {
	r := assert.New(t)

	cases := []struct {
		addHosts     []string
		removeHosts  []string
		expectedKeys []string
		expectedPos  map[string]int
	}{
		{
			addHosts:     []string{},
			removeHosts:  []string{"host1"},
			expectedKeys: nil,
			expectedPos:  map[string]int{},
		},
		{
			addHosts:     []string{"host1"},
			removeHosts:  []string{"host1"},
			expectedKeys: []string{},
			expectedPos:  map[string]int{},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host1"},
			expectedKeys: []string{"host2", "host3"},
			expectedPos:  map[string]int{"host2": 0, "host3": 1},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host2"},
			expectedKeys: []string{"host1", "host3"},
			expectedPos:  map[string]int{"host1": 0, "host3": 1},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host3"},
			expectedKeys: []string{"host1", "host2"},
			expectedPos:  map[string]int{"host1": 0, "host2": 1},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host1", "host2"},
			expectedKeys: []string{"host3"},
			expectedPos:  map[string]int{"host3": 0},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host1", "host3"},
			expectedKeys: []string{"host2"},
			expectedPos:  map[string]int{"host2": 0},
		},
		{
			addHosts:     []string{"host1", "host2", "host3"},
			removeHosts:  []string{"host2", "host3"},
			expectedKeys: []string{"host1"},
			expectedPos:  map[string]int{"host1": 0},
		},
	}

	for i, testCase := range cases {
		s := RoundRobinSelector()

		for _, hostName := range testCase.addHosts {
			h := NewHostInfo(hostName, "")
			r.NoError(s.AddHost(h), "test case ", i)
		}

		for _, hostName := range testCase.removeHosts {
			h := NewHostInfoWithState(hostName, "", HostDown)
			r.NoError(s.RemoveHost(h), "test case ", i)
		}

		r.Equal(testCase.expectedKeys, s.keys, "test case ", i)
		r.Equal(testCase.expectedPos, s.keysPos, "test case ", i)
	}
}

func TestRRobin_Pick(t *testing.T) {
	r := assert.New(t)
	db := "default"

	cases := []struct {
		addHosts    []HostInfo
		removeHosts []HostInfo

		expectedPicks []HostInfo
	}{
		{
			addHosts: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host2", db),
				NewHostInfo("host3", db),
			},
			expectedPicks: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host2", db),
				NewHostInfo("host3", db),
				NewHostInfo("host1", db),
				NewHostInfo("host2", db),
				NewHostInfo("host3", db),
			},
		},
		{
			addHosts: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host2", db),
				NewHostInfo("host3", db),
			},
			removeHosts: []HostInfo{
				NewHostInfoWithState("host2", db, HostDown),
			},
			expectedPicks: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host3", db),
				NewHostInfo("host1", db),
				NewHostInfo("host3", db),
			},
		},
		{
			addHosts: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host2", db),
				NewHostInfo("host3", db),
			},
			removeHosts: []HostInfo{
				NewHostInfoWithState("host2", db, HostDown),
				NewHostInfoWithState("host3", db, HostDown),
			},
			expectedPicks: []HostInfo{
				NewHostInfo("host1", db),
				NewHostInfo("host1", db),
				NewHostInfo("host1", db),
			},
		},
	}

	for i, testCase := range cases {
		s := RoundRobinSelector()

		for _, h := range testCase.addHosts {
			r.NoError(s.AddHost(h), "test case ", i)
		}

		for _, h := range testCase.removeHosts {
			r.NoError(s.RemoveHost(h), "test case ", i)
		}

		for _, h := range testCase.expectedPicks {
			r.Equal(h, s.Pick())
		}
	}
}

func TestWRR_AddZeroWeightHost(t *testing.T) {
	s := WeightRoundRobinSelector()
	assert.Error(t, s.AddHost(NewWeightHostInfo("host1", "default", 0)))
}

func TestWRR_AddHostWithInvalidState(t *testing.T) {
	s := WeightRoundRobinSelector()
	h := NewWeightHostInfoWithState("host1", "default", 1, HostDown)
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
		r.NoError(s.AddHost(NewWeightHostInfo(testCase.host, "", testCase.weight)))

		r.Equal(len(s.hosts), 1)
		h := s.hosts[testCase.host]
		r.Equal(*h, NewWeightHostInfo(testCase.host, "", testCase.weight))

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
	db := "default"

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
			host1:        NewWeightHostInfo("host1", db, 1),
			host2:        NewWeightHostInfo("host2", db, 1),
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
			host1:        NewWeightHostInfo("host1", db, 1),
			host2:        NewWeightHostInfo("host2", db, 2),
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
			host1:        NewWeightHostInfo("host1", db, 2),
			host2:        NewWeightHostInfo("host2", db, 1),
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
			host1:        NewWeightHostInfo("host1", db, 2),
			host2:        NewWeightHostInfo("host2", db, 2),
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
			host1:        NewWeightHostInfo("host1", db, 2),
			host2:        NewWeightHostInfo("host2", db, 4),
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
			host1:        NewWeightHostInfo("host1", db, 4),
			host2:        NewWeightHostInfo("host2", db, 2),
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

		h1 := s.hosts[testCase.host1.ID()]
		h2 := s.hosts[testCase.host2.ID()]

		r.Equal(map[string]int{
			testCase.host1.ID(): 0,
			testCase.host2.ID(): 1,
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
	db := "default"
	withDB := func(v string) string { return v + db }
	newSelectorWithHosts := func() *wRoundRobinSelector {
		s := WeightRoundRobinSelector()
		s.AddHost(NewWeightHostInfo(hostname1, db, 2))
		s.AddHost(NewWeightHostInfo(hostname2, db, 1))

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
			host:         NewWeightHostInfo(hostname2, db, 1),
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
			host:         NewWeightHostInfo(hostname2, db, 3),
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
			host:         NewWeightHostInfo(hostname2, db, 5),
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

		h1 := s.hosts[withDB(hostname1)]
		h2 := s.hosts[withDB(hostname2)]

		r.Equal(map[string]int{
			withDB(hostname1): 0,
			withDB(hostname2): 1,
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
	db := "default"
	withDB := func(v string) string { return v + db }
	newSelectorWithHosts := func() *wRoundRobinSelector {
		s := WeightRoundRobinSelector()
		s.AddHost(NewWeightHostInfo(hostname1, db, 2))
		s.AddHost(NewWeightHostInfo(hostname2, db, 2))

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
			host:         NewWeightHostInfo(hostname1, db, 1),
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
			host:         NewWeightHostInfo(hostname1, db, 3),
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
			host:         NewWeightHostInfo(hostname1, db, 5),
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

		h1 := s.hosts[withDB(hostname1)]
		h2 := s.hosts[withDB(hostname2)]

		r.Equal(map[string]int{
			withDB(hostname1): 0,
			withDB(hostname2): 1,
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
	h := NewWeightHostInfo("host1", "default", 1)

	assert.Error(t, s.RemoveHost(h))
}

func TestWRR_RemoveHostThatDoesntExist(t *testing.T) {
	s := WeightRoundRobinSelector()
	h := NewWeightHostInfoWithState("host1", "default", 1, HostDown)
	assert.NoError(t, s.RemoveHost(h))
}

func TestWRR_RemoveHost(t *testing.T) {
	r := assert.New(t)

	db := "default"
	withDB := func(v string) string { return v + db }
	cases := []struct {
		addHosts    []WeightHostInfo
		removeHosts []WeightHostInfo

		expectedStates map[string]HostState
	}{
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("1", db, 1, HostDown),
			},
			expectedStates: map[string]HostState{
				withDB("1"): HostDown,
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 1),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("1", db, 1, HostDown),
			},
			expectedStates: map[string]HostState{
				withDB("1"): HostDown,
				withDB("2"): HostUp,
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 1),
				NewWeightHostInfo("3", db, 1),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("1", db, 1, HostDown),
				NewWeightHostInfoWithState("2", db, 1, HostDown),
			},
			expectedStates: map[string]HostState{
				withDB("1"): HostDown,
				withDB("2"): HostDown,
				withDB("3"): HostUp,
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

		states := map[string]HostState{}
		for k, h := range s.hosts {
			states[k] = h.State
		}

		r.Equal(testCase.expectedStates, states)
	}
}

func TestWRR_Pick(t *testing.T) {
	r := assert.New(t)

	db := "default"
	cases := []struct {
		addHosts    []WeightHostInfo
		removeHosts []WeightHostInfo

		expectedPicks []HostInfo
	}{
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"1", db, HostUp},
				{"1", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"1", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"1", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("1", db, 1, HostDown),
			},

			expectedPicks: []HostInfo{
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"2", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("2", db, 1, HostDown),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"1", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
				{"3", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("3", db, 1, HostDown),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"1", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("1", db, 1, HostDown),
				NewWeightHostInfoWithState("3", db, 1, HostDown),
			},

			expectedPicks: []HostInfo{
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
			},
		},
		{
			addHosts: []WeightHostInfo{
				NewWeightHostInfo("1", db, 1),
				NewWeightHostInfo("2", db, 2),
				NewWeightHostInfo("3", db, 3),
				NewWeightHostInfo("1", db, 3),
			},
			removeHosts: []WeightHostInfo{
				NewWeightHostInfoWithState("3", db, 1, HostDown),
			},

			expectedPicks: []HostInfo{
				{"1", db, HostUp},
				{"1", db, HostUp},
				{"1", db, HostUp},
				{"2", db, HostUp},
				{"2", db, HostUp},
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
		h := NewWeightHostInfo(strconv.Itoa(i), "default", 1)
		r.NoError(s.AddHost(h))
		h = h.SetState(HostDown).(WeightHostInfo)
		r.NoError(s.RemoveHost(h))

		hosts[h.Info()] = struct{}{}
	}

	for i := 0; i < 10000; i++ {
		h := s.Pick()
		_, ok := hosts[h]
		r.True(ok)
	}
}
