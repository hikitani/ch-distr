package chdistr

import "fmt"

type HostState uint32

const (
	HostUp HostState = iota
	HostDown
)

var hostStateStrings = [...]string{"HOST_UP", "HOST_DOWN"}

func (s HostState) String() string {
	return hostStateStrings[s]
}

type Host interface {
	Info() HostInfo
	SetState(s HostState) Host
}

var (
	_ Host = &HostInfo{}
	_ Host = &WeightHostInfo{}
)

type HostInfo struct {
	Address  string
	Database string

	State HostState
}

func (h HostInfo) SetState(s HostState) Host {
	h.State = s
	return h
}

func (h HostInfo) Info() HostInfo {
	return h
}

func (h HostInfo) ID() string {
	return h.Address + h.Database
}

func (h HostInfo) String() string {
	return fmt.Sprintf("Host[addr: %s; db: %s]", h.Address, h.Database)
}

// Creates HostInfo with HostUp state.
func NewHostInfo(hostname, database string) HostInfo {
	return NewHostInfoWithState(hostname, database, HostUp)
}

func NewHostInfoWithState(hostname, database string, state HostState) HostInfo {
	return HostInfo{
		Address:  hostname,
		Database: database,
		State:    state,
	}
}

type WeightHostInfo struct {
	HostInfo

	Weight uint32
}

func (h WeightHostInfo) Info() HostInfo {
	return h.HostInfo
}

func (h WeightHostInfo) SetState(s HostState) Host {
	h.HostInfo.State = s
	return h
}

// Creates WeightHostInfo with HostUp state.
func NewWeightHostInfo(hostname, database string, weight uint32) WeightHostInfo {
	return NewWeightHostInfoWithState(hostname, database, weight, HostUp)
}

func NewWeightHostInfoWithState(hostname, database string, weight uint32, state HostState) WeightHostInfo {
	return WeightHostInfo{
		HostInfo: NewHostInfoWithState(hostname, database, state),
		Weight:   weight,
	}
}
