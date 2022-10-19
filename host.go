package chdistr

type NodeState uint32

const (
	NodeUp NodeState = iota
	NodeDown
)

var nodeStateStrings = [...]string{"NODE_UP", "NODE_DOWN"}

func (s NodeState) String() string {
	return nodeStateStrings[s]
}

type Node interface {
	Info() HostInfo
	SetState(s NodeState) Node
}

var (
	_ Node = &HostInfo{}
	_ Node = &WeightHostInfo{}
)

type HostInfo struct {
	hostname string
	state    NodeState
}

func (h *HostInfo) Hostname() string {
	return h.hostname
}

func (h *HostInfo) State() NodeState {
	return h.state
}

func (h HostInfo) SetState(s NodeState) Node {
	h.state = s
	return h
}

func (h HostInfo) Info() HostInfo {
	return h
}

// Creates HostInfo with NodeUp state.
func NewHostInfo(hostname string) HostInfo {
	return NewHostInfoWithState(hostname, NodeUp)
}

func NewHostInfoWithState(hostname string, state NodeState) HostInfo {
	return HostInfo{
		hostname: hostname,
		state:    state,
	}
}

type WeightHostInfo struct {
	HostInfo

	weight uint32
}

func (h WeightHostInfo) Info() HostInfo {
	return h.HostInfo
}

func (h WeightHostInfo) SetState(s NodeState) Node {
	h.HostInfo.state = s
	return h
}

func (h *WeightHostInfo) Weight() uint32 {
	return h.weight
}

// Creates WeightHostInfo with NodeUp state.
func NewWeightHostInfo(hostname string, weight uint32) WeightHostInfo {
	return NewWeightHostInfoWithState(hostname, weight, NodeUp)
}

func NewWeightHostInfoWithState(hostname string, weight uint32, state NodeState) WeightHostInfo {
	return WeightHostInfo{
		HostInfo: NewHostInfoWithState(hostname, state),
		weight:   weight,
	}
}
