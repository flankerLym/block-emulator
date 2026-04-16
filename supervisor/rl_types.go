package supervisor

import (
	"sync"
	"time"
)

type RLAction struct {
	ActionID   int                    `json:"action_id"`
	ActionName string                 `json:"action_name"`
	ShardID    int                    `json:"shard_id"`
	Params     map[string]interface{} `json:"params"`
}

type RLState struct {
	ShardID        int     `json:"shard_id"`
	Load           float64 `json:"load"`
	TPS            float64 `json:"tps"`
	CrossRatio     float64 `json:"cross_ratio"`
	InnerTx        float64 `json:"inner_tx"`
	CrossTx        float64 `json:"cross_tx"`
	TCL            float64 `json:"tcl"`
	Imbalance      float64 `json:"imbalance"`
	ShardNum       int     `json:"shard_num"`
	BrokerRatio    float64 `json:"broker_ratio"`
	RelayRatio     float64 `json:"relay_ratio"`
	RecentReconfig float64 `json:"recent_reconfig"`
}

type RLRuntime struct {
	mu             sync.RWMutex
	BrokerEnabled  bool
	RelayEnabled   bool
	CooldownUntil  time.Time
	LastAction     string
	LastActionTime time.Time
}

var GlobalRLRuntime = &RLRuntime{}

func (r *RLRuntime) InCooldown() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Now().Before(r.CooldownUntil)
}

func (r *RLRuntime) SetCooldown(roundSeconds int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.CooldownUntil = time.Now().Add(time.Duration(roundSeconds) * time.Second)
	r.LastAction = "enter_cooldown"
	r.LastActionTime = time.Now()
}

func (r *RLRuntime) SetBrokerEnabled(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.BrokerEnabled = v
	r.LastAction = "enable_broker"
	r.LastActionTime = time.Now()
}

func (r *RLRuntime) SetRelayEnabled(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.RelayEnabled = v
	r.LastAction = "enable_relay"
	r.LastActionTime = time.Now()
}
