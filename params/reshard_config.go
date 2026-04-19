package params

import (
	"encoding/json"
	"os"

	"blockEmulator/partition/reshard"
)

type RootConfig struct {
	Reshard reshard.ReshardConfig `json:"reshard"`
}

func LoadReshardConfigFromJSON(path string) (reshard.ReshardConfig, error) {
	var root RootConfig

	data, err := os.ReadFile(path)
	if err != nil {
		return reshard.ReshardConfig{}, err
	}
	if err := json.Unmarshal(data, &root); err != nil {
		return reshard.ReshardConfig{}, err
	}

	applyReshardDefaults(&root.Reshard)
	return root.Reshard, nil
}

func applyReshardDefaults(cfg *reshard.ReshardConfig) {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 100
	}
	if cfg.TriggerThreshold <= 0 {
		cfg.TriggerThreshold = 0.65
	}
	if cfg.AlphaLoad == 0 {
		cfg.AlphaLoad = 0.35
	}
	if cfg.AlphaCross == 0 {
		cfg.AlphaCross = 0.30
	}
	if cfg.AlphaHot == 0 {
		cfg.AlphaHot = 0.20
	}
	if cfg.AlphaRisk == 0 {
		cfg.AlphaRisk = 0.15
	}

	if cfg.GammaCross == 0 {
		cfg.GammaCross = 0.40
	}
	if cfg.GammaBalance == 0 {
		cfg.GammaBalance = 0.30
	}
	if cfg.GammaMig == 0 {
		cfg.GammaMig = 0.20
	}
	if cfg.GammaRisk == 0 {
		cfg.GammaRisk = 0.10
	}

	if cfg.HotThreshold == 0 {
		cfg.HotThreshold = 0.20
	}
	if cfg.MaxCandidates <= 0 {
		cfg.MaxCandidates = 100
	}
	if cfg.MaxMoves <= 0 {
		cfg.MaxMoves = 20
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 0.20
	}
}
