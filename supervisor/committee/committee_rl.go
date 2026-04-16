package committee

import (
	"fmt"
	"log"

	"blockEmulator/supervisor"
)

func ApplyRLAction(act supervisor.RLAction) error {
	// 冷却期内，除 noop 外不执行重配置类动作
	if supervisor.GlobalRLRuntime.InCooldown() && act.ActionName != "noop" {
		log.Printf("[RL] in cooldown, ignore action=%s\n", act.ActionName)
		return nil
	}

	switch act.ActionName {
	case "noop":
		log.Printf("[RL] noop")
		return nil

	case "split_shard":
		return ExecuteShardScaleOut(act)

	case "merge_shard":
		return ExecuteShardScaleIn(act)

	case "trigger_clpa":
		return ExecuteCLPARepartition(act)

	case "enable_broker":
		supervisor.GlobalRLRuntime.SetBrokerEnabled(true)
		supervisor.GlobalRLRuntime.SetRelayEnabled(false)
		log.Printf("[RL] broker enabled")
		return nil

	case "enable_relay":
		supervisor.GlobalRLRuntime.SetRelayEnabled(true)
		supervisor.GlobalRLRuntime.SetBrokerEnabled(false)
		log.Printf("[RL] relay enabled")
		return nil

	case "enter_cooldown":
		rounds := 3
		if v, ok := act.Params["cooldown_rounds"]; ok {
			if x, ok2 := v.(float64); ok2 {
				rounds = int(x)
			}
		}
		supervisor.GlobalRLRuntime.SetCooldown(rounds)
		log.Printf("[RL] cooldown for %d s", rounds)
		return nil

	default:
		return fmt.Errorf("unknown action: %s", act.ActionName)
	}
}
