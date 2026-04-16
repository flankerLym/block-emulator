package committee

type RLAction struct {
	ActionID   int                    `json:"action_id"`
	ActionName string                 `json:"action_name"`
	ShardID    int                    `json:"shard_id"`
	Params     map[string]interface{} `json:"params"`
}
