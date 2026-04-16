package committee

import "blockEmulator/message"

type CommitteeModule interface {
	HandleBlockInfo(*message.BlockInfoMsg)
	MsgSendingControl()
	HandleOtherMessage([]byte)

	// RL 统一动作入口
	ApplyRLAction(RLAction) error
}
