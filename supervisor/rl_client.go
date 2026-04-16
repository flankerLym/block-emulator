package supervisor

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

func SendStateToPython(addr string, state RLState) error {
	conn, err := net.DialTimeout("tcp", addr, 800*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()

	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}

	req := fmt.Sprintf(
		"POST /state HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		addr, len(payload), payload,
	)

	_ = conn.SetDeadline(time.Now().Add(800 * time.Millisecond))
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}

	// 可选：读一点响应，避免部分环境下半关闭异常
	buf := make([]byte, 256)
	_, _ = conn.Read(buf)

	return nil
}

func MustPushStateAsync(addr string, state RLState) {
	go func() {
		if err := SendStateToPython(addr, state); err != nil {
			log.Printf("[RL] push state failed: %v\n", err)
		}
	}()
}
