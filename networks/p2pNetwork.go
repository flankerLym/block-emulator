package networks

import (
	"blockEmulator/params"
	"bytes"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

var connMaplock sync.Mutex
var connectionPool = make(map[string]net.Conn, 0)

var randomDelayGenerator *rand.Rand
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

func InitNetworkTools() {
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

func TcpDial(context []byte, addr string) {
	go func() {
		thisDelay := params.Delay
		if params.JitterRange != 0 {
			thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
		}
		time.Sleep(time.Millisecond * time.Duration(thisDelay))

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Println("Connect error", err)
			return
		}
		defer conn.Close()

		writeToConn(append(context, '\n'), conn, rateLimiterUpload)
	}()
}

func Broadcast(sender string, receivers []string, msg []byte) {
	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		go TcpDial(msg, ip)
	}
}

func CloseAllConnInPool() {
	connMaplock.Lock()
	defer connMaplock.Unlock()

	for _, conn := range connectionPool {
		conn.Close()
	}
	connectionPool = make(map[string]net.Conn)
}

func ReadFromConn(addr string) {
	conn := connectionPool[addr]

	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		messageBuffer.Write(buffer[:n])

		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				break
			} else if err == nil {
				log.Println("Received from", addr, ":", message)
			} else {
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
