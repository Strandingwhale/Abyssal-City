// 启动 TCP 监听，接受客户端连接；并维护与 login 服务的连接、回包转发。
package gateway

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

const heartbeatTimeout = 10 * time.Second
const loginAddr = "127.0.0.1:9999"

// 客户端上下文
type ClientSession struct {
	conn net.Conn
}

var (
	loginConn net.Conn
	clientMap = make(map[net.Conn]*ClientSession)
	mapLock   sync.RWMutex
)

// Run 连接 login 服务并启动网关 TCP 监听
func Run() {
	go connectLogin()

	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	fmt.Println("网关启动：监听：8888")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn)
	}
}

// 连接login服务，并且启动协程读取login的回包，原路转发给客户端
func connectLogin() {
	for {
		conn, err := net.Dial("tcp", loginAddr)
		if err != nil {
			fmt.Printf("连接login失败，3s后重连: %v\n", err)
			time.Sleep(3 * time.Second)
			continue
		}
		loginConn = conn
		fmt.Println("网关成功连上login服务")
		go readLoginResp(conn)
		return
	}
}

// 读取login返回的包，转发给客户端（这里简化：login回包直接广播，后面我再加uid绑定）
func readLoginResp(conn net.Conn) {
	defer func() {
		loginConn = nil
		_ = conn.Close()
		go connectLogin()
	}()
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{}
	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			fmt.Println("网关和login连接断开：", err)
			return
		}
		cache.Write(readBuf[:n])
		for {
			p, err := common.Decode(cache)
			if err != nil {
				fmt.Println("gateway decode login resp err:", err)
				return
			}
			if p == nil {
				break
			}

			fmt.Printf("login返回 msgId=%d, data=%s\n", p.MessageID, string(p.Payload))

			mapLock.RLock()
			for _, cli := range clientMap {
				pkt, _ := common.Encode(p.MessageID, p.Payload)
				_, _ = cli.conn.Write(pkt)
			}
			mapLock.RUnlock()

		}
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	fmt.Println("客户端连上：", conn.RemoteAddr())
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{} //接收缓冲区，解决"半包"和"粘包"问题。
	ticker := time.NewTicker(heartbeatTimeout)
	defer ticker.Stop()

	recvCh := make(chan struct{}, 1)
	remote := conn.RemoteAddr()

	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("连接 %v 心跳超时，断开\n", remote)
				_ = conn.Close()
				return
			case <-recvCh:
				ticker.Reset(heartbeatTimeout)
			}
		}

	}()

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			fmt.Println("链接关闭，err：", err)
			return
		}

		// 收到数据，通知重置心跳计时
		select {
		case recvCh <- struct{}{}:
		default:
		}

		cache.Write(readBuf[:n])

		for {
			p, err := common.Decode(cache)
			if err != nil {
				fmt.Println("decode err:", err)
				return
			}
			// 半包：数据还不够，等下一次读取
			if p == nil {
				break
			}

			if p.MessageID == 0 {
				fmt.Printf("收到心跳 %v\n", remote)
			} else if p.MessageID >= 1000 && p.MessageID <= 1999 {
				fmt.Printf("转发登录消息 msgId=%d, payload=%s\n", p.MessageID, string(p.Payload))
				if loginConn != nil {
					pkt, _ := common.Encode(p.MessageID, p.Payload)
					_, _ = loginConn.Write(pkt)
				}
			} else {
				fmt.Printf("收到包 msg=%d,payload=%s\n", p.MessageID, string(p.Payload))
			}
		}
	}
}
