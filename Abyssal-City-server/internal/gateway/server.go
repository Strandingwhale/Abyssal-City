// 启动 TCP 监听，接受客户端连接；并维护与 login 服务的连接、回包转发。
package gateway

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/api"
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

const heartbeatTimeout = 10 * time.Second
const loginAddr = "127.0.0.1:9999"

var (
	logger    = common.NewLogger("gateway")
	loginConn net.Conn
	clientMap = make(map[uint64]*Session) // connID → 会话
	mapLock   sync.RWMutex
	connSeq   atomic.Uint64 // 网关内自增的 connID，供 login 回包定向投递
)

// Run 连接 login 服务并启动网关 TCP 监听
func Run() {
	go connectLogin()

	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	logger.Println("网关启动：监听：8888")
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
			logger.Printf("连接login失败，3s后重连: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		loginConn = conn
		logger.Println("网关成功连上login服务")
		go readLoginResp(conn)
		return
	}
}

// 读取login返回的包，转发给客户端
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
			logger.Println("网关和login连接断开：", err)
			return
		}
		cache.Write(readBuf[:n])
		for {
			p, err := common.Decode(cache)
			if err != nil {
				logger.Println("decode login resp err:", err)
				return
			}
			if p == nil {
				break
			}

			logger.Printf("login返回 msgId=%d, connID=%d, data=%s", p.MessageID, p.ConnId, string(p.Payload))

			// 按 connID 定向投递：只写给对应的那个客户端，不再广播
			mapLock.RLock()
			target := clientMap[p.ConnId]
			mapLock.RUnlock()

			if target == nil {
				logger.Printf("找不到 connID=%d 的客户端，回包丢弃", p.ConnId)
				continue
			}

			// 登录成功时把 uid 绑定到会话，之后再透传给客户端
			if p.MessageID == 1002 {
				var resp api.LoginResp
				if err := proto.Unmarshal(p.Payload, &resp); err == nil && resp.Code == 0 {
					target.SetLogin(uint64(resp.Uid), resp.SessionId)
					logger.Printf("connID=%d 绑定 uid=%d", p.ConnId, resp.Uid)
				}
			}

			pkt, _ := common.Encode(p.MessageID, p.ConnId, p.Payload)
			_ = target.WriteMsg(pkt)

		}
	}
}

func handleConn(conn net.Conn) {
	// 为这条连接分配网关内唯一的 connID，并登记到 clientMap
	session := NewSession(conn, connSeq.Add(1))
	mapLock.Lock()
	clientMap[session.ConnID()] = session
	mapLock.Unlock()

	defer func() {
		mapLock.Lock()
		delete(clientMap, session.ConnID())
		mapLock.Unlock()
		session.Close()
	}()

	logger.Printf("客户端连上：%s (connID=%d)", session.remoteAddr, session.ConnID())

	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{} //接收缓冲区，解决"半包"和"粘包"问题。

	// 心跳监控：每秒检查一次是否超时；会话关闭后退出
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-session.Closed():
				return
			case <-ticker.C:
				if session.IsTimeout(heartbeatTimeout) {
					logger.Printf("连接 %s 心跳超时，断开", session.remoteAddr)
					session.Close()
					return
				}
			}
		}
	}()

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			logger.Println("链接关闭，err：", err)
			return
		}

		// 收到数据，刷新最后接收时间（心跳计时以此为准）
		session.UpdateRecvTime()

		cache.Write(readBuf[:n])

		for {
			p, err := common.Decode(cache)
			if err != nil {
				logger.Println("decode err:", err)
				return
			}
			// 半包：数据还不够，等下一次读取
			if p == nil {
				break
			}

			if p.MessageID == 0 {
				logger.Printf("收到心跳 %s", session.remoteAddr)
			} else if p.MessageID >= 1000 && p.MessageID <= 1999 {
				logger.Printf("转发登录消息 msgId=%d", p.MessageID)
				if loginConn != nil {
					// 用网关本地 connID 覆盖客户端发来的值，login 回包才能原路返回
					pkt, _ := common.Encode(p.MessageID, session.ConnID(), p.Payload)
					_, _ = loginConn.Write(pkt)
				}
			} else {
				logger.Printf("收到包 msg=%d,payload=%s", p.MessageID, string(p.Payload))
			}
		}
	}
}
