// 启动 TCP 监听，接受客户端连接；并维护与各后端服务的连接、回包转发。
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

const (
	heartbeatTimeout  = 10 * time.Second
	reconnectInterval = 3 * time.Second

	loginAddr  = "127.0.0.1:9999"
	battleAddr = "127.0.0.1:9998"
)

var (
	logger    = common.NewLogger("gateway")
	clientMap = make(map[uint64]*Session) // connID → 会话
	mapLock   sync.RWMutex
	connSeq   atomic.Uint64 // 网关内自增的 connID，供后端回包定向投递

	// 两个后端：login（1000-1999）与 battle（2000+）
	loginBackend  = newBackend("login", loginAddr, handleLoginResp)
	battleBackend = newBackend("battle", battleAddr, forwardToClient)
)

// Run 连接后端服务并启动网关 TCP 监听
func Run() {
	loginBackend.start()  //后端回包处理
	battleBackend.start() //后端回包处理

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
		go handleConn(conn) //客户端发送请求处理
	}
}

// backend 封装到单个后端服务的连接：自动重连 + 读循环 + 按 connID 回包
type backend struct {
	name  string
	addr  string
	conn  atomic.Pointer[net.Conn] // 当前连接，重连时整体置换
	onPkt func(*common.Packet)     // 收到回包时的处理回调
}

func newBackend(name, addr string, onPkt func(*common.Packet)) *backend {
	return &backend{name: name, addr: addr, onPkt: onPkt}
}

// start 异步启动连接与重连循环
func (b *backend) start() {
	go b.connectLoop()
}

// connectLoop 不断尝试连接后端；连接断开后自动重连
func (b *backend) connectLoop() {
	for {
		conn, err := net.Dial("tcp", b.addr)
		if err != nil {
			logger.Printf("连接%s失败，%v后重连: %v", b.name, reconnectInterval, err)
			time.Sleep(reconnectInterval)
			continue
		}
		b.conn.Store(&conn)
		logger.Printf("网关成功连上%s服务", b.name)
		b.readLoop(conn) // 连接断开后返回，继续下一轮重连
	}
}

// readLoop 持续读取后端回包并交给 onPkt 处理
func (b *backend) readLoop(conn net.Conn) {
	defer func() {
		b.conn.Store(nil)
		_ = conn.Close()
	}()
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{}
	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			logger.Printf("网关和%s连接断开：%v", b.name, err)
			return
		}
		cache.Write(readBuf[:n])
		for {
			p, err := common.Decode(cache)
			if err != nil {
				logger.Printf("decode %s resp err: %v", b.name, err)
				return
			}
			if p == nil {
				break
			}
			logger.Printf("%s返回 msgId=%d, connID=%d, data=%s", b.name, p.MessageID, p.ConnId, string(p.Payload))
			b.onPkt(p)
		}
	}
}

// send 把包发给后端；连接未就绪时返回 false（调用方决定是否丢弃）
func (b *backend) send(pkt []byte) bool {
	if c := b.conn.Load(); c != nil {
		_, err := (*c).Write(pkt)
		return err == nil
	}
	return false
}

// forwardToClient 通用回包处理：按 connID 定向投递给对应客户端
func forwardToClient(p *common.Packet) {
	mapLock.RLock()
	target := clientMap[p.ConnId]
	mapLock.RUnlock()

	if target == nil {
		logger.Printf("找不到 connID=%d 的客户端，回包丢弃", p.ConnId)
		return
	}

	pkt, _ := common.Encode(p.MessageID, p.ConnId, p.Payload)
	_ = target.WriteMsg(pkt)
}

// handleLoginResp 登录回包处理：先按 connID 投递，登录成功时额外把 uid 绑定到会话
func handleLoginResp(p *common.Packet) {
	mapLock.RLock()
	target := clientMap[p.ConnId]
	mapLock.RUnlock()

	if target == nil {
		logger.Printf("找不到 connID=%d 的客户端，回包丢弃", p.ConnId)
		return
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

			switch {
			case p.MessageID == 0:
				// 心跳闭环：解出 PingReq，回 PingResp（msgId=1），时间戳原样带回
				var req api.PingReq
				if err := proto.Unmarshal(p.Payload, &req); err != nil {
					logger.Println("解析 PingReq 失败:", err)
					continue
				}
				respBody, err := proto.Marshal(&api.PingResp{Timestamp: req.Timestamp})
				if err != nil {
					logger.Println("序列化 PingResp 失败:", err)
					continue
				}
				pkt, _ := common.Encode(1, session.ConnID(), respBody)
				if err := session.WriteMsg(pkt); err != nil {
					logger.Println("回 Pong 失败:", err)
					return
				}

			case p.MessageID >= 1000 && p.MessageID <= 1999:
				// 登录消息 → login 服务
				logger.Printf("转发登录消息 msgId=%d", p.MessageID)
				forwardToBackend(loginBackend, p, session.ConnID())

			case p.MessageID >= 2000:
				// 战斗消息 → battle 服务
				logger.Printf("转发战斗消息 msgId=%d", p.MessageID)
				forwardToBackend(battleBackend, p, session.ConnID())

			default:
				logger.Printf("收到包 msg=%d,payload=%s", p.MessageID, string(p.Payload))
			}
		}
	}
}

// forwardToBackend 用网关本地 connID 覆盖客户端发来的值，转发给指定后端
func forwardToBackend(b *backend, p *common.Packet, connID uint64) {
	// 用网关本地 connID 覆盖，后端回包才能原路返回
	pkt, _ := common.Encode(p.MessageID, connID, p.Payload)
	if !b.send(pkt) {
		logger.Printf("后端 %s 未连接，msgId=%d 丢弃", b.name, p.MessageID)
	}
}
