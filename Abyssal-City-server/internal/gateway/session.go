// Session 结构体，保存每一个客户端连接信息：sessionId、玩家 ID、连接、最后心跳时间。
package gateway

import (
	"net"
	"sync"
	"time"
)

type Session struct {
	conn       net.Conn
	remoteAddr string    //客户端地址
	readCache  []byte    //拆包缓存（处理粘包半包）
	lastRecvAt time.Time // 最后收到消息时间，用于心跳超时
	sessionID  string    // 登录成功后分配的会话id
	uid        uint64    // 用户id，登录成功后赋值
	closeOnce  sync.Once
	closeChan  chan struct{}
	mu         sync.Mutex
}

// NewSession 创建新会话
func NewSession(conn net.Conn) *Session {
	return &Session{
		conn:       conn,
		remoteAddr: conn.RemoteAddr().String(),
		readCache:  make([]byte, 0, 4096),
		lastRecvAt: time.Now(),
		closeChan:  make(chan struct{}),
	}
}

// UpdateRecvTime 更新最后接收时间（收到任何包的时候调用，重置心跳计时）
func (s *Session) UpdateRecvTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRecvAt = time.Now()
}

// IsTimeout 判断会话是否心跳超时
func (s *Session) IsTimeout(timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastRecvAt) > timeout
}

// WriteMsg 给客户端发协议包
func (s *Session) WriteMsg(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.conn.Write(data)
	return err
}

// Close 关闭会话
func (s *Session) Close() {
	s.closeOnce.Do(func() {
		_ = s.conn.Close()
		close(s.closeChan)
	})
}

// Closed 返回关闭信号通道
func (s *Session) Closed() <-chan struct{} {
	return s.closeChan
}
