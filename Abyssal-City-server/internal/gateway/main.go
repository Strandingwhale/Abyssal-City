// 初始化日志、启动网关 TCP 服务
package gateway

import (
	"bytes"
	"fmt"
	"net"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

func handleConn(conn net.Conn) {
	defer conn.Close()
	fmt.Println("客户端连上：", conn.RemoteAddr())
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{}

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			fmt.Println("链接关闭，err：", err)
			return
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
			fmt.Printf("收到包 msg=%d,payload=%s\n", p.MessageID, string(p.Payload))
		}
	}
}

func main() {
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
