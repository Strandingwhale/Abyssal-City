// 登录联调客户端：连网关发 1001 登录包，等待并打印回包。
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

func main() {
	account := "test"
	if len(os.Args) > 1 {
		account = os.Args[1]
	}

	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		fmt.Println("连接网关失败:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("已连上网关 127.0.0.1:8888")

	pkt, err := common.Encode(1001, 0, []byte(account))
	if err != nil {
		fmt.Println("编码失败:", err)
		os.Exit(1)
	}
	if _, err := conn.Write(pkt); err != nil {
		fmt.Println("发送登录请求失败:", err)
		os.Exit(1)
	}
	fmt.Printf("已发送登录请求 msgId=1001 payload=%q，等待回包...\n", account)

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	cache := &bytes.Buffer{}
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("读取失败/超时:", err)
			os.Exit(1)
		}
		cache.Write(buf[:n])
		p, err := common.Decode(cache)
		if err != nil {
			fmt.Println("解码失败:", err)
			os.Exit(1)
		}
		if p == nil {
			continue // 半包，继续读
		}
		fmt.Printf("收到回包 msgId=%d connID=%d payload=%q\n", p.MessageID, p.ConnId, string(p.Payload))
		return
	}
}
