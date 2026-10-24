package main

import (
	"bytes"
	"fmt"
	"net"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

func handleGatewayConn(conn net.Conn) {
	defer conn.Close()
	fmt.Println("网关接入login服务")
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{}

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			fmt.Println("网关连接断开：", err)
			return
		}
		cache.Write(readBuf[:n])

		for {
			p, err := common.Decode(cache)
			if err != nil {
				fmt.Println("login decode err:", err)
				return
			}
			if p == nil {
				break
			}

			var respMsgId uint16
			var respBody []byte
			if p.MessageID == 1001 {
				body := string(p.Payload)
				fmt.Printf("收到登录请求：%s\n", body)
				if body == "test" {
					respMsgId = 1002
					respBody = []byte("ok:session-10086")
				} else {
					respMsgId = 1002
					respBody = []byte("err:账号密码错误")
				}

				// 把网关发来的 connID 原样填回去，网关才能靠它定位客户端
				respPkt, _ := common.Encode(respMsgId, p.ConnId, respBody)
				_, _ = conn.Write(respPkt)
			}
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		panic(err)
	}
	fmt.Println("login服务启动，监听：9999")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleGatewayConn(conn)
	}
}
