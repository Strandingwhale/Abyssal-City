// 双客户端串包检测：两个客户端分别发不同账号的登录请求，
// 校验回包内容、connID 是否与各自请求一一对应，且不多收包。
package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

type result struct {
	pkt     *common.Packet
	extra   int // 窗口期内多收的包数
	readErr error
}

// 登录并读取回包：connID 传 0（由网关覆盖），读窗口 2 秒
func loginOnce(addr, account string) result {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return result{readErr: err}
	}
	defer conn.Close()

	pkt, err := common.Encode(1001, 0, []byte(account))
	if err != nil {
		return result{readErr: err}
	}
	if _, err := conn.Write(pkt); err != nil {
		return result{readErr: err}
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	cache := &bytes.Buffer{}
	buf := make([]byte, 4096)
	var first *common.Packet
	extra := 0
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break // 读超时或断开：本轮收集结束
		}
		cache.Write(buf[:n])
		for {
			p, err := common.Decode(cache)
			if err != nil {
				return result{readErr: err}
			}
			if p == nil {
				break
			}
			if first == nil {
				first = p
			} else {
				extra++
			}
		}
	}
	return result{pkt: first, extra: extra}
}

func main() {
	const addr = "127.0.0.1:8888"

	// 两个客户端几乎同时登录：A 用正确账号，B 用错误账号
	chA := make(chan result, 1)
	chB := make(chan result, 1)
	go func() { chA <- loginOnce(addr, "test") }()  // A 应收到 ok
	go func() { chB <- loginOnce(addr, "wrong") }() // B 应收到 err

	ra := <-chA
	rb := <-chB

	fail := false
	check := func(name string, r result, wantPayload string) {
		if r.readErr != nil {
			fmt.Printf("[FAIL] 客户端%s 读写错误: %v\n", name, r.readErr)
			fail = true
			return
		}
		if r.pkt == nil {
			fmt.Printf("[FAIL] 客户端%s 未收到任何回包\n", name)
			fail = true
			return
		}
		fmt.Printf("[INFO] 客户端%s 收到 msgId=%d connID=%d payload=%q\n",
			name, r.pkt.MessageID, r.pkt.ConnId, string(r.pkt.Payload))
		if string(r.pkt.Payload) != wantPayload {
			fmt.Printf("[FAIL] 客户端%s 回包串了：期望 %q，实际 %q\n",
				name, wantPayload, string(r.pkt.Payload))
			fail = true
		}
		if r.extra > 0 {
			fmt.Printf("[FAIL] 客户端%s 多收了 %d 个包（疑似广播串包）\n", name, r.extra)
			fail = true
		}
	}
	check("A", ra, "ok:session-10086")
	check("B", rb, "err:账号密码错误")

	if ra.pkt != nil && rb.pkt != nil && ra.pkt.ConnId == rb.pkt.ConnId {
		fmt.Printf("[FAIL] 两个客户端 connID 相同（%d），定向投递无法区分\n", ra.pkt.ConnId)
		fail = true
	}

	if fail {
		fmt.Println("===> 串包检测：FAIL")
		os.Exit(1)
	}
	fmt.Println("===> 串包检测：PASS（A、B 回包各自正确且无多收）")
}
