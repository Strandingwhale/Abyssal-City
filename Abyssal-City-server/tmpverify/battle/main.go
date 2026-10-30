// 战斗链路验证客户端：进房 → 部署 → 收状态推送 → 收结算
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/api"
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

func main() {
	deploy := flag.Bool("deploy", true, "是否部署干员")
	flag.Parse()

	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		fmt.Println("连接网关失败:", err)
		return
	}
	defer conn.Close()

	// 2000 进房
	body, _ := proto.Marshal(&api.EnterRoomReq{LevelId: 1})
	write(conn, 2000, body)

	cache := &bytes.Buffer{}
	roomID := uint32(0)
	deployed := false
	deadline := time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if n > 0 {
			cache.Write(buf[:n])
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			fmt.Println("读失败:", err)
			return
		}

		for {
			p, err := common.Decode(cache)
			if err != nil {
				fmt.Println("decode err:", err)
				return
			}
			if p == nil {
				break
			}
			switch p.MessageID {
			case 2001:
				var resp api.EnterRoomResp
				proto.Unmarshal(p.Payload, &resp)
				roomID = resp.RoomId
				fmt.Printf("[进房] code=%d roomId=%d seed=%d\n", resp.Code, resp.RoomId, resp.Seed)
			case 2003:
				var resp api.DeployOperatorResp
				proto.Unmarshal(p.Payload, &resp)
				fmt.Printf("[部署] code=%d msg=%s remaining_cost=%d\n", resp.Code, resp.Msg, resp.RemainingCost)
			case 2004:
				var push api.BattleStatePush
				proto.Unmarshal(p.Payload, &push)
				fmt.Printf("[状态] frame=%d cost=%d baseHP=%d 干员=%d 敌人=%d",
					push.Frame, push.Cost, push.BaseHp, len(push.Operators), len(push.Enemies))
				for _, e := range push.Enemies {
					fmt.Printf(" | enemy hp=%d pos=(%.2f,%.2f)", e.Hp, e.PosX, e.PosY)
				}
				fmt.Println()

				// 收到第一帧后再部署，确保房间已创建
				if *deploy && !deployed && roomID != 0 {
					deployed = true
					d, _ := proto.Marshal(&api.DeployOperatorReq{
						RoomId: roomID, OperatorId: 1, GridX: 4, GridY: 0, Direction: 3,
					})
					write(conn, 2002, d)
				}
			case 2005:
				var settle api.BattleSettlePush
				proto.Unmarshal(p.Payload, &settle)
				fmt.Printf("[结算] result=%d score=%d duration=%ds rewards=%v\n",
					settle.Result, settle.Score, settle.DurationSec, settle.Rewards)
				if settle.Result == 0 {
					fmt.Println("结论：胜利")
				} else {
					fmt.Println("结论：失败")
				}
				return
			}
		}
	}
	fmt.Println("超时，未收到结算")
}

func write(conn net.Conn, msgID uint16, body []byte) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(len(body)))
	binary.Write(&buf, binary.BigEndian, msgID)
	binary.Write(&buf, binary.BigEndian, uint64(0))
	buf.Write(body)
	conn.Write(buf.Bytes())
}
