// 战斗服务：监听网关连接，处理 2000+ 战斗消息。
package battle

import (
	"bytes"
	"net"

	"google.golang.org/protobuf/proto"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/api"
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
)

const listenAddr = ":9998"

var logger = common.NewLogger("battle")

// 回包错误码
const (
	codeOK          = 0
	codeBadParam    = 1 // 参数缺失/格式错误
	codeServerError = 3 // 服务器内部错误
)

// Run 启动战斗服务，监听网关连接
func Run() {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatal("战斗服务监听失败:", err)
	}
	logger.Printf("battle服务启动，监听：%s", listenAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleGatewayConn(conn)
	}
}

// 处理来自网关的连接：解码、按 msgId 分发、回包
func handleGatewayConn(conn net.Conn) {
	defer conn.Close()
	logger.Println("网关接入battle服务")
	readBuf := make([]byte, 4096)
	cache := &bytes.Buffer{}

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			logger.Println("网关连接断开：", err)
			return
		}
		cache.Write(readBuf[:n])

		for {
			p, err := common.Decode(cache)
			if err != nil {
				logger.Println("battle decode err:", err)
				return
			}
			if p == nil {
				break
			}

			// 按 msgId 分发到具体处理函数
			var respMsgId uint16
			var resp proto.Message
			switch p.MessageID {
			case 2000:
				respMsgId, resp = 2001, handleEnterRoom(p.Payload)
			case 2002:
				respMsgId, resp = 2003, handleDeployOperator(p.Payload)
			default:
				logger.Printf("未知战斗消息 msgId=%d，丢弃", p.MessageID)
				continue
			}

			respBody, err := proto.Marshal(resp)
			if err != nil {
				logger.Println("序列化回包失败:", err)
				continue
			}
			// 把网关发来的 connID 原样填回去，网关才能靠它定位客户端
			respPkt, _ := common.Encode(respMsgId, p.ConnId, respBody)
			_, _ = conn.Write(respPkt)
		}
	}
}

// handleEnterRoom 进入/创建房间
func handleEnterRoom(payload []byte) *api.EnterRoomResp {
	var req api.EnterRoomReq
	if err := proto.Unmarshal(payload, &req); err != nil {
		logger.Println("解析 EnterRoomReq 失败:", err)
		return &api.EnterRoomResp{Code: codeBadParam, Msg: "请求格式错误"}
	}
	logger.Printf("收到进房请求：roomId=%d levelId=%d", req.RoomId, req.LevelId)
	// TODO: 房间创建/匹配逻辑
	return &api.EnterRoomResp{Code: codeOK, Msg: "ok", RoomId: req.RoomId}
}

// handleDeployOperator 部署干员
func handleDeployOperator(payload []byte) *api.DeployOperatorResp {
	var req api.DeployOperatorReq
	if err := proto.Unmarshal(payload, &req); err != nil {
		logger.Println("解析 DeployOperatorReq 失败:", err)
		return &api.DeployOperatorResp{Code: codeBadParam, Msg: "请求格式错误"}
	}
	logger.Printf("收到部署请求：roomId=%d opId=%d (%d,%d) dir=%d",
		req.RoomId, req.OperatorId, req.GridX, req.GridY, req.Direction)
	// TODO: 费用校验、部署合法性校验、写入战斗状态
	return &api.DeployOperatorResp{Code: codeOK, Msg: "ok", OperatorId: req.OperatorId}
}
