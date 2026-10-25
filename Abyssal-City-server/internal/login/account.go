package main

import (
	"bytes"
	"database/sql"
	"errors"
	"net"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/api"
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/common"
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/datasave"
)

var logger = common.NewLogger("login")

var db *sql.DB

// 回包错误码
const (
	codeOK          = 0
	codeBadParam    = 1 // 参数缺失/格式错误
	codeAccountDup  = 2 // 账号已存在
	codeServerError = 3 // 服务器内部错误
)

func handleGatewayConn(conn net.Conn) {
	defer conn.Close()
	logger.Println("网关接入login服务")
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
				logger.Println("login decode err:", err)
				return
			}
			if p == nil {
				break
			}

			// 按 msgId 分发到具体处理函数
			var respMsgId uint16
			var resp proto.Message
			switch p.MessageID {
			case 1001:
				respMsgId, resp = 1002, handleLogin(p.Payload)
			case 1003:
				respMsgId, resp = 1004, handleRegister(p.Payload)
			default:
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

// handleLogin 校验账号密码（临时实现：账号 test 直接放行，3c 再改为查库+bcrypt 校验）
func handleLogin(payload []byte) *api.LoginResp {
	var req api.LoginReq
	if err := proto.Unmarshal(payload, &req); err != nil {
		logger.Println("解析 LoginReq 失败:", err)
		return &api.LoginResp{Code: codeBadParam, Msg: "请求格式错误"}
	}
	logger.Printf("收到登录请求：account=%s", req.Account)

	if req.Account == "test" {
		return &api.LoginResp{Code: codeOK, Msg: "ok", Uid: 10086, Token: "token-10086", SessionId: "session-10086"}
	}
	return &api.LoginResp{Code: codeBadParam, Msg: "账号密码错误"}
}

// handleRegister 注册：bcrypt 哈希密码后入库，绝不存明文
func handleRegister(payload []byte) *api.RegisterResp {
	var req api.RegisterReq
	if err := proto.Unmarshal(payload, &req); err != nil {
		logger.Println("解析 RegisterReq 失败:", err)
		return &api.RegisterResp{Code: codeBadParam, Msg: "请求格式错误"}
	}
	if req.Account == "" || req.Password == "" {
		return &api.RegisterResp{Code: codeBadParam, Msg: "账号或密码不能为空"}
	}

	// bcrypt 哈希：每次结果不同（自带随机盐），相同密码也无法从哈希看出
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		logger.Println("bcrypt 哈希失败:", err)
		return &api.RegisterResp{Code: codeServerError, Msg: "服务器繁忙"}
	}

	_, err = db.Exec("INSERT INTO account (account, password_hash) VALUES (?, ?)", req.Account, string(hash))
	if err != nil {
		// 唯一索引冲突 = 账号已存在
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1062 {
			return &api.RegisterResp{Code: codeAccountDup, Msg: "账号已存在"}
		}
		logger.Println("插入账号失败:", err)
		return &api.RegisterResp{Code: codeServerError, Msg: "服务器繁忙"}
	}

	logger.Printf("注册成功：account=%s", req.Account)
	return &api.RegisterResp{Code: codeOK, Msg: "ok"}
}

func main() {
	var err error
	db, err = datasave.InitDB("root:root123@tcp(127.0.0.1:3306)/abyssal_city?parseTime=true")
	if err != nil {
		logger.Fatal("数据库连接失败：", err)
	}
	logger.Println("数据库连接成功")

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		panic(err)
	}
	logger.Println("login服务启动，监听：9999")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleGatewayConn(conn)
	}
}
