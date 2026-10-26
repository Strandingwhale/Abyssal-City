// 战斗服务入口：监听网关连接并分发 2000+ 战斗消息
package main

import (
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/battle"
)

func main() {
	battle.Run()
}
