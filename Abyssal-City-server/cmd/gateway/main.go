// 网关服务入口：启动 TCP 监听并维持与 login 服务的连接
package main

import (
	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/internal/gateway"
)

func main() {
	gateway.Run()
}
