// 统一日志：带时间戳和模块标记，如 [gateway] / [login]。
// 基于标准库 log，够用就行，不做日志分割。
package common

import (
	"log"
	"os"
)

// NewLogger 创建一个带模块标记的日志器。
// 输出形如：2026/10/25 10:30:00 [gateway] 客户端连上：127.0.0.1:1234
func NewLogger(module string) *log.Logger {
	return log.New(os.Stdout, "["+module+"] ", log.LstdFlags|log.Lmsgprefix)
}
