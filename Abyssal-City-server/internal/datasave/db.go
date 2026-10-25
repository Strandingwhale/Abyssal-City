// 数据库连接池：datasave 服务及其他需要写库的模块统一从这里拿 *sql.DB。
package datasave

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL 驱动，注册到 database/sql
)

// InitDB 打开 MySQL 连接池并验证连通性。
// dsn 形如：root:root123@tcp(127.0.0.1:3306)/abyssal_city?parseTime=true
func InitDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	// 连接池参数：够用就行
	db.SetMaxOpenConns(20)           // 最多同时打开的连接数
	db.SetMaxIdleConns(10)           // 空闲时保留的连接数，避免频繁建连
	db.SetConnMaxLifetime(time.Hour) // 连接最长存活时间，防 MySQL 服务端超时踢掉

	// Open 只是配置，Ping 才真正建立连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}
