// 房间与战斗推进：所有位移、伤害、胜负判定都在服务端完成，客户端只收状态推送。
package battle

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Strandingwhale/Abyssal-City/Abyssal-City-server/api"
)

const tickInterval = 200 * time.Millisecond // 5 帧/秒

// 结算结果，对应 BattleSettlePush.result
const (
	resultWin  = 0 // 胜利
	resultLose = 1 // 失败
)

var (
	roomMu  sync.RWMutex
	rooms   = map[uint32]*Room{}
	roomSeq atomic.Uint32
)

// Operator 场上干员
type Operator struct {
	ID        uint32
	X, Y      uint32
	Direction int32
}

// Enemy 场上敌人
type Enemy struct {
	InstanceID uint64
	HP         int32
	// Dist 已沿路径行进的距离
	Dist float64
}

// Room 一场战斗。创建后 connID 与 send 不再变更，其余状态由 mu 保护。
type Room struct {
	ID     uint32
	Seed   uint64
	connID uint64
	send   func(msgID uint16, body []byte) // 推送给房间所属客户端

	mu        sync.Mutex
	frame     uint32
	cost      int32
	baseHP    int32
	operators []*Operator
	enemies   []*Enemy
	over      bool
	startedAt time.Time

	done chan struct{}
}

// createRoom 创建房间并立即开始 tick 循环
func createRoom(connID uint64, send func(msgID uint16, body []byte)) *Room {
	id := roomSeq.Add(1)
	r := &Room{
		ID:        id,
		Seed:      uint64(time.Now().UnixNano()),
		connID:    connID,
		send:      send,
		cost:      initCost,
		baseHP:    baseMaxHP,
		startedAt: time.Now(),
		done:      make(chan struct{}),
	}
	// 极简关卡：开打即生成一只怪
	r.enemies = []*Enemy{{InstanceID: 1, HP: enemyMaxHP}}

	roomMu.Lock()
	rooms[id] = r
	roomMu.Unlock()

	go r.run()
	return r
}

func getRoom(id uint32) *Room {
	roomMu.RLock()
	defer roomMu.RUnlock()
	return rooms[id]
}

func removeRoom(id uint32) {
	roomMu.Lock()
	delete(rooms, id)
	roomMu.Unlock()
}

// run 服务端 tick 循环
func (r *Room) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	dt := tickInterval.Seconds()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			if r.step(dt) {
				return
			}
		}
	}
}

// deploy 部署干员，返回错误码、提示与剩余费用
func (r *Room) deploy(opID, x, y uint32, dir int32) (int32, string, int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.over {
		return codeBadParam, "战斗已结束", r.cost
	}
	if x >= mapWidth || y >= mapHeight {
		return codeBadParam, "坐标超出地图", r.cost
	}
	if len(r.operators) >= maxOperators {
		return codeBadParam, "干员数量已达上限", r.cost
	}
	if r.cost < deployCost {
		return codeBadParam, "费用不足", r.cost
	}

	r.cost -= deployCost
	r.operators = append(r.operators, &Operator{ID: opID, X: x, Y: y, Direction: dir})
	logger.Printf("房间%d 部署干员%d 于(%d,%d)，剩余费用%d", r.ID, opID, x, y, r.cost)
	return codeOK, "ok", r.cost
}

// step 推进一帧；返回 true 表示战斗已结束
func (r *Room) step(dt float64) bool {
	r.mu.Lock()
	if r.over {
		r.mu.Unlock()
		return true
	}
	r.frame++

	total := pathLength()

	// 1. 敌人沿路径前进
	for _, e := range r.enemies {
		e.Dist += enemySpeed * dt
	}

	// 2. 射程内的干员对敌人造成伤害（每帧一次）
	for _, op := range r.operators {
		for _, e := range r.enemies {
			if e.HP <= 0 {
				continue
			}
			p := pointAtDistance(e.Dist) //pointAtDistance 按已行进距离求路径上的坐标；超出总长时返回终点
			if math.Hypot(p.X-float64(op.X), p.Y-float64(op.Y)) <= attackRange {
				e.HP -= attackDamage
			}
		}
	}

	// 3. 移除阵亡敌人；走到终点的敌人扣基地生命
	alive := make([]*Enemy, 0, len(r.enemies))
	for _, e := range r.enemies {
		if e.HP <= 0 {
			continue
		}
		if e.Dist >= total {
			r.baseHP--
			logger.Printf("房间%d 敌人到达终点，基地生命%d", r.ID, r.baseHP)
			continue
		}
		alive = append(alive, e)
	}
	r.enemies = alive

	// 4. 胜负判定
	result := int32(-1)
	switch {
	case r.baseHP <= 0:
		result = resultLose
	case len(r.enemies) == 0:
		result = resultWin
	}
	state := r.snapshotLocked()
	if result >= 0 {
		r.over = true
	}
	r.mu.Unlock()

	// 先解锁再推送，避免慢客户端阻塞 tick
	r.pushState(state)
	if result >= 0 {
		r.pushSettle(result)
		return true
	}
	return false
}

// snapshotLocked 生成当前帧状态快照，调用方需持有 r.mu
func (r *Room) snapshotLocked() *api.BattleStatePush {
	s := &api.BattleStatePush{
		RoomId: r.ID,
		Frame:  r.frame,
		Cost:   r.cost,
		BaseHp: r.baseHP,
	}
	for _, op := range r.operators {
		s.Operators = append(s.Operators, &api.OperatorState{
			OperatorId: op.ID,
			GridX:      op.X,
			GridY:      op.Y,
			Hp:         operatorMaxHP,
			Direction:  op.Direction,
		})
	}
	for _, e := range r.enemies {
		p := pointAtDistance(e.Dist)
		s.Enemies = append(s.Enemies, &api.EnemyState{
			EnemyId:    enemyConfigID,
			InstanceId: e.InstanceID,
			Hp:         e.HP,
			PosX:       float32(p.X),
			PosY:       float32(p.Y),
		})
	}
	return s
}

// pushState 下发帧状态（msgId 2004）
func (r *Room) pushState(state *api.BattleStatePush) {
	body, err := proto.Marshal(state)
	if err != nil {
		logger.Println("序列化 BattleStatePush 失败:", err)
		return
	}
	r.send(2004, body)
}

// pushSettle 下发结算（msgId 2005），随后销毁房间
func (r *Room) pushSettle(result int32) {
	duration := int32(time.Since(r.startedAt).Seconds())
	score := int32(0)
	var rewards []string
	if result == resultWin {
		score = 1000
		rewards = []string{"exp:100", "gold:500"}
	}
	body, err := proto.Marshal(&api.BattleSettlePush{
		RoomId:      r.ID,
		Result:      result,
		Score:       score,
		DurationSec: duration,
		Rewards:     rewards,
	})
	if err != nil {
		logger.Println("序列化 BattleSettlePush 失败:", err)
		return
	}
	r.send(2005, body)
	logger.Printf("房间%d 战斗结束：result=%d 用时%ds 分数%d", r.ID, result, duration, score)
	removeRoom(r.ID)
}
