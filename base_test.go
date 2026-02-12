package sharding

import (
	"gorm.io/gorm"
	"testing"
)

type OrderOther struct {
	ID      int64 `gorm:"primarykey"`
	UserID  uint64
	Product string
	Deleted gorm.DeletedAt

	BaseSharding
}

func (OrderOther) TableName() string {
	return "order_other"
}

func useBase() {
	shardingMiddleware := RegisterWithModel(&OrderOther{})
	db.Use(shardingMiddleware)
}

func useBaseWithShardingNumbers() {
	orderOther := &OrderOther{}
	orderOther.DefaultShards = 2
	shardingMiddleware := RegisterWithModel(orderOther)
	db.Use(shardingMiddleware)
}

func TestUseBase(t *testing.T) {
	useBase()
	db.AutoMigrate(&OrderOther{})
}

func TestUseBaseInsert(t *testing.T) {
	useBaseWithShardingNumbers()
	tx := db.Create(&OrderOther{ID: 100, UserID: 100, Product: "iPhone"})
	assertQueryResult(t, `INSERT INTO order_other_0 ("user_id", "product", "deleted", "id") VALUES ($1, $2, $3, $4) RETURNING "id"`, tx)
}
