package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/sharding"
)

var globalDB *gorm.DB

type Order struct {
	ID        int64  `gorm:"primaryKey"`
	OrderId   string `gorm:"sharding:order_id"` // 指明 OrderId 是分片键
	UserID    int64  `gorm:"sharding:user_id"`
	ProductID int64
	OrderDate time.Time
	OrderYear int `gorm:"sharding:order_year"`
}

func InitGormDb() *gorm.DB {
	log := logger.Default.LogMode(logger.Info)
	// 连接到 MySQL 数据库
	dsn := "user:password@tcp(ip:port)/sharding?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: dsn,
	}), &gorm.Config{
		Logger: log,
	})
	if err != nil {
		panic("failed to connect database")
	}
	globalDB = db
	return db
}

// orders 表的分表键为order_year，根据order_year分表
func customShardingAlgorithmWithOrderYear(value any) (suffix string, err error) {
	if year, ok := value.(int); ok {
		return fmt.Sprintf("_%d", year), nil
	}
	return "", fmt.Errorf("invalid order_date")
}

// orders 表的分表键为user_id，根据user_id分表
func customShardingAlgorithmWithUserId(value any) (suffix string, err error) {
	if userId, ok := value.(int64); ok {
		return fmt.Sprintf("_%d", userId%4), nil
	}
	return "", fmt.Errorf("invalid user_id")
}

// orders 表的分表键为user_id，根据order_id分表
func customShardingAlgorithmWithOrderId(value any) (suffix string, err error) {
	if orderId, ok := value.(string); ok {
		// 截取字符串，截取前8位，获取年份
		orderId = orderId[0:8]
		orderDate, err := time.Parse("20060102", orderId)
		if err != nil {
			return "", fmt.Errorf("invalid order_date")
		}
		year := orderDate.Year()
		return fmt.Sprintf("_%d", year), nil
	}
	return "", fmt.Errorf("invalid order_date")
}

// customePrimaryKeyGeneratorFn 自定义主键生成函数
func customePrimaryKeyGeneratorFn(tableIdx int64) int64 {
	var id int64
	seqTableName := "gorm_sharding_orders_id_seq" // 序列表名
	db := globalDB
	err := db.Exec("UPDATE `" + seqTableName + "` SET id = id+1").Error
	if err != nil {
		panic(err)
	}
	err = db.Raw("SELECT id FROM " + seqTableName + " ORDER BY id DESC LIMIT 1").Scan(&id).Error
	if err != nil {
		panic(err)
	}
	return id
}
func Test_Gorm_CreateTable(t *testing.T) {
	// 初始化 Gorm DB
	db := InitGormDb()

	// 创建gorm_sharding_orders_id_seq表
	err := db.Exec(`CREATE TABLE IF NOT EXISTS gorm_sharding_orders_id_seq (
	id BIGINT PRIMARY KEY NOT NULL DEFAULT 1
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).Error
	if err != nil {
		panic("failed to create table")
	}
	// 插入一条记录
	err = db.Exec(`INSERT INTO gorm_sharding_orders_id_seq (id) VALUES (1)`).Error
	if err != nil {
		panic("failed to insert data")
	}

	// 预先创建 4 个分片表。
	// orders_0, orders_1, orders_2, orders_3
	// 根据 user_id 分片键策略，每个分片表存储 user_id 取模 4 余数为 0, 1, 2, 3 的订单数据。
	for i := 0; i < 4; i++ {
		table := fmt.Sprintf("orders_%d", i)
		// 删除已存在的表（如果存在）
		db.Exec(`DROP TABLE IF EXISTS ` + table)
		// 创建新的分片表
		db.Exec(`CREATE TABLE ` + table + ` (
			id BIGINT PRIMARY KEY,
		    order_id VARCHAR(50),
		    user_id INT,
		    product_id INT,
		    order_date DATETIME
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	}

	// 创建 Order 表
	// 根据 order_id 分片键策略，每个分片表存储不同年份的订单数据。
	// 也可根据order_year分片策略路由到不同的分片表。
	// orders_2024, orders_2025
	err = db.Exec(`CREATE TABLE IF NOT EXISTS orders_2024 (
		id BIGINT PRIMARY KEY,
		order_id VARCHAR(50),
		user_id INT,
		product_id INT,
		order_date DATETIME,
		order_year INT
	)`).Error
	if err != nil {
		panic("failed to create table")
	}
	err = db.Exec(`CREATE TABLE IF NOT EXISTS orders_2025 (
		id BIGINT PRIMARY KEY,
		order_id VARCHAR(50),
		user_id INT,
		product_id INT,
		order_date DATETIME,
		order_year INT
	)`).Error
	if err != nil {
		panic("failed to create table")
	}
}

func Test_Gorm_Sharding_WithKeys(t *testing.T) {
	// 初始化 Gorm DB
	db := InitGormDb()

	// 分表策略配置
	configWithOrderYear := sharding.Config{
		ShardingKey:           "order_year",
		ShardingAlgorithm:     customShardingAlgorithmWithOrderYear, // 使用自定义的分片算法
		PrimaryKeyGenerator:   sharding.PKCustom,                    // 使用自定义的主键生成函数
		PrimaryKeyGeneratorFn: customePrimaryKeyGeneratorFn,         // 自定义主键生成函数
	}
	configWithUserId := sharding.Config{
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		ShardingAlgorithm:   customShardingAlgorithmWithUserId, // 使用自定义的分片算法
		PrimaryKeyGenerator: sharding.PKSnowflake,              // 使用 Snowflake 算法生成主键
	}
	configWithOrderId := sharding.Config{
		ShardingKey:           "order_id",
		ShardingAlgorithm:     customShardingAlgorithmWithOrderId, // 使用自定义的分片算法
		PrimaryKeyGenerator:   sharding.PKCustom,
		PrimaryKeyGeneratorFn: customePrimaryKeyGeneratorFn,
	}
	mapConfig := make(map[string]sharding.Config)
	mapConfig["orders_order_year"] = configWithOrderYear
	mapConfig["orders_user_id"] = configWithUserId
	mapConfig["orders_order_id"] = configWithOrderId

	// 配置 Gorm Sharding 中间件，注册分表策略配置
	middleware := sharding.RegisterWithKeys(mapConfig) // 逻辑表名为 "orders"
	db.Use(middleware)

	// 根据order_year分片键策略，插入和查询示例
	InsertOrderByOrderYearKey(db)
	FindByOrderYearKey(db, 2024)

	// 根据user_id分片键策略，插入和查询示例
	InsertOrderByUserId(db)
	FindByUserIDKey(db, int64(100))

	// 根据order_id分片键策略，插入、查询、更新和删除示例
	InsertOrderByOrderIdKey(db)
	FindOrderByOrderIdKey(db, "20240101ORDER0002")
	UpdateByOrderIdKey(db, "20240101ORDER0002")
	DeleteByOrderIdKey(db, "20240101ORDER8480")
}

func InsertOrderByOrderYearKey(db *gorm.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_year")
	db = db.WithContext(ctx)
	// 随机2024年或者2025年
	orderYear := rand.Intn(2) + 2024
	// 随机userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// 示例：插入订单数据
	order := Order{
		OrderId:   orderId,
		UserID:    int64(userId),
		ProductID: 100,
		OrderDate: time.Date(orderYear, 1, 1, 0, 0, 0, 0, time.UTC),
		OrderYear: orderYear,
	}
	err := db.Table("orders").Create(&order).Error
	if err != nil {
		fmt.Println("Error creating order:", err)
	}
	return err
}
func FindByOrderYearKey(db *gorm.DB, orderYear int) ([]Order, error) {
	// 查询示例
	var orders []Order
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_year")
	db = db.WithContext(ctx)
	db = db.Table("orders")
	err := db.Model(&Order{}).Where("order_year=? and product_id=? and order_id=?", orderYear, 102, "20240101ORDER0002").Find(&orders).Error
	if err != nil {
		fmt.Println("Error querying orders:", err)
	}
	fmt.Printf("sharding key order_year Selected orders: %#v\n", orders)
	return orders, err
}

func InsertOrderByOrderIdKey(db *gorm.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_id")
	db = db.WithContext(ctx)
	// 随机2024年或者2025年
	orderYear := rand.Intn(2) + 2024
	// 随机userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// 示例：插入订单数据
	order := Order{
		OrderId:   orderId,
		UserID:    int64(userId),
		ProductID: 100,
		OrderDate: time.Date(orderYear, 1, 1, 0, 0, 0, 0, time.UTC),
		OrderYear: orderYear,
	}
	db = db.Table("orders")
	err := db.Create(&order).Error
	if err != nil {
		fmt.Println("Error creating order:", err)
	}
	return err
}

func UpdateByOrderIdKey(db *gorm.DB, orderId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_id")
	db = db.WithContext(ctx)
	db = db.Table("orders")
	err := db.Model(&Order{}).Where("order_id=?", orderId).Update("product_id", 102).Error
	if err != nil {
		fmt.Println("Error updating order:", err)
	}
	return err
}

func DeleteByOrderIdKey(db *gorm.DB, orderId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_id")
	db = db.WithContext(ctx)
	db = db.Table("orders")
	err := db.Where("order_id=? and product_id=?", orderId, 100).Delete(&Order{}).Error
	if err != nil {
		fmt.Println("Error deleting order:", err)
	}
	return err
}
func FindOrderByOrderIdKey(db *gorm.DB, orderId string) ([]Order, error) {
	var orders []Order
	// 查询示例
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_id")
	db = db.WithContext(ctx)
	db = db.Table("orders")
	err := db.Model(&Order{}).Where("order_id=?", orderId).Find(&orders).Error
	if err != nil {
		fmt.Println("Error querying orders:", err)
	}
	fmt.Printf("sharding key order_id Selected orders: %#v\n", orders)
	return orders, err
}

type OrderByUserId struct {
	ID        int64  `gorm:"primaryKey"`
	OrderId   string `gorm:"sharding:order_id"` // 指明 OrderId 是分片键
	UserID    int64  `gorm:"sharding:user_id"`
	ProductID int64
	OrderDate time.Time
}

func InsertOrderByUserId(db *gorm.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "user_id")
	db = db.WithContext(ctx)
	// 随机2024年或者2025年
	orderYear := rand.Intn(2) + 2024
	// 随机userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// 示例：插入订单数据
	order := OrderByUserId{
		OrderId:   orderId,
		UserID:    int64(userId),
		ProductID: 100,
		OrderDate: time.Date(orderYear, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	err := db.Table("orders").Create(&order).Error
	if err != nil {
		fmt.Println("Error creating order:", err)
	}
	return err
}

func FindByUserIDKey(db *gorm.DB, userID int64) ([]Order, error) {
	var orders []Order
	// 查询示例
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "user_id")
	db = db.WithContext(ctx)
	db = db.Table("orders")
	err := db.Model(&Order{}).Where("user_id = ?", userID).Find(&orders).Error
	if err != nil {
		fmt.Println("Error querying orders:", err)
	}
	fmt.Printf("sharding key user_id Selected orders: %#v\n", orders)
	return orders, err
}
