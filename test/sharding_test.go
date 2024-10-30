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
	OrderId   string `gorm:"sharding:order_id"` // Specify that OrderId is the sharding key
	UserID    int64  `gorm:"sharding:user_id"`
	ProductID int64
	OrderDate time.Time
	OrderYear int `gorm:"sharding:order_year"`
}

func InitGormDb() *gorm.DB {
	log := logger.Default.LogMode(logger.Info)
	// Connect to MySQL database
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

// The sharding key of the orders table is order_year, sharding based on order_year
func customShardingAlgorithmWithOrderYear(value any) (suffix string, err error) {
	if year, ok := value.(int); ok {
		return fmt.Sprintf("_%d", year), nil
	}
	return "", fmt.Errorf("invalid order_date")
}

// The sharding key of the orders table is user_id, sharding based on user_id
func customShardingAlgorithmWithUserId(value any) (suffix string, err error) {
	if userId, ok := value.(int64); ok {
		return fmt.Sprintf("_%d", userId%4), nil
	}
	return "", fmt.Errorf("invalid user_id")
}

// The sharding key of the orders table is order_id, sharding based on order_id
func customShardingAlgorithmWithOrderId(value any) (suffix string, err error) {
	if orderId, ok := value.(string); ok {
		// Extract the first 8 characters of the string to get the year
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

// customePrimaryKeyGeneratorFn Custom primary key generation function
func customePrimaryKeyGeneratorFn(tableIdx int64) int64 {
	var id int64
	seqTableName := "gorm_sharding_orders_id_seq" // Sequence table name
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
	// Initialize Gorm DB
	db := InitGormDb()

	// Create gorm_sharding_orders_id_seq table
	err := db.Exec(`CREATE TABLE IF NOT EXISTS gorm_sharding_orders_id_seq (
	id BIGINT PRIMARY KEY NOT NULL DEFAULT 1
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`).Error
	if err != nil {
		panic("failed to create table")
	}
	// Insert a record
	err = db.Exec(`INSERT INTO gorm_sharding_orders_id_seq (id) VALUES (1)`).Error
	if err != nil {
		panic("failed to insert data")
	}

	// Pre-create 4 shard tables.
	// orders_0, orders_1, orders_2, orders_3
	// According to the user_id sharding key strategy, each shard table stores order data with user_id modulo 4 remainder of 0, 1, 2, 3.
	for i := 0; i < 4; i++ {
		table := fmt.Sprintf("orders_%d", i)
		// Drop existing table (if exists)
		db.Exec(`DROP TABLE IF EXISTS ` + table)
		// Create new shard table
		db.Exec(`CREATE TABLE ` + table + ` (
			id BIGINT PRIMARY KEY,
						order_id VARCHAR(50),
						user_id INT,
						product_id INT,
						order_date DATETIME
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	}

	// Create Order table
	// According to the order_id sharding key strategy, each shard table stores order data of different years.
	// It can also be routed to different shard tables according to the order_year sharding strategy.
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
	// Initialize Gorm DB
	db := InitGormDb()

	// Configure Gorm Sharding middleware, register sharding strategy configuration
	// Logical table name is "orders"
	db.Use(sharding.RegisterWithKeys(map[string]sharding.Config{
		"orders_order_year": {
			ShardingKey:           "order_year",
			// Use custom sharding algorithm
			ShardingAlgorithm:     customShardingAlgorithmWithOrderYear,
			// Use custom primary key generation function
			PrimaryKeyGenerator:   sharding.PKCustom,
			// Custom primary key generation function
			PrimaryKeyGeneratorFn: customePrimaryKeyGeneratorFn,
		},
		"orders_user_id": {
			ShardingKey:         "user_id",
			NumberOfShards:      4,
			// Use custom sharding algorithm
			ShardingAlgorithm:   customShardingAlgorithmWithUserId,
			// Use Snowflake algorithm to generate primary key
			PrimaryKeyGenerator: sharding.PKSnowflake,
		},
		"orders_order_id": {
			ShardingKey:           "order_id",
			// Use custom sharding algorithm
			ShardingAlgorithm:     customShardingAlgorithmWithOrderId,
			PrimaryKeyGenerator:   sharding.PKCustom,
			PrimaryKeyGeneratorFn: customePrimaryKeyGeneratorFn,
		},
	}))

	// Insert and query examples based on order_year sharding key strategy
	InsertOrderByOrderYearKey(db)
	FindByOrderYearKey(db, 2024)

	// Insert and query examples based on user_id sharding key strategy
	InsertOrderByUserId(db)
	FindByUserIDKey(db, int64(100))

	// Insert, query, update, and delete examples based on order_id sharding key strategy
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
	// Randomly 2024 or 2025
	orderYear := rand.Intn(2) + 2024
	// Random userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// Example: Insert order data
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
	// Query example
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
	fmt.Printf("sharding key order_year Selected orders: %#v\nn", orders)
	return orders, err
}

func InsertOrderByOrderIdKey(db *gorm.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "order_id")
	db = db.WithContext(ctx)
	// Randomly 2024 or 2025
	orderYear := rand.Intn(2) + 2024
	// Random userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// Example: Insert order data
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
	// Query example
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
	OrderId   string `gorm:"sharding:order_id"` // Specify that OrderId is the sharding key
	UserID    int64  `gorm:"sharding:user_id"`
	ProductID int64
	OrderDate time.Time
}

func InsertOrderByUserId(db *gorm.DB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "sharding_key", "user_id")
	db = db.WithContext(ctx)
	// Randomly 2024 or 2025
	orderYear := rand.Intn(2) + 2024
	// Random userId
	userId := rand.Intn(100)
	orderId := fmt.Sprintf("%d0101ORDER%04v", orderYear, rand.Int31n(10000))
	// Example: Insert order data
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
	// Query example
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
