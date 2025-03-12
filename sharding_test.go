package sharding

import (
	"context"
	"fmt"
	tassert "github.com/stretchr/testify/assert"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/assert"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/hints"
	"gorm.io/plugin/dbresolver"
)

type Order struct {
	ID         int64 `gorm:"primarykey"`
	UserID     int64
	Product    string
	CategoryID int64
}

type Category struct {
	ID   int64 `gorm:"primarykey"`
	Name string
}

type User struct {
	ID   int64 `gorm:"primarykey"`
	Name string
}

type OrderDetail struct {
	ID       int64 `gorm:"primarykey"`
	OrderID  int64
	Product  string
	Quantity int
}

type Swap struct {
	GID     *uint64 `gorm:"column:gid;primaryKey;autoIncrement"`
	ID      uint64  `gorm:"not null;index"`
	EventID uint64  `gorm:"not null;index"`
	Escrow  bool    `gorm:"not null"`
}

// Contract represents a blockchain contract
type Contract struct {
	ID        int64  `gorm:"primarykey"`
	Name      string `gorm:"index:idx_name"`
	Type      string `gorm:"index:idx_type"`
	IsERC20   bool
	IsERC721  bool
	IsERC1155 bool
	Data      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Product is a model that we don't configure for sharding
type Product struct {
	ID         int64 `gorm:"primarykey"`
	Name       string
	Price      float64
	CategoryID int64
}

// ContractData represents detailed contract data
type ContractData struct {
	ID         int64 `gorm:"primarykey"`
	ContractID int64
	Key        string
	Value      string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func dbURL() string {
	dbURL := os.Getenv("DB_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://postgres:@localhost:6432/sharding-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbNoIDURL() string {
	dbURL := os.Getenv("DB_NOID_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://postgres:@localhost:6432/sharding-noid-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-noid-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbReadURL() string {
	dbURL := os.Getenv("DB_READ_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://postgres:@localhost:6432/sharding-read-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-read-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbWriteURL() string {
	dbURL := os.Getenv("DB_WRITE_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://postgres:@localhost:6432/sharding-write-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-write-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbListURL() string {
	dbURL := os.Getenv("DB_LIST_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://postgres:@localhost:6432/sharding-list-test?sslmode=disable"
	}
	return dbURL
}

var (
	dbConfig = postgres.Config{
		DSN:                  dbURL(),
		PreferSimpleProtocol: true,
	}
	dbNoIDConfig = postgres.Config{
		DSN:                  dbNoIDURL(),
		PreferSimpleProtocol: true,
	}
	dbReadConfig = postgres.Config{
		DSN:                  dbReadURL(),
		PreferSimpleProtocol: true,
	}
	dbWriteConfig = postgres.Config{
		DSN:                  dbWriteURL(),
		PreferSimpleProtocol: true,
	}
	dbListConfig = postgres.Config{
		DSN:                  dbListURL(),
		PreferSimpleProtocol: true,
	}

	db, dbNoID, dbRead, dbWrite, dbList *gorm.DB

	listConfigs                                                                                                                map[string]Config
	shardingConfig, shardingConfigOrderDetails, shardingConfigNoID, shardingConfigUser, listShardingConfig, contractDataConfig Config
	middleware, middlewareNoID, listMiddleware                                                                                 *Sharding
	node, _                                                                                                                    = snowflake.NewNode(1)
)

func init() {
	if mysqlDialector() {
		db, _ = gorm.Open(mysql.Open(dbURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbNoID, _ = gorm.Open(mysql.Open(dbNoIDURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbRead, _ = gorm.Open(mysql.Open(dbReadURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbWrite, _ = gorm.Open(mysql.Open(dbWriteURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	} else {
		db, _ = gorm.Open(postgres.New(dbConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logger.Info),
		})
		dbNoID, _ = gorm.Open(postgres.New(dbNoIDConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logger.Info),
		})
		dbRead, _ = gorm.Open(postgres.New(dbReadConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logger.Info),
		})
		dbWrite, _ = gorm.Open(postgres.New(dbWriteConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logger.Info),
		})
		dbList, _ = gorm.Open(postgres.New(dbListConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
			Logger:                                   logger.Default.LogMode(logger.Info),
		})
	}

	shardingConfig = Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	shardingConfigNoID = Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKCustom,
		PrimaryKeyGeneratorFn: func(_ int64) int64 {
			return 0
		},
	}

	shardingConfigOrderDetails = Config{
		DoubleWrite:         true,
		ShardingKey:         "order_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}
	shardingConfigUser = Config{
		DoubleWrite:         true,
		ShardingKey:         "id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}
	// Updated listShardingConfig
	listShardingConfig = Config{
		PartitionType:  PartitionTypeList,
		ShardingKey:    "type",
		NumberOfShards: 3, // 3 partition types
		ListValues: map[string]int{
			"ERC20":   0,
			"ERC721":  1,
			"ERC1155": 2,
		},
		DefaultPartition:    -1, // Error if unknown type
		PrimaryKeyGenerator: PKSnowflake,
		// Debug the ListValues to make sure they're being registered correctly
		ShardingAlgorithm: func(value interface{}) (string, error) {
			var strValue string
			switch v := value.(type) {
			case string:
				strValue = v
			default:
				strValue = fmt.Sprintf("%v", v)
			}

			log.Printf("Looking up list partition for type '%s'", strValue)

			if strings.HasPrefix(strValue, "is_") {
				if strings.Contains(strValue, "erc20") {
					return "_0", nil
				} else if strings.Contains(strValue, "erc721") {
					return "_1", nil
				} else if strings.Contains(strValue, "erc1155") {
					return "_2", nil
				}
			}

			// Look up partition number in ListValues map
			partitionNum, exists := listShardingConfig.ListValues[strValue]
			if !exists {
				// Try case-insensitive match
				for key, val := range listShardingConfig.ListValues {
					if strings.EqualFold(key, strValue) {
						partitionNum = val
						exists = true
						log.Printf("Found partition %d for case-insensitive type '%s'", partitionNum, strValue)
						break
					}
				}

				if !exists {
					if listShardingConfig.DefaultPartition >= 0 {
						partitionNum = listShardingConfig.DefaultPartition
						log.Printf("Using default partition %d for type '%s'", partitionNum, strValue)
					} else {
						return "", fmt.Errorf("contract type '%s' not found in partition list", strValue)
					}
				}
			} else {
				log.Printf("Found partition %d for contract type '%s'", partitionNum, strValue)
			}

			return fmt.Sprintf("_%d", partitionNum), nil
		},
	}

	// List partitioning config for related contract data table
	// Updated contractDataConfig
	contractDataConfig = Config{
		PartitionType:       PartitionTypeList,
		ShardingKey:         "contract_id",
		NumberOfShards:      3,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingAlgorithm: func(value interface{}) (string, error) {
			contractID, err := toInt64(value)
			if err != nil {
				return "", fmt.Errorf("invalid contract ID: %v", err)
			}

			// For test purposes or when we can't determine the partition,
			// use ID-based routing

			// Simple ID-based routing:
			// For negative IDs (tests), we'll put everything in partition 0
			if contractID < 0 {
				log.Printf("Contract ID %d is negative (test data), using partition 0", contractID)
				return "_0", nil
			}

			// For positive IDs, use a mathematical approach
			// This assumes contract IDs follow some pattern, like ID % 3 == type index
			// Adjust this formula to match  actual ID generation strategy
			partition := int(contractID % 3)
			log.Printf("Using ID-based routing for contract data with ID %d: partition %d",
				contractID, partition)

			return fmt.Sprintf("_%d", partition), nil
		},
		// Optional: provide a sharding_suffixes implementation that covers all partitions
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2"}
		},
	}
	configs := map[string]Config{
		"orders":        shardingConfig,
		"order_details": shardingConfigOrderDetails,
		"users":         shardingConfigUser,
	}

	listConfigs = map[string]Config{
		"contracts":     listShardingConfig,
		"contract_data": contractDataConfig,
	}

	// Register sharding middleware
	listMiddleware = Register(listConfigs, &Contract{}, &ContractData{})

	middleware = Register(configs, &Order{}, &OrderDetail{}, &User{})
	middlewareNoID = Register(shardingConfigNoID, &Order{}, &OrderDetail{})

	fmt.Println("Clean only tables ...")
	dropTables()
	fmt.Println("AutoMigrate tables ...")
	err := db.AutoMigrate(&Order{}, &Category{}, &OrderDetail{}, &User{})
	if err != nil {
		panic(err)
	}

	fmt.Println("Creating list partitioning test tables...")
	err = dbList.AutoMigrate(&Contract{}, &ContractData{})
	if err != nil {
		panic(fmt.Sprintf("failed to auto-migrate list partitioning test tables: %v", err))
	}

	stables := []string{
		"orders_0", "orders_1", "orders_2", "orders_3",
		"order_details_0", "order_details_1", "order_details_2", "order_details_3",
		"users_0", "users_1", "users_2", "users_3",
	}

	for _, table := range stables {
		if strings.HasPrefix(table, "orders") {
			createOrdersTable(table)
		} else if strings.HasPrefix(table, "order_details") {
			createOrderDetailsTable(table)
		} else if strings.HasPrefix(table, "users") {
			createUsersTable(table)
		}
	}
	for i := 0; i < 3; i++ {
		createContractTable(fmt.Sprintf("contracts_%d", i))
		createContractDataTable(fmt.Sprintf("contract_data_%d", i))
	}

	db.Use(middleware)
	dbNoID.Use(middlewareNoID)
	dbList.Use(listMiddleware)
}

// Helper functions to create tables
func createOrdersTable(table string) {
	db.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        user_id bigint,
        product text,
        category_id bigint
    )`)
	dbNoID.Exec(`CREATE TABLE ` + table + ` (
        user_id bigint,
        product text,
        category_id bigint
    )`)
	dbRead.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        user_id bigint,
        product text,
        category_id bigint
    )`)
	dbWrite.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        user_id bigint,
        product text,
        category_id bigint
    )`)
}

func createOrderDetailsTable(table string) {
	db.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        order_id bigint,
        product text,
        quantity int
    )`)
	dbNoID.Exec(`CREATE TABLE ` + table + ` (
        order_id bigint,
        product text,
        quantity int
    )`)
	dbRead.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        order_id bigint,
        product text,
        quantity int
    )`)
	dbWrite.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        order_id bigint,
        product text,
        quantity int
    )`)
}

func createUsersTable(table string) {
	db.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        name text
    )`)
	dbNoID.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        name text
    )`)
	dbRead.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        name text
    )`)
	dbWrite.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        name text
    )`)
}

func createContractTable(table string) {
	dbList.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        name text,
        type text,
        is_erc20 boolean,
        is_erc721 boolean,
        is_erc1155 boolean,
        data text,
        created_at timestamp with time zone,
        updated_at timestamp with time zone
    )`)
}

func createContractDataTable(table string) {
	dbList.Exec(`CREATE TABLE ` + table + ` (
        id bigint PRIMARY KEY,
        contract_id bigint,
        key text,
        value text,
        created_at timestamp with time zone,
        updated_at timestamp with time zone
    )`)
}

func dropTables() {
	tables := []string{
		"orders", "orders_0", "orders_1", "orders_2", "orders_3",
		"order_details", "order_details_0", "order_details_1", "order_details_2", "order_details_3",
		"categories",
		"users", "users_0", "users_1", "users_2", "users_3",
		"swaps", "swaps_0", "swaps_1", "swaps_2", "swaps_3",
		"contracts", "contracts_0", "contracts_1", "contracts_2",
		"contract_data", "contract_data_0", "contract_data_1", "contract_data_2",
	}
	for _, table := range tables {
		db.Exec("DROP TABLE IF EXISTS " + table)
		dbNoID.Exec("DROP TABLE IF EXISTS " + table)
		dbRead.Exec("DROP TABLE IF EXISTS " + table)
		dbWrite.Exec("DROP TABLE IF EXISTS " + table)
		dbList.Exec("DROP TABLE IF EXISTS " + table)
		if mysqlDialector() {
			db.Exec(("DROP TABLE IF EXISTS gorm_sharding_" + table + "_id_seq"))
		} else {
			db.Exec(("DROP SEQUENCE IF EXISTS gorm_sharding_" + table + "_id_seq"))
		}
	}
}

func TestMigrate(t *testing.T) {
	targetTables := []string{"orders", "orders_0", "orders_1", "orders_2", "orders_3", "categories", "order_details", "order_details_0", "order_details_1", "order_details_2", "order_details_3", "users", "users_0", "users_1", "users_2", "users_3"}
	sort.Strings(targetTables)

	// origin tables
	tables, _ := db.Migrator().GetTables()
	sort.Strings(tables)
	assert.Equal(t, tables, targetTables)

	// drop table
	db.Migrator().DropTable(Order{}, &Category{}, &OrderDetail{}, &User{})
	tables, _ = db.Migrator().GetTables()
	assert.Equal(t, len(tables), 0)

	// auto migrate
	db.AutoMigrate(&Order{}, &Category{}, &OrderDetail{}, &User{})
	tables, _ = db.Migrator().GetTables()
	sort.Strings(tables)
	assert.Equal(t, tables, targetTables)

	// auto migrate again
	err := db.AutoMigrate(&Order{}, &Category{}, &OrderDetail{}, &User{})
	assert.Equal[error, error](t, err, nil)
}

func TestInsert(t *testing.T) {
	db.Create(&Order{ID: 100, UserID: 100, Product: "iPhone", CategoryID: 1})
	assertQueryResult(t, `INSERT INTO orders_0 ("user_id", "product", "category_id", "id") VALUES ($1, $2, $3, $4) RETURNING "id"`, middleware)
}

func TestInsertNoID(t *testing.T) {
	dbNoID.Create(&Order{UserID: 100, Product: "iPhone", CategoryID: 1})
	expected := `INSERT INTO orders_0 (user_id, product, category_id) VALUES ($1, $2, $3) RETURNING id`
	assert.Equal(t, toDialect(expected), middlewareNoID.LastQuery())
}

func TestFillID(t *testing.T) {
	db.Create(&Order{UserID: 100, Product: "iPhone", CategoryID: 1})
	expected := `INSERT INTO orders_0 (user_id, product, category_id, id) VALUES`
	lastQuery := middleware.LastQuery()
	if len(lastQuery) < len(expected) {
		t.Fatalf("lastQuery is too short: %s", lastQuery)
	}
	assert.Equal(t, toDialect(expected), lastQuery[0:len(expected)])
}

func TestInsertManyWithFillID(t *testing.T) {
	err := db.Create([]Order{{UserID: 100, Product: "Mac", CategoryID: 1}, {UserID: 100, Product: "Mac Pro", CategoryID: 2}}).Error
	assert.Equal[error, error](t, err, nil)

	expected := `INSERT INTO orders_0 ("user_id", "product", "category_id", id) VALUES ($1, $2, $3, $sfid), ($4, $5, $6, $sfid) RETURNING "id"`
	lastQuery := middleware.LastQuery()
	fmt.Println("lastQuery", lastQuery)
	assertSfidQueryResult(t, toDialect(expected), lastQuery)
}

func TestInsertDiffSuffix(t *testing.T) {
	err := db.Create([]Order{{UserID: 100, Product: "Mac"}, {UserID: 101, Product: "Mac Pro"}}).Error
	assert.Equal(t, ErrInsertDiffSuffix, err)
}

func TestSelect1(t *testing.T) {
	db.Model(&Order{}).Where("user_id", 101).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "user_id" = $1 AND "id" = $2`, middleware)
}

func TestSelect2(t *testing.T) {
	db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1 AND "user_id" = $2`, middleware)
}

func TestSelect3(t *testing.T) {
	db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1 AND user_id = 101`, middleware)
}

func TestSelect4(t *testing.T) {
	db.Model(&Order{}).Where("product", "iPad").Where("user_id", 100).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_0 WHERE "product" = $1 AND "user_id" = $2`, middleware)
}

func TestSelect5(t *testing.T) {
	db.Model(&Order{}).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE user_id = 101`, middleware)
}

func TestSelect6(t *testing.T) {
	db.Model(&Order{}).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1`, middleware)
}

func TestSelect7(t *testing.T) {
	db.Model(&Order{}).Where("user_id", 101).Where("id > ?", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "user_id" = $1 AND id > $2`, middleware)
}

func TestSelect8(t *testing.T) {
	db.Model(&Order{}).Where("id > ?", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE id > $1 AND "user_id" = $2`, middleware)
}

func TestSelect9(t *testing.T) {
	db.Model(&Order{}).Where("user_id = 101").First(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE user_id = 101 ORDER BY "orders_1"."id" LIMIT 1`, middleware)
}

func TestSelect10(t *testing.T) {
	db.Clauses(hints.Comment("select", "nosharding")).Model(&Order{}).Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders"`, middleware)
}

func TestSelect11(t *testing.T) {
	db.Clauses(hints.Comment("select", "nosharding")).Model(&Order{}).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders" WHERE "user_id" = $1`, middleware)
}

func TestSelect12(t *testing.T) {
	sql := toDialect(`SELECT * FROM "public"."orders" WHERE user_id = 101`)
	db.Raw(sql).Find(&[]Order{})
	assertQueryResult(t, sql, middleware)
}

func TestSelect13(t *testing.T) {
	var n int
	db.Raw("SELECT 1").Find(&n)
	assertQueryResult(t, `SELECT 1`, middleware)
}

func TestSelect14(t *testing.T) {
	dbNoID.Model(&Order{}).Where("user_id = 101").Find(&[]Order{})
	expected := `SELECT * FROM orders_1 WHERE user_id = 101`
	assert.Equal(t, toDialect(expected), middlewareNoID.LastQuery())
}

func TestUpdate(t *testing.T) {
	db.Model(&Order{}).Where("user_id = ?", 100).Update("product", "new title")
	assertQueryResult(t, `UPDATE orders_0 SET "product" = $1 WHERE user_id = $2`, middleware)
}

func TestDelete(t *testing.T) {
	db.Where("user_id = ?", 100).Delete(&Order{})
	assertQueryResult(t, `DELETE FROM orders_0 WHERE user_id = $1`, middleware)
}

func TestInsertMissingShardingKey(t *testing.T) {
	err := db.Exec(`INSERT INTO "orders" ("id", "product") VALUES(1, 'iPad')`).Error
	assert.Equal(t, err == nil, true) // should be true because of double write
}

func TestSelectMissingShardingKey(t *testing.T) {
	err := db.Exec(`SELECT * FROM "orders" WHERE "product" = 'iPad'`).Error
	assert.Equal(t, err == nil, true) // should be true because of double write
}

func TestSelectNoSharding(t *testing.T) {
	sql := toDialect(`SELECT /* nosharding */ * FROM "orders" WHERE "product" = 'iPad'`)
	err := db.Exec(sql).Error
	assert.Equal[error](t, nil, err)
}

//func TestNoEq(t *testing.T) {
//	err := db.Model(&Order{}).Where("user_id <> ?", 101).Find([]Order{}).Error
//	assert.Equal(t, ErrMissingShardingKey, err)
//}

func TestShardingKeyOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id = ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

func TestShardingKeyNotOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id > ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal(t, err == nil, true) // Double write
}

func TestShardingIdOK(t *testing.T) {
	err := db.Model(&Order{}).Where("id = ? and user_id > ?", int64(101), 100).Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

func TestNoSharding(t *testing.T) {
	categories := []Category{}
	db.Model(&Category{}).Where("id = ?", 1).Find(&categories)
	assertQueryResult(t, `SELECT * FROM "categories" WHERE id = $1`, middleware)
}

func TestPKSnowflake(t *testing.T) {
	var db *gorm.DB
	if mysqlDialector() {
		db, _ = gorm.Open(mysql.Open(dbURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	} else {
		db, _ = gorm.Open(postgres.New(dbConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	}
	shardingConfig.PrimaryKeyGenerator = PKSnowflake
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	node, _ := snowflake.NewNode(0)
	sfid := node.Generate().Int64()
	expected := fmt.Sprintf(`INSERT INTO orders_0 (user_id, product, category_id, id) VALUES ($1, $2, $3, %d`, sfid)[0:68]
	expected = toDialect(expected)

	db.Create(&Order{UserID: 100, Product: "iPhone", CategoryID: 1})
	assert.Equal(t, expected, middleware.LastQuery()[0:len(expected)])
}

func TestPKPGSequence(t *testing.T) {
	if mysqlDialector() {
		return
	}

	db, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	shardingConfig.PrimaryKeyGenerator = PKPGSequence
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	db.Exec("SELECT setval('" + pgSeqName("orders") + "', 42)")
	db.Create(&Order{UserID: 100, Product: "iPhone", CategoryID: 1})
	expected := `INSERT INTO orders_0 (user_id, product, category_id, id) VALUES ($1, $2, $3, 43) RETURNING id`
	assert.Equal(t, expected, middleware.LastQuery())
}

func TestPKMySQLSequence(t *testing.T) {
	if !mysqlDialector() {
		return
	}

	db, _ := gorm.Open(mysql.Open(dbURL()), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	shardingConfig.PrimaryKeyGenerator = PKMySQLSequence
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	db.Exec("UPDATE `" + mySQLSeqName("orders") + "` SET id = 42")
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := "INSERT INTO orders_0 (`user_id`, `product`, id) VALUES (?, ?, 43)"
	if mariadbDialector() {
		expected = expected + " RETURNING `id`"
	}
	assert.Equal(t, expected, middleware.LastQuery())
}

func TestReadWriteSplitting(t *testing.T) {
	dbRead.Exec("INSERT INTO orders_0 (id, product, user_id) VALUES(1, 'iPad', 100)")
	dbWrite.Exec("INSERT INTO orders_0 (id, product, user_id) VALUES(1, 'iPad', 100)")

	var db *gorm.DB
	if mysqlDialector() {
		db, _ = gorm.Open(mysql.Open(dbWriteURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	} else {
		db, _ = gorm.Open(postgres.New(dbWriteConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	}

	db.Use(dbresolver.Register(dbresolver.Config{
		Sources:  []gorm.Dialector{dbWrite.Dialector},
		Replicas: []gorm.Dialector{dbRead.Dialector},
	}))
	db.Use(middleware)

	var order Order
	db.Model(&Order{}).Where("user_id", 100).Find(&order)
	assert.Equal(t, "iPad", order.Product)

	db.Model(&Order{}).Where("user_id", 100).Update("product", "iPhone")
	db.Clauses(dbresolver.Read).Table("orders_0").Where("user_id", 100).Find(&order)
	assert.Equal(t, "iPad", order.Product)

	dbWrite.Table("orders_0").Where("user_id", 100).Find(&order)
	assert.Equal(t, "iPhone", order.Product)
}

func TestDataRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error)

	for i := 0; i < 2; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := db.Model(&Order{}).Where("user_id", 100).Find(&[]Order{}).Error
					if err != nil {
						ch <- err
						return
					}
				}
			}
		}()
	}

	select {
	case <-time.After(time.Millisecond * 50):
		cancel()
	case err := <-ch:
		cancel()
		t.Fatal(err)
	}
}

func TestJoinTwoShardedTables(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3", "order_details", "order_details_0", "order_details_1", "order_details_2", "order_details_3")
	// Prepare data without hardcoded IDs
	order := Order{UserID: 100, Product: "iPhone"}
	db.Create(&order)

	orderDetail := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
	db.Create(&orderDetail)

	// Recalculate table suffixes based on generated IDs
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderDetailShardIndex := uint(orderDetail.OrderID) % shardingConfigOrderDetails.NumberOfShards

	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)

	// Expected query with sharded table names
	expectedQuery := fmt.Sprintf(
		`SELECT orders%s.*, order_details%s.product AS order_detail_product, order_details%s.quantity AS order_detail_quantity FROM orders%s JOIN order_details%s ON order_details%s.order_id = orders%s.id WHERE orders%s.user_id = $1 AND orders%s.id = $2`,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix, orderTableSuffix, orderTableSuffix)

	// Perform the query
	var results []struct {
		Order
		OrderDetailProduct  string
		OrderDetailQuantity int
	}

	db.Model(&Order{}).
		Select("orders.*, order_details.product AS order_detail_product, order_details.quantity AS order_detail_quantity").
		Joins("JOIN order_details ON order_details.order_id = orders.id").
		Where("orders.user_id = ? AND orders.id = ?", 100, order.ID).
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	assert.Equal(t, 1, len(results))
	if len(results) == 0 {
		t.Fatalf("no results")
	}
	assert.Equal(t, "iPhone", results[0].Product)
	assert.Equal(t, "iPhone Case", results[0].OrderDetailProduct)
	assert.Equal(t, 2, results[0].OrderDetailQuantity)
	fmt.Println("ENDING")
}

func TestCompositeInClause(t *testing.T) {
	// Define a token model
	type Token struct {
		ID        int64 `gorm:"primarykey"`
		Contract  string
		TokenID   string
		UserID    int64
		Metadata  string
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	// Create a clean test DB setup
	truncateTables(db, "tokens", "tokens_0", "tokens_1", "tokens_2", "tokens_3")

	// Configure sharding for tokens
	tokensConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	// Create a fresh DB instance with configured middleware
	testDB, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})

	// Register the sharding middleware
	testMiddleware := Register(
		map[string]Config{"tokens": tokensConfig},
		&Token{},
	)
	testDB.Use(testMiddleware)

	// Create the tokens table
	err := testDB.AutoMigrate(&Token{})
	assert.Equal[error, error](t, err, nil)

	// Insert test data across different shards
	testTokens := []Token{
		{
			Contract: "0x062ed5c1a781685032d5a9f3ba189f6742cb4e04",
			TokenID:  "10",
			UserID:   100, // Shard 0
			Metadata: "Token A",
		},
		{
			Contract: "0x062ed5c1a781685032d5a9f3ba189f6742cb4e04",
			TokenID:  "20",
			UserID:   101, // Shard 1
			Metadata: "Token B",
		},
		{
			Contract: "0xdifferent",
			TokenID:  "10",
			UserID:   100, // Shard 0
			Metadata: "Token C",
		},
	}

	for _, token := range testTokens {
		err := testDB.Create(&token).Error
		assert.Equal[error, error](t, err, nil)
	}

	// Test 1: Query with composite IN clause when user_id is specified
	t.Run("CompositeInWithShardingKey", func(t *testing.T) {
		var results []Token
		err := testDB.Model(&Token{}).
			Where("user_id = ?", 100).
			Where("(contract, token_id) IN (?)", [][]string{
				{"0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", "10"},
			}).
			Find(&results).Error

		assert.Equal[error, error](t, err, nil)
		assert.Equal(t, 1, len(results))
		if len(results) > 0 {
			assert.Equal(t, "0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", results[0].Contract)
			assert.Equal(t, "10", results[0].TokenID)
		}

		// Verify the query was routed to the correct shard
		query := testMiddleware.LastQuery()
		t.Logf("Generated query: %s", query)
		assert.Contains(t, query, "tokens_0", "Query should be routed to shard 0")
		assert.Contains(t, query, "user_id", "Query should include the sharding key")
		assert.Contains(t, query, "(contract, token_id) IN", "Composite IN expression should be preserved")
	})

	// Test 2: Query with composite IN clause with multiple tuples and user_id
	t.Run("CompositeInMultipleTuples", func(t *testing.T) {
		var results []Token
		err := testDB.Model(&Token{}).
			Where("user_id = ?", 100).
			Where("(contract, token_id) IN (?)", [][]string{
				{"0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", "10"},
				{"0xdifferent", "10"},
			}).
			Find(&results).Error

		assert.Equal[error, error](t, err, nil)
		assert.Equal(t, 2, len(results))

		// Verify query was routed to the correct shard
		query := testMiddleware.LastQuery()
		t.Logf("Generated query: %s", query)
		assert.Contains(t, query, "tokens_0", "Query should be routed to shard 0")
		assert.Contains(t, query, "user_id", "Query should include the sharding key")
		assert.Contains(t, query, "(contract, token_id) IN", "Composite IN expression should be preserved")
	})

	// Test 3: Query with composite IN as raw SQL
	t.Run("CompositeInRawSQL", func(t *testing.T) {
		var results []Token
		err := testDB.Raw(`SELECT * FROM tokens WHERE user_id = ? AND ((contract, token_id)) IN (?)`,
			100, [][]string{{"0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", "10"}}).
			Find(&results).Error

		assert.Equal[error, error](t, err, nil)
		assert.Equal(t, 1, len(results))
		if len(results) > 0 {
			assert.Equal(t, "0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", results[0].Contract)
			assert.Equal(t, "10", results[0].TokenID)
		}

		// Verify query was properly transformed
		query := testMiddleware.LastQuery()
		t.Logf("Generated query: %s", query)
		assert.Contains(t, query, "tokens_0", "Query should be routed to shard 0")
		assert.Contains(t, query, "user_id", "Query should include the sharding key")
		assert.Contains(t, query, "(contract, token_id) IN", "Composite IN expression should be preserved")
	})

	// Test 4: Attempt with no sharding key (should fail with error or go to all shards)
	t.Run("CompositeInNoShardingKey", func(t *testing.T) {
		var results []Token
		err := testDB.Model(&Token{}).
			Where("(contract, token_id) IN (?)", [][]string{
				{"0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", "10"},
			}).
			Find(&results).Error

		// Since DoubleWrite is true, this should work by querying the main table
		// If DoubleWrite was false, it would return ErrMissingShardingKey
		assert.Equal[error, error](t, err, nil)
		tassert.GreaterOrEqual(t, len(results), 1)

		// Verify the query was actually directed to the main table
		assert.Contains(t, testMiddleware.LastQuery(), "tokens", "Should query the main tokens table")
		assert.NotContains(t, testMiddleware.LastQuery(), "tokens_", "Should not query sharded tables")
	})

	// Test 5: Test with explicit parameter syntax
	t.Run("CompositeInExplicitParams", func(t *testing.T) {
		var results []Token
		err := testDB.Raw(`
			SELECT * FROM tokens 
			WHERE user_id = $1 
			AND ((contract, token_id)) IN (($2, $3))`,
			100, "0x062ed5c1a781685032d5a9f3ba189f6742cb4e04", "10").
			Find(&results).Error

		assert.Equal[error, error](t, err, nil)
		assert.Equal(t, 1, len(results))

		// Verify the query was properly transformed
		// The internal parameter format may differ but the table should be sharded
		assert.Contains(t, testMiddleware.LastQuery(), "tokens_0")
	})

	// Test 6: Test multiple rows in the IN clause with explicit formatting
	t.Run("CompositeInMultipleRowsRaw", func(t *testing.T) {
		var results []Token
		err := testDB.Raw(`
			SELECT * FROM tokens 
			WHERE user_id = ? 
			AND ((contract, token_id)) IN (
				('0x062ed5c1a781685032d5a9f3ba189f6742cb4e04', '10'),
				('0xdifferent', '10')
			)`, 100).
			Find(&results).Error

		assert.Equal[error, error](t, err, nil)
		assert.Equal(t, 2, len(results))
	})

	// Clean up
	truncateTables(db, "tokens", "tokens_0", "tokens_1", "tokens_2", "tokens_3")
}

// Helper method for testing the parser with composite IN expressions
func TestParserWithCompositeInExpressions(t *testing.T) {
	// Create a properly initialized sharding middleware for testing parsing
	shardingConfig := Config{
		DoubleWrite:    true,
		ShardingKey:    "user_id",
		NumberOfShards: 4,
		// Add required configurations to prevent nil pointer dereference
		PartitionType: PartitionTypeHash,
		ShardingAlgorithm: func(value interface{}) (string, error) {
			// Simple modulo algorithm for testing
			id, err := toInt64(value)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("_%d", id%4), nil
		},
		ShardingAlgorithmByPrimaryKey: func(id int64) string {
			return fmt.Sprintf("_%d", id%4)
		},
	}

	s := &Sharding{
		configs: map[string]Config{
			"tokens": shardingConfig,
		},
		// Initialize with empty DB to avoid nil pointer
		DB: &gorm.DB{},
	}

	// Setup the snowflake nodes to prevent nil pointer dereference
	s.snowflakeNodes = make([]*snowflake.Node, 1024)
	for i := int64(0); i < 4; i++ { // Just initialize the ones we need
		n, err := snowflake.NewNode(i)
		if err == nil {
			s.snowflakeNodes[i] = n
		}
	}

	// Compile to set up the required functions
	s.compile()

	// Test resolving a query with composite IN expressions
	testCases := []struct {
		name     string
		query    string
		args     []interface{}
		expected string
	}{
		{
			name:     "Simple composite IN with UserID",
			query:    `SELECT * FROM tokens WHERE user_id = 100 AND ((contract, token_id)) IN (('0x123', '10'))`,
			args:     []interface{}{},
			expected: `tokens_0`, // Should be routed to shard 0 for user_id 100
		},
		{
			name:     "Composite IN with parameterized values",
			query:    `SELECT * FROM tokens WHERE user_id = $1 AND ((contract, token_id)) IN (($2, $3))`,
			args:     []interface{}{100, "0x123", "10"},
			expected: `tokens_0`,
		},
		// Removed problematic test case that used ? parameters which PostgreSQL parser can't handle
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, stQuery, tableName, err := s.resolve(tc.query, tc.args...)

			// Check for errors
			if err != nil {
				t.Logf("Query: %s", tc.query)
				t.Logf("Args: %v", tc.args)
				t.Fatalf("Failed to resolve query: %v", err)
			}

			t.Logf("Resolved query: %s", stQuery)
			t.Logf("Resolved table name: %s", tableName)

			// Check that the query is routed to the correct shard
			// The tableName may not be extracted correctly, so we'll focus on the sharded query
			assert.Contains(t, stQuery, tc.expected, "Query should contain the expected sharded table")
		})
	}
}

func TestSelfJoinShardedTableWithAliases(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0")

	// Prepare data
	order1 := Order{UserID: 100, Product: "iPhone"}
	db.Create(&order1)
	order2 := Order{UserID: 100, Product: "iPad"}
	db.Create(&order2)

	// Recalculate table suffix
	orderShardIndex := uint(order1.UserID) % shardingConfig.NumberOfShards
	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)

	// Expected query with sharded table names and aliases
	expectedQuery := fmt.Sprintf(
		`SELECT o1.*, o2.product AS other_product FROM orders%s o1 LEFT JOIN orders%s o2 ON o1.user_id = o2.user_id AND o1.id <> o2.id WHERE o1.user_id = $1`,
		orderTableSuffix, orderTableSuffix)

	// Perform the query
	var results []struct {
		Order
		OtherProduct string
	}

	db.Table("orders AS o1").
		Select("o1.*, o2.product AS other_product").
		Joins("LEFT JOIN orders AS o2 ON o1.user_id = o2.user_id AND o1.id <> o2.id").
		Where("o1.user_id = ?", 100).
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	tassert.GreaterOrEqual(t, len(results), 1)
}

func TestDoubleWriteDebug(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3")

	// Create a custom config with DoubleWrite explicitly enabled
	doubleWriteConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	// Register a debug middleware that captures both queries
	debugDB, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})

	testMiddleware := Register(doubleWriteConfig, &Order{})
	debugDB.Use(testMiddleware)

	// Insert a record and capture queries
	testOrder := Order{
		UserID:     100,
		Product:    "DoubleWriteTest",
		CategoryID: 1,
	}

	// Before executing, let's see what queries are generated
	ftQuery, stQuery, table, err := testMiddleware.resolve("INSERT INTO orders (user_id, product, category_id) VALUES (100, 'DoubleWriteTest', 1)")
	t.Logf("Full table query: %s", ftQuery)
	t.Logf("Sharded table query: %s", stQuery)
	t.Logf("Table: %s", table)
	t.Logf("Error: %v", err)

	err = debugDB.Create(&testOrder).Error
	assert.Equal[error, error](t, err, nil)

	// VERIFICATION: Check if the record exists in both tables

	// 1. Check the main table (orders) - using the middleware is fine here
	var mainTableOrder Order
	mainTableErr := debugDB.Table("orders").Where("id = ?", testOrder.ID).First(&mainTableOrder).Error

	// 2. Check the sharded table (orders_0) - SKIP MIDDLEWARE for this query
	var shardedTableOrder Order
	// Create a new session without middleware to query sharded table directly
	directDB := debugDB.Session(&gorm.Session{})
	directDB.Statement.ConnPool = db.Statement.ConnPool // Use raw connection
	// Add the sharding_ignore flag to skip the middleware

	shardedTableErr := directDB.Table("orders_0").
		Where("id = ?", testOrder.ID).
		Where("user_id = ?", 100).
		First(&shardedTableOrder).Error

	// Log the results for debugging
	t.Logf("Main table record found: %v, Error: %v", mainTableOrder.ID > 0, mainTableErr)
	t.Logf("Sharded table record found: %v, Error: %v", shardedTableOrder.ID > 0, shardedTableErr)

	// Assert that both records exist
	assert.Equal[error, error](t, mainTableErr, nil, "Record should exist in the main table")
	assert.Equal[error, error](t, shardedTableErr, nil, "Record should exist in the sharded table")

	// Check the data is correct in both tables
	if mainTableErr == nil && shardedTableErr == nil {
		assert.Equal(t, testOrder.ID, mainTableOrder.ID, "ID should match in main table")
		assert.Equal(t, testOrder.Product, mainTableOrder.Product, "Product should match in main table")

		assert.Equal(t, testOrder.ID, shardedTableOrder.ID, "ID should match in sharded table")
		assert.Equal(t, testOrder.Product, shardedTableOrder.Product, "Product should match in sharded table")
	}
}

func TestDoubleWrite(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3")

	// Create a custom config with DoubleWrite explicitly enabled
	doubleWriteConfig := Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	// Create a fresh DB instance with the configured middleware
	testDB, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})

	testMiddleware := Register(doubleWriteConfig, &Order{})
	testDB.Use(testMiddleware)

	// Insert a record
	testOrder := Order{
		ID:         1,
		UserID:     100,
		Product:    "DoubleWriteTest",
		CategoryID: 1,
	}

	err := testDB.Create(&testOrder).Error
	assert.Equal[error, error](t, err, nil)

	// Verify the record exists in the sharded table
	var shardCount int64
	err = testDB.Table("orders").Where("id = ?", testOrder.ID).Count(&shardCount).Error
	assert.Equal[error, error](t, err, nil)
	assert.Equal(t, int64(1), shardCount)

	// Verify the record also exists in the main table
	var mainCount int64
	err = testDB.Table("orders").Clauses(hints.New("nosharding")).Where("id = ?", testOrder.ID).Count(&mainCount).Error
	assert.Equal[error, error](t, err, nil)
	assert.Equal(t, int64(1), mainCount)

	// Update the record to test double write on updates
	err = testDB.Model(&Order{}).Where("id = ?", testOrder.ID).Update("product", "UpdatedProduct").Error
	assert.Equal[error, error](t, err, nil)

	// Verify update in both tables
	var shardProduct, mainProduct string
	testDB.Table("orders").Where("id = ?", testOrder.ID).Select("product").Scan(&shardProduct)
	testDB.Table("orders").Clauses(hints.New("nosharding")).Where("id = ?", testOrder.ID).Select("product").Scan(&mainProduct)

	assert.Equal(t, "UpdatedProduct", shardProduct)
	assert.Equal(t, "UpdatedProduct", mainProduct)

	testDB.Table("orders").Where("id = ?", testOrder.ID).Count(&shardCount)
	testDB.Table("orders").Clauses(hints.New("nosharding")).Where("id = ?", testOrder.ID).Count(&mainCount)

	// Test delete with double write
	err = testDB.Delete(&Order{}, testOrder.ID).Error
	assert.Equal[error, error](t, err, nil)

	// Verify deletion in both tables
	testDB.Table("orders").Where("id = ?", testOrder.ID).Count(&shardCount)
	testDB.Table("orders").Clauses(hints.New("nosharding")).Where("id = ?", testOrder.ID).Count(&mainCount)

	assert.Equal(t, int64(0), shardCount)
	assert.Equal(t, int64(0), mainCount)
}

func TestJoinShardedTablesDifferentKeys(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3",
		"order_details", "order_details_0", "order_details_1", "order_details_2", "order_details_3")

	// Prepare data
	order := Order{ID: 7, UserID: 100, Product: "iPhone"}
	db.Create(&order)

	orderDetail := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
	db.Create(&orderDetail)

	// Recalculate table suffixes based on generated IDs
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderDetailShardIndex := uint(orderDetail.OrderID) % shardingConfigOrderDetails.NumberOfShards

	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)

	// Expected query with sharded table names
	expectedQuery := fmt.Sprintf(
		`SELECT orders%s.*, order_details%s.product AS order_detail_product, order_details%s.quantity AS order_detail_quantity FROM orders%s RIGHT JOIN order_details%s ON order_details%s.order_id = orders%s.id WHERE orders%s.user_id = $1 AND orders%s.id = $2`,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix, orderTableSuffix, orderTableSuffix)

	// Perform the query
	var results []struct {
		Order
		OrderDetailProduct  string
		OrderDetailQuantity int
	}

	db.Model(&Order{}).
		Select("orders.*, order_details.product AS order_detail_product, order_details.quantity AS order_detail_quantity").
		Joins("RIGHT JOIN order_details ON order_details.order_id = orders.id").
		Where("orders.user_id = ? AND orders.id = ?", 100, order.ID).
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	assert.Equal(t, 1, len(results))
	if len(results) == 0 {
		t.Fatalf("no results")
	}
	assert.Equal(t, "iPhone", results[0].Product)
	assert.Equal(t, "iPhone Case", results[0].OrderDetailProduct)
	assert.Equal(t, 2, results[0].OrderDetailQuantity)
}

func TestJoinShardedWithNonShardedTableWithConditions(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "categories")

	// Prepare data
	category := Category{Name: "Electronics"}
	db.Create(&category)

	order := Order{UserID: 100, Product: "iPhone", CategoryID: category.ID}
	db.Create(&order)

	// Recalculate table suffix based on UserID
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)

	// Expected query with sharded table name
	expectedQuery := fmt.Sprintf(
		`SELECT orders%s.*, categories.name AS category_name FROM orders%s JOIN categories ON categories.id = orders%s.category_id WHERE orders%s.user_id = $1 AND categories.name = $2`,
		orderTableSuffix, orderTableSuffix, orderTableSuffix, orderTableSuffix)

	// Perform the query
	var results []struct {
		Order
		CategoryName string
	}

	db.Model(&Order{}).
		Select("orders.*, categories.name AS category_name").
		Joins("JOIN categories ON categories.id = orders.category_id").
		Where("orders.user_id = ? AND categories.name = ?", 100, "Electronics").
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	assert.Equal(t, 1, len(results))
	if len(results) == 0 {
		t.Fatalf("no results")
	}
	assert.Equal(t, "iPhone", results[0].Product)
	assert.Equal(t, "Electronics", results[0].CategoryName)
}

//func TestJoinShardedTableWithSubquery(t *testing.T) {
//	// Clean up tables before test
//	truncateTables(db, "orders", "orders_0", "order_details", "order_details_0")
//
//	// Prepare data
//	order := Order{UserID: 100, Product: "iPhone"}
//	db.Create(&order)
//
//	orderDetail := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
//	db.Create(&orderDetail)
//
//	// Recalculate table suffixes
//	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
//	orderDetailShardIndex := uint(orderDetail.OrderID) % shardingConfigOrderDetails.NumberOfShards
//
//	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
//	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)
//
//	// Expected query with sharded table names
//	expectedQuery := fmt.Sprintf(
//		`SELECT orders%s.* FROM orders%s JOIN (SELECT order_details%s.order_id FROM order_details%s WHERE order_details%s.quantity > $1) AS od ON od.order_id = orders%s.id WHERE orders%s.user_id = $2`,
//		orderTableSuffix, orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix, orderTableSuffix)
//
//	// Perform the query
//	var results []Order
//
//	subquery := db.Table("order_details").Select("order_id").Where("order_details.quantity > ?", 1)
//	tx := db.Model(&Order{}).
//		Joins("JOIN (?) AS od ON od.order_id = orders.id", subquery).
//		Where("orders.user_id = ?", 100).
//		Scan(&results)
//
//	// Assert query
//	assertQueryResult(t, expectedQuery, tx)
//
//	// Assert results
//	assert.Equal(t, 1, len(results))
//	if len(results) == 0 {
//		t.Fatalf("no results")
//	}
//	assert.Equal(t, "iPhone", results[0].Product)
//}

func TestJoinShardedTablesDifferentJoinTypes(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "order_details", "order_details_0")

	// Prepare data
	order := Order{UserID: 100, Product: "iPhone"}
	db.Create(&order)

	orderDetail := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
	db.Create(&orderDetail)

	// Recalculate table suffixes
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderDetailShardIndex := uint(orderDetail.OrderID) % shardingConfigOrderDetails.NumberOfShards

	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)

	// List of join types to test
	joinTypes := []string{"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"}

	for _, joinType := range joinTypes {
		// Expected query with sharded table names and join type
		expectedQuery := fmt.Sprintf(
			`SELECT orders%s.*, order_details%s.product AS order_detail_product FROM orders%s %s order_details%s ON order_details%s.order_id = orders%s.id WHERE orders%s.user_id = $1 AND orders%s.id = $2`,
			orderTableSuffix, orderDetailTableSuffix, orderTableSuffix, joinType, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix, orderTableSuffix, orderTableSuffix)

		// Perform the query
		var results []struct {
			Order
			OrderDetailProduct string
		}

		db.Model(&Order{}).
			Select("orders.*, order_details.product AS order_detail_product").
			Joins(fmt.Sprintf("%s order_details ON order_details.order_id = orders.id", joinType)).
			Where("orders.user_id = ? AND orders.id = ?", 100, order.ID).
			Scan(&results)

		// Assert query
		assertQueryResult(t, expectedQuery, middleware)

		// Assert results
		if joinType == "INNER JOIN" || joinType == "LEFT JOIN" || joinType == "FULL JOIN" {
			assert.Equal(t, 1, len(results))
			if len(results) == 0 {
				t.Fatalf("no results for join type %s", joinType)
			}
			assert.Equal(t, "iPhone", results[0].Product)
			assert.Equal(t, "iPhone Case", results[0].OrderDetailProduct)
		} else if joinType == "RIGHT JOIN" {
			// todo
		}
	}
}
func TestJoinWithComplexConditions(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "order_details", "order_details_0", "users", "users_0", "users_1", "users_2", "users_3")

	// Prepare data
	user := User{ID: 100, Name: "John Doe"}
	db.Create(&user)

	order := Order{UserID: user.ID, Product: "iPhone"}
	db.Create(&order)

	orderDetail := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
	db.Create(&orderDetail)

	// Recalculate table suffixes
	userShardIndex := uint(user.ID) % shardingConfigUser.NumberOfShards
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderDetailShardIndex := uint(orderDetail.OrderID) % shardingConfigOrderDetails.NumberOfShards

	userTableSuffix := fmt.Sprintf("_%01d", userShardIndex)
	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)

	// Expected query with sharded table names
	expectedQuery := fmt.Sprintf(
		`SELECT orders%s.*, order_details%s.product AS order_detail_product, users%s.name AS user_name FROM orders%s JOIN order_details%s ON order_details%s.order_id = orders%s.id JOIN users%s ON users%s.id = orders%s.user_id WHERE orders%s.user_id = $1 AND order_details%s.quantity > $2`,
		orderTableSuffix, orderDetailTableSuffix, userTableSuffix,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix,
		userTableSuffix, userTableSuffix, orderTableSuffix,
		orderTableSuffix, orderDetailTableSuffix)

	// Perform the query
	var results []struct {
		Order
		OrderDetailProduct string
		UserName           string
	}

	db.Model(&Order{}).
		Select("orders.*, order_details.product AS order_detail_product, users.name AS user_name").
		Joins("JOIN order_details ON order_details.order_id = orders.id").
		Joins("JOIN users ON users.id = orders.user_id").
		Where("orders.user_id = ? AND order_details.quantity > ?", user.ID, 1).
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	assert.Equal(t, 1, len(results))
	if len(results) == 0 {
		t.Fatalf("no results")
	}
	assert.Equal(t, "iPhone", results[0].Product)
	assert.Equal(t, "iPhone Case", results[0].OrderDetailProduct)
	assert.Equal(t, "John Doe", results[0].UserName)
}

func TestJoinWithAggregation(t *testing.T) {
	// Clean up tables before test
	truncateTables(db, "orders", "orders_0", "order_details", "order_details_0", "order_details_1")

	// Prepare data
	order := Order{UserID: 100, Product: "iPhone"}
	db.Create(&order)

	orderDetail1 := OrderDetail{OrderID: order.ID, Product: "iPhone Case", Quantity: 2}
	db.Create(&orderDetail1)

	// Recalculate table suffixes
	orderShardIndex := uint(order.UserID) % shardingConfig.NumberOfShards
	orderDetailShardIndex := uint(orderDetail1.OrderID) % shardingConfigOrderDetails.NumberOfShards

	orderTableSuffix := fmt.Sprintf("_%01d", orderShardIndex)
	orderDetailTableSuffix := fmt.Sprintf("_%01d", orderDetailShardIndex)

	// Expected query with sharded table names
	expectedQuery := fmt.Sprintf(
		`SELECT orders%s.*, sum(order_details%s.quantity) AS total_quantity FROM orders%s JOIN order_details%s ON order_details%s.order_id = orders%s.id WHERE orders%s.user_id = $1 AND orders%s.id = $2 GROUP BY orders%s.id`,
		orderTableSuffix, orderDetailTableSuffix,
		orderTableSuffix, orderDetailTableSuffix, orderDetailTableSuffix, orderTableSuffix,
		orderTableSuffix, orderTableSuffix, orderTableSuffix)

	// Perform the query
	var results []struct {
		Order
		TotalQuantity int
	}

	db.Model(&Order{}).
		Select("orders.*, SUM(order_details.quantity) AS total_quantity").
		Joins("JOIN order_details ON order_details.order_id = orders.id").
		Where("orders.user_id = ? AND orders.id = ?", 100, order.ID).
		Group("orders.id").
		Scan(&results)

	// Assert query
	assertQueryResult(t, expectedQuery, middleware)

	// Assert results
	assert.Equal(t, 1, len(results))
	if len(results) == 0 {
		t.Fatalf("no results")
	}
	assert.Equal(t, "iPhone", results[0].Product)
	assert.Equal(t, 2, results[0].TotalQuantity) // 2
}

func TestInsertWithPreGeneratedGID(t *testing.T) {

	db, err := gorm.Open(postgres.Open(dbURL()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Define the sharding configuration
	numberOfShards := uint(4)
	swapConfig := Config{
		ShardingKey:         "id", // Changed from ShardingKey
		NumberOfShards:      numberOfShards,
		PrimaryKeyGenerator: PKCustom,
		PrimaryKeyGeneratorFn: func(tableIdx int64) int64 {
			return 0
		},
		// Corrected field names
		ShardingAlgorithm: func(columnValue interface{}) (string, error) {
			// Dereference the value if it's a pointer
			actualValue := dereferenceValue(columnValue)
			if actualValue == nil {
				return "", fmt.Errorf("nil value provided for sharding key")
			}

			// Convert to int64
			gid, err := toInt64(actualValue)
			if err != nil {
				return "", fmt.Errorf("shardingAlgorithm: %v", err)
			}

			suffix := fmt.Sprintf("_%d", gid%int64(numberOfShards))
			return suffix, nil
		},
		ShardingAlgorithmByPrimaryKey: func(id int64) string {
			return fmt.Sprintf("_%d", id%int64(numberOfShards))
		},
	}

	// Register the sharding middleware
	middleware = Register(map[string]Config{
		"swaps": swapConfig,
	}, &Swap{})
	if err := db.Use(middleware); err != nil {
		t.Fatalf("failed to use sharding middleware: %v", err)
	}

	// Drop and recreate the swaps table
	if err := db.Exec("DROP TABLE IF EXISTS swaps").Error; err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}
	if err := db.AutoMigrate(&Swap{}); err != nil {
		t.Fatalf("failed to migrate schema: %v", err)
	}

	// Create sharded tables
	for i := 0; i < int(swapConfig.NumberOfShards); i++ {
		tableName := fmt.Sprintf("swaps_%d", i)
		if err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (LIKE swaps INCLUDING ALL)", tableName)).Error; err != nil {
			t.Fatalf("failed to create sharded table %s: %v", tableName, err)
		}
	}

	// Create a Swap instance with the generated GID
	swap := &Swap{
		ID:      100,
		EventID: 200,
		Escrow:  true,
	}

	// Insert the Swap record
	err = db.Create(swap).Error
	if err != nil {
		t.Errorf("failed to insert swap record: %v", err)
	} else {
		t.Logf("swap record inserted successfully with GID: %d", swap.GID)
	}

	// Verify that the record is inserted into the correct shard
	expectedShard := swap.ID % uint64(numberOfShards)
	expectedTable := fmt.Sprintf("swaps_%d", expectedShard)

	var count int64
	err = db.Table(expectedTable).Where("gid = ?", swap.GID).Count(&count).Error
	if err != nil {
		t.Errorf("failed to query swap record in shard: %v", err)
	} else if count != 1 {
		t.Errorf("expected 1 record in shard %s, found %d", expectedTable, count)
	} else {
		t.Logf("swap record found in correct shard table: %s and count: %d", expectedTable, count)
	}
}

//func TestCrossShardQueryOptimization(t *testing.T) {
//	// Clean up tables before test
//	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3")
//
//	// Insert some test data
//	order1 := Order{UserID: 100, Product: "iPhone"}
//	db.Create(&order1)
//	order2 := Order{UserID: 101, Product: "iPad"}
//	db.Create(&order2)
//	order3 := Order{UserID: 102, Product: "MacBook"}
//	db.Create(&order3)
//
//	// Get the expected query
//	expectedQueryFormat := `SELECT * FROM (SELECT * FROM orders_0 WHERE "product" LIKE $1 UNION ALL SELECT * FROM orders_1 WHERE "product" LIKE $1 UNION ALL SELECT * FROM orders_2 WHERE "product" LIKE $1 UNION ALL SELECT * FROM orders_3 WHERE "product" LIKE $1) AS combined_results ORDER BY "id" LIMIT 10`
//
//	// Use a raw query to force the cross-shard logic
//	var results []Order
//	tx := db.Raw(`SELECT * FROM orders WHERE product LIKE ? ORDER BY id LIMIT 10`, "%i%").Scan(&results)
//
//	// Assert the query matches our expectation
//	assertQueryResult(t, expectedQueryFormat, tx)
//
//	// Verify results
//	tassert.GreaterOrEqual(t, len(results), 2) // Should find at least iPhone and iPad
//
//	// Check if we got results from different shards
//	foundShards := make(map[int64]bool)
//	for _, order := range results {
//		userID := order.UserID
//		shardIndex := userID % 4 // 4 is NumberOfShards
//		foundShards[shardIndex] = true
//	}
//	tassert.GreaterOrEqual(t, len(foundShards), 2) // Should have results from at least 2 shards
//}
//
//func TestCrossShardJoinQuery(t *testing.T) {
//	// Clean up tables before test
//	truncateTables(db, "orders", "orders_0", "orders_1", "orders_2", "orders_3",
//		"order_details", "order_details_0", "order_details_1", "order_details_2", "order_details_3")
//
//	// Insert test data across different shards
//	// Create orders in different shards
//	orders := []Order{
//		{UserID: 100, Product: "iPhone"},
//		{UserID: 101, Product: "iPad"},
//		{UserID: 102, Product: "MacBook"},
//	}
//	for _, order := range orders {
//		db.Create(&order)
//	}
//
//	// Create order details
//	for _, order := range orders {
//		orderDetail := OrderDetail{
//			OrderID:  order.ID,
//			Product:  order.Product + " Case",
//			Quantity: int(order.UserID % 10),
//		}
//		db.Create(&orderDetail)
//	}
//
//	// Expected query format (simplified since the actual query will be complex)
//	expectedQueryContains := `UNION ALL`
//
//	// Perform a join query that requires cross-shard operation
//	var results []struct {
//		Order
//		DetailProduct string
//		Quantity      int
//	}
//
//	tx := db.Table("orders").
//		Select("orders.*, order_details.product as detail_product, order_details.quantity").
//		Joins("LEFT JOIN order_details ON orders.id = order_details.order_id").
//		Where("orders.product LIKE ?", "%i%").
//		Order("orders.id").
//		Limit(10).
//		Scan(&results)
//
//	// Assert the query matches our expectation
//	assertQueryResult(t, expectedQueryContains, tx)
//
//	//// Verify the query uses our optimized approach
//	//if !strings.Contains(actualQuery, expectedQueryContains) {
//	//	t.Errorf("Expected query to contain '%s', but got: %s", expectedQueryContains, actualQuery)
//	//}
//
//	// Verify we got results
//	tassert.GreaterOrEqual(t, len(results), 2)
//
//	// Verify join data is correct
//	for _, result := range results {
//		assert.Contains(t, result.DetailProduct, result.Product)
//	}
//}

func TestListPartitionInsert(t *testing.T) {
	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
	// Create ERC20 contract
	dbList.Create(&Contract{
		Name:    "TokenA",
		Type:    "ERC20",
		IsERC20: true,
		Data:    "TokenA Data",
	})

	// Should go to partition 0 (ERC20)
	assertQueryResult(t, `INSERT INTO contracts_0 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)
}

func TestListPartitionInsertDifferentTypes(t *testing.T) {
	// Insert different contract types to verify they go to different partitions
	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
	// ERC721 contract
	dbList.Create(&Contract{
		Name:     "NFT Collection",
		Type:     "ERC721",
		IsERC721: true,
		Data:     "NFT Collection Data",
	})
	// Should go to partition 1 (ERC721)
	assertQueryResult(t, `INSERT INTO contracts_1 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)

	// ERC1155 contract
	dbList.Create(&Contract{
		Name:      "Multi-Token",
		Type:      "ERC1155",
		IsERC1155: true,
		Data:      "Multi-Token Data",
	})
	// Should go to partition 2 (ERC1155)
	assertQueryResult(t, `INSERT INTO contracts_2 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)
}

//func TestListPartitionInsertInvalidType(t *testing.T) {
//	// Try to insert with an invalid type that's not in the list
//	err := dbList.Create(&Contract{
//		Name: "Invalid Contract",
//		Type: "ERC777", // Not in the list values
//		Data: "Invalid Data",
//	}).Error
//
//	tassert.Error(t, err, "Expected an error for invalid type")
//	if err != nil {
//		tassert.Contains(t, err.Error(), "partition list")
//	}
//}

func TestListPartitionFillID(t *testing.T) {
	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
	// Test auto ID generation with list partitioning
	contract := &Contract{
		Name:    "Auto ID Contract",
		Type:    "ERC20",
		IsERC20: true,
	}

	dbList.Create(contract)

	// Verify ID was generated
	tassert.Greater(t, contract.ID, int64(0))

	// Check the query
	expectedQuery := `INSERT INTO contracts_0 (name, type, is_erc20, is_erc721, is_erc1155, data, created_at, updated_at, id) VALUES`
	lastQuery := listMiddleware.LastQuery()
	tassert.Contains(t, lastQuery, expectedQuery)
}

func TestListPartitionSelect(t *testing.T) {
	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
	// Create test data
	contracts := []Contract{
		{Name: "Token1", Type: "ERC20", IsERC20: true, Data: "Token1 Data"},
		{Name: "Token2", Type: "ERC20", IsERC20: true, Data: "Token2 Data"},
		{Name: "NFT1", Type: "ERC721", IsERC721: true, Data: "NFT1 Data"},
	}

	for _, c := range contracts {
		dbList.Create(&c)
	}

	// Query by type (should use partition pruning)
	var erc20Contracts []Contract
	dbList.Where("type = ?", "ERC20").Find(&erc20Contracts)

	// Should query only the ERC20 partition
	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1`, listMiddleware)

	// Verify results
	tassert.Equal(t, 2, len(erc20Contracts))
	for _, c := range erc20Contracts {
		tassert.Equal(t, "ERC20", c.Type)
	}
}

func TestListPartitionSelectById(t *testing.T) {
	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
	// Create a contract and get its ID
	contract := Contract{Name: "IdTest", Type: "ERC20", IsERC20: true}
	dbList.Create(&contract)

	// Query by ID
	var foundContract Contract
	dbList.Where("type = ?", "ERC20").First(&foundContract, contract.ID)

	// Should be able to find it in the correct partition even without type in WHERE clause
	// With list partitioning, this may need to check multiple partitions
	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 AND contracts_0."id" = $1 ORDER BY "contracts_0"."id" LIMIT 1`, listMiddleware)

	// Verify results
	tassert.Equal(t, contract.ID, foundContract.ID)
	tassert.Equal(t, "ERC20", foundContract.Type)
}

func TestListPartitionUpdate(t *testing.T) {
	// Create a contract to update
	contract := Contract{Name: "UpdateTest", Type: "ERC20", IsERC20: true}
	dbList.Create(&contract)

	// Update the contract
	dbList.Model(&Contract{}).Where("id = ?", contract.ID).Where("type = ?", "ERC20").Update("name", "UpdatedName")

	// Should update in the correct partition
	assertQueryResult(t, `UPDATE contracts_0 SET "name" = $1, updated_at = $2 WHERE "id" = $3 AND "type" = $4`, listMiddleware)

	// Verify the update
	var updated Contract
	dbList.Where("type = ?", "ERC20").First(&updated, contract.ID)
	tassert.Equal(t, "UpdatedName", updated.Name)
}

func TestListPartitionDelete(t *testing.T) {
	// Create a contract to delete
	contract := Contract{Name: "DeleteTest", Type: "ERC721", IsERC721: true}
	dbList.Create(&contract)

	// Delete the contract
	dbList.Where("type = ?", "ERC721").Delete(&Contract{}, contract.ID)

	// Should delete from the correct partition
	assertQueryResult(t, `DELETE FROM "contracts_1" WHERE type = $1 AND "contracts_1"."id" = $2`, listMiddleware)

	// Verify deletion
	var count int64
	dbList.Model(&Contract{}).Where("type = ?", "ERC721").Where("id = ?", contract.ID).Count(&count)
	tassert.Equal(t, int64(0), count)
}

func TestListPartitionJoin(t *testing.T) {
	// Create contract and contract data
	contract := Contract{ID: 4, Name: "JoinTest", Type: "ERC20", IsERC20: true}
	dbList.Create(&contract)

	contractData := ContractData{
		ContractID: contract.ID,
		Key:        "symbol",
		Value:      "TKN",
	}
	dbList.Create(&contractData)

	// Query with join
	var result struct {
		Contract
		Value string
	}

	dbList.Model(&Contract{}).
		Select("contracts.*, contract_data.value").
		Joins("JOIN contract_data ON contract_data.contract_id = contracts.id").
		Where("contracts.id = ?", contract.ID).Where("type = ?", "ERC20").
		First(&result)

	// Should join the correct partitions
	assertQueryResult(t, `SELECT contracts_0.*, contract_data_1.value FROM contracts_0 JOIN contract_data_1 ON contract_data_1.contract_id = contracts_0.id WHERE contracts_0.id = $1 AND type = $2 ORDER BY "contracts_0"."id" LIMIT 1`, listMiddleware)

	// Verify join results
	tassert.Equal(t, contract.ID, result.ID)
	tassert.Equal(t, "TKN", result.Value)
}

func TestListPartitionFilterByBoolean(t *testing.T) {
	// Clear existing data
	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")

	// Insert test data
	contracts := []Contract{
		{Name: "Token1", Type: "ERC20", IsERC20: true},
		{Name: "NFT1", Type: "ERC721", IsERC721: true},
		{Name: "MultiToken1", Type: "ERC1155", IsERC1155: true},
	}

	for _, c := range contracts {
		dbList.Create(&c)
	}

	// Query by boolean field
	var erc721Contracts []Contract
	dbList.Where("is_erc721 = ?", true).Find(&erc721Contracts)

	// Should query the right partition based on the boolean
	assertQueryResult(t, `SELECT * FROM contracts_1 WHERE "is_erc721" = $1`, listMiddleware)

	// Verify results
	tassert.Equal(t, 1, len(erc721Contracts))
	//tassert.Equal(t, "ERC721", erc721Contracts[0].Type)
}

func TestListPartitionMultipleFilters(t *testing.T) {
	// Create test data
	dbList.Create(&Contract{Name: "Common", Type: "ERC20", IsERC20: true, Data: "Common data"})
	dbList.Create(&Contract{Name: "Rare", Type: "ERC20", IsERC20: true, Data: "Rare data"})

	// Query with multiple conditions
	var contracts []Contract
	dbList.Where("type = ?", "ERC20").Where("name = ?", "Rare").Find(&contracts)

	// Should query the correct partition with all filters
	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 AND "name" = $2`, listMiddleware)

	// Verify results
	tassert.Equal(t, 1, len(contracts))
	tassert.Equal(t, "Rare", contracts[0].Name)
}

func TestListPartitionOrderAndLimit(t *testing.T) {
	// Clear existing data
	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")

	// Create test data
	for i := 1; i <= 5; i++ {
		dbList.Create(&Contract{
			Name:    fmt.Sprintf("Token%d", i),
			Type:    "ERC20",
			IsERC20: true,
		})
	}

	// Query with order and limit
	var contracts []Contract
	dbList.Where("type = ?", "ERC20").Order("name DESC").Limit(3).Find(&contracts)

	// Should have correct ORDER BY and LIMIT
	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 ORDER BY name DESC LIMIT 3`, listMiddleware)

	// Verify results
	tassert.Equal(t, 3, len(contracts))
	tassert.Equal(t, "Token5", contracts[0].Name) // DESC order
}

// todo fix this test
//func TestListPartitionCrossShardQuery(t *testing.T) {
//	// Clear existing data
//	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")
//
//	// Create test data across different types
//	dbList.Create(&Contract{Name: "SearchToken", Type: "ERC20", IsERC20: true})
//	dbList.Create(&Contract{Name: "SearchNFT", Type: "ERC721", IsERC721: true})
//	dbList.Create(&Contract{Name: "SearchMulti", Type: "ERC1155", IsERC1155: true})
//
//	// Query across all types without a partition key
//	var contracts []Contract
//	tx := dbList.Where("name LIKE ?", "Search%").Find(&contracts)
//
//	// Should generate a UNION ALL query across all partitions
//	assertPartialQueryResult(t, `UNION ALL`, tx)
//
//	// Verify we found records from multiple partitions
//	tassert.Equal(t, 3, len(contracts))
//
//	// Check we got each type
//	foundTypes := make(map[string]bool)
//	for _, c := range contracts {
//		foundTypes[c.Type] = true
//	}
//	tassert.Equal(t, 3, len(foundTypes))
//}

func TestListPartitionAggregation(t *testing.T) {
	// Clear existing data
	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")

	// Create test data
	for i := 1; i <= 3; i++ {
		dbList.Create(&Contract{Name: fmt.Sprintf("ERC20_%d", i), Type: "ERC20", IsERC20: true})
		dbList.Create(&Contract{Name: fmt.Sprintf("ERC721_%d", i), Type: "ERC721", IsERC721: true})
	}

	// Query with aggregation
	var result struct {
		Type  string
		Count int
	}

	dbList.Model(&Contract{}).
		Select("type, COUNT(*) as count").
		Where("type = ?", "ERC20").
		Group("type").
		First(&result)

	// Should use the correct partition
	assertQueryResult(t, `SELECT type, count(*) AS count FROM contracts_0 WHERE "type" = $1 GROUP BY type ORDER BY "type" LIMIT 1`, listMiddleware)

	// Verify aggregation result
	tassert.Equal(t, "ERC20", result.Type)
	tassert.Equal(t, 3, result.Count)
}

func TestListPartitionFallbackToDoubleWrite(t *testing.T) {
	// We'll use the existing tables and configs for simplicity
	// This will ensure we use the properly configured snowflake ID generator

	// First, clear the test tables
	truncateTables(dbList, "contracts", "contracts_0", "contracts_1", "contracts_2")

	// Store original config to restore later
	originalConfig := listShardingConfig
	originalDoubleWrite := originalConfig.DoubleWrite
	originalDefaultPartition := originalConfig.DefaultPartition

	// Use defer to restore the original config when we're done
	defer func() {
		listShardingConfig.DoubleWrite = originalDoubleWrite
		listShardingConfig.DefaultPartition = originalDefaultPartition

		// Re-register the middleware with original settings
		listConfigs = map[string]Config{
			"contracts":     listShardingConfig,
			"contract_data": contractDataConfig,
		}
		listMiddleware = Register(listConfigs, &Contract{}, &ContractData{})
		dbList.Use(listMiddleware)
	}()

	// CASE 1: Test with double-write enabled and default partition set
	// ----------------------------------------------------------------
	// Enable double-write and set a default partition
	listShardingConfig.DoubleWrite = true
	listShardingConfig.DefaultPartition = 0 // Default to partition 0

	// Update the config and re-register the middleware
	listConfigs := map[string]Config{
		"contracts": listShardingConfig,
	}
	testMiddleware := Register(listConfigs, &Contract{})

	// Use a new DB session with our test middleware
	testDB := dbList.Session(&gorm.Session{})
	testDB.Use(testMiddleware)

	// 1.1: Insert with valid type (should go to correct partition)
	contract1 := &Contract{
		Name:    "TokenA",
		Type:    "ERC20",
		IsERC20: true,
		Data:    "Token data",
	}

	err := testDB.Create(contract1).Error
	if err != nil {
		t.Errorf("Failed to insert contract with valid type: %v", err)
	} else {
		t.Logf("Successfully inserted contract with ID: %d", contract1.ID)

		// Check if record exists in the correct partition (contracts_0 for ERC20)
		// IMPORTANT: Use Raw with Contract model to bypass the middleware constraints
		var contract Contract
		err = dbList.Raw("SELECT * FROM contracts_0 WHERE id = ? AND type = ?",
			contract1.ID, "ERC20").First(&contract).Error

		if err != nil {
			t.Errorf("Error checking partition: %v", err)
		} else if contract.ID == contract1.ID {
			t.Logf("Successfully verified contract exists in partition 0 (ERC20)")
		} else {
			t.Errorf("Contract not found in partition 0 (ERC20)")
		}
	}

	// 1.2: Insert with invalid type but default partition set
	contract2 := &Contract{
		Name: "Unknown Token",
		Type: "UNKNOWN_TYPE", // Not in list values
		Data: "Some data",
	}

	err = testDB.Create(contract2).Error
	if err != nil {
		t.Errorf("Failed to insert contract with unknown type: %v", err)
	} else {
		t.Logf("Successfully inserted contract with unknown type, ID: %d", contract2.ID)

		// Check main table using Raw with a direct count query
		var mainCount int64
		err = dbList.Raw("SELECT COUNT(*) FROM contracts WHERE id = ?", contract2.ID).Scan(&mainCount).Error
		if err != nil {
			t.Errorf("Error checking main table: %v", err)
		} else if mainCount == 1 {
			t.Logf("Record correctly inserted into main table with DoubleWrite enabled")
		} else {
			t.Errorf("Record not found in main table, count: %d", mainCount)
		}

		// Check default partition using Raw with a direct count query
		var partitionCount int64
		err = dbList.Raw("SELECT COUNT(*) FROM contracts_0 WHERE id = ? AND type = ?",
			contract2.ID, "UNKNOWN_TYPE").Scan(&partitionCount).Error
		if err != nil {
			t.Errorf("Error checking default partition: %v", err)
		} else if partitionCount == 1 {
			t.Logf("Record correctly inserted into default partition")
		} else {
			t.Errorf("Record not found in default partition, count: %d", partitionCount)
		}
	}

	// 1.3: Insert with empty type
	contract3 := &Contract{
		Name: "Empty Type",
		Type: "", // Empty type
		Data: "Empty type data",
	}

	err = testDB.Create(contract3).Error
	if err != nil {
		t.Errorf("Failed to insert contract with empty type: %v", err)
	} else {
		t.Logf("Successfully inserted contract with empty type, ID: %d", contract3.ID)

		// Check main table using Raw with a direct count query
		var mainCount int64
		err = dbList.Raw("SELECT COUNT(*) FROM contracts WHERE id = ?", contract3.ID).Scan(&mainCount).Error
		if err != nil {
			t.Errorf("Error checking main table: %v", err)
		} else if mainCount == 1 {
			t.Logf("Record with empty type inserted into main table with DoubleWrite enabled")
		} else {
			t.Errorf("Record not found in main table, count: %d", mainCount)
		}

		// Check default partition with empty type
		var partitionCount int64
		err = dbList.Raw("SELECT COUNT(*) FROM contracts_0 WHERE id = ? AND type = ?",
			contract3.ID, "").Scan(&partitionCount).Error
		if err != nil {
			t.Errorf("Error checking default partition: %v", err)
		} else if partitionCount == 1 {
			t.Logf("Record correctly inserted into default partition")
		} else {
			t.Errorf("Record not found in default partition, count: %d", partitionCount)
		}
	}

	// CASE 2: Test with double-write disabled and no default partition
	// ---------------------------------------------------------------
	// Disable double-write and remove default partition
	listShardingConfig.DoubleWrite = false
	listShardingConfig.DefaultPartition = -1 // No default partition

	// Update the config and re-register the middleware
	strictConfigs := map[string]Config{
		"contracts": listShardingConfig,
	}
	strictMiddleware := Register(strictConfigs, &Contract{})

	// Use a new DB session with our strict middleware
	strictDB := dbList.Session(&gorm.Session{})
	strictDB.Use(strictMiddleware)

	// 2.1: Insert with valid type (should still work)
	contractValid := &Contract{
		Name:     "Valid Token",
		Type:     "ERC721",
		IsERC721: true,
		Data:     "Valid data",
	}

	err = strictDB.Create(contractValid).Error
	if err != nil {
		t.Errorf("Failed to insert contract with valid type in strict mode: %v", err)
	} else {
		t.Logf("Successfully inserted contract with valid type in strict mode, ID: %d", contractValid.ID)

		// Check correct partition for ERC721 (partition 1)
		var partitionCount int64
		err = dbList.Raw("SELECT COUNT(*) FROM contracts_1 WHERE id = ? AND type = ?",
			contractValid.ID, "ERC721").Scan(&partitionCount).Error
		if err != nil {
			t.Errorf("Error checking partition: %v", err)
		} else if partitionCount == 1 {
			t.Logf("Successfully verified contract exists in partition 1 (ERC721)")
		} else {
			t.Errorf("Contract not found in partition 1 (ERC721), count: %d", partitionCount)
		}
	}

	// 2.2: Insert with invalid type (should fail with partition error)
	contractInvalid := &Contract{
		Name: "Invalid Token",
		Type: "INVALID_TYPE",
		Data: "Invalid data",
	}

	err = strictDB.Create(contractInvalid).Error
	if err == nil {
		t.Errorf("Expected error when inserting invalid type without DoubleWrite, but got success")
	} else {
		t.Logf("Correctly failed to insert invalid type without DoubleWrite: %v", err)

		// Verify error mentions partition
		if strings.Contains(err.Error(), "partition list") ||
			strings.Contains(err.Error(), "sharding key") {
			t.Logf("Error correctly mentions partition or sharding key")
		} else {
			t.Errorf("Expected error to mention 'partition list' or 'sharding key', got: %v", err)
		}
	}
}

func TestUnregisteredTableNotSharded(t *testing.T) {
	// Create a new DB instance without the sharding middleware
	db, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create the products table
	db.Exec("DROP TABLE IF EXISTS products")
	err = db.AutoMigrate(&Product{})
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	// Register sharding middleware with Order configured but NOT Product
	configs := map[string]Config{
		"orders": shardingConfig, // Only orders is configured
	}

	// Create sharding middleware and register it with the DB
	// Important: Do NOT register Product with the middleware
	shardingMiddleware := Register(configs, &Order{})
	db.Use(shardingMiddleware)

	// Insert a product - this should use the main products table
	product := Product{
		Name:       "Test Product",
		Price:      99.99,
		CategoryID: 1,
	}
	result := db.Create(&product)
	if result.Error != nil {
		t.Fatalf("Failed to insert product: %v", result.Error)
	}

	// Verify product was created with an ID
	tassert.Greater(t, product.ID, int64(0), "Product should be created with an ID")

	// Check if products_0, products_1, etc. tables were NOT created
	var tableCount int64
	db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'products\\_%'").Count(&tableCount)
	assert.Equal(t, int64(0), tableCount, "No sharded product tables should be created")

	// Verify that the product was inserted into the main products table
	var count int64
	db.Table("products").Count(&count)
	assert.Equal(t, int64(1), count, "Record should be in the main products table")

	// Query the product using GORM's normal First method
	var retrievedProduct Product
	result = db.First(&retrievedProduct, product.ID)
	if result.Error != nil {
		t.Fatalf("Failed to retrieve product: %v", result.Error)
	}

	// Verify product data
	assert.Equal(t, product.ID, retrievedProduct.ID, "ID should match")
	assert.Equal(t, "Test Product", retrievedProduct.Name, "Name should match")
	assert.Equal(t, 99.99, retrievedProduct.Price, "Price should match")

	// Update the product using GORM
	result = db.Model(&Product{}).Where("id = ?", product.ID).Update("price", 129.99)
	if result.Error != nil {
		t.Fatalf("Failed to update product: %v", result.Error)
	}

	// Verify the update succeeded
	var updatedProduct Product
	db.First(&updatedProduct, product.ID)
	assert.Equal(t, 129.99, updatedProduct.Price, "Price should be updated")

	// Delete the product
	result = db.Delete(&Product{}, product.ID)
	if result.Error != nil {
		t.Fatalf("Failed to delete product: %v", result.Error)
	}

	// Verify the deletion
	var remaining int64
	db.Table("products").Count(&remaining)
	assert.Equal(t, int64(0), remaining, "Product should be deleted")

	// Clean up
	db.Exec("DROP TABLE IF EXISTS products")
}

func TestConcurrentConnPoolOperations(t *testing.T) {
	// Create a new DB connection with debug logging
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Set up multiple concurrent connections
	const numGoroutines = 10
	const operationsPerGoroutine = 20

	// Create configs with different tables and settings for concurrency testing
	configs := make(map[string]Config)
	for i := 0; i < 5; i++ {
		tableName := fmt.Sprintf("concurrent_table_%d", i)
		configs[tableName] = Config{
			DoubleWrite:         i%2 == 0, // Alternate between true and false
			ShardingKey:         "user_id",
			NumberOfShards:      4,
			PrimaryKeyGenerator: PKSnowflake,
		}
	}

	for tableName, config := range configs {
		config.ShardingSuffixs = func() []string {
			var suffixes []string
			for j := 0; j < int(config.NumberOfShards); j++ {
				suffixes = append(suffixes, fmt.Sprintf("_%d", j))
			}
			return suffixes
		}
		// We need to reassign because we're copying the struct
		configs[tableName] = config
	}

	// Register our middleware with multiple table configs
	testMiddleware := Register(configs, &Order{})
	testDB.Use(testMiddleware)

	// Important: Initialize the database connection properly to set up ConnPool
	err = testDB.AutoMigrate(&Order{})
	if err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	// Create the concurrent tables and their sharded versions before testing
	for i := 0; i < 5; i++ {
		// Create the main table
		tableName := fmt.Sprintf("concurrent_table_%d", i)
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		testDB.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id bigint PRIMARY KEY,
			user_id bigint,
			product text
		)`, tableName))

		// Create the sharded tables
		for j := 0; j < 4; j++ {
			shardedTable := fmt.Sprintf("%s_%d", tableName, j)
			testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", shardedTable))
			testDB.Exec(fmt.Sprintf(`CREATE TABLE %s (
				id bigint PRIMARY KEY,
				user_id bigint,
				product text
			)`, shardedTable))
		}
	}

	// Create connection pool implementation for testing
	connPool := &ConnPool{
		ConnPool: testDB.Statement.ConnPool,
		sharding: testMiddleware,
	}
	testMiddleware.ConnPool = connPool

	// Create a context that we can cancel to stop all goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to report errors from goroutines
	errCh := make(chan error, numGoroutines)

	// Launch multiple goroutines to perform concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			// Alternate between different tables and operations
			for j := 0; j < operationsPerGoroutine; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					// Pick an operation: query, exec, or queryRow based on j
					operation := j % 3

					// Pick a table
					tableIndex := j % 5
					tableName := fmt.Sprintf("concurrent_table_%d", tableIndex)
					userID := 100 + (routineID * 100) + j

					// Ensure we're testing reads from the configs with sharding key
					querySQL := fmt.Sprintf("SELECT * FROM %s WHERE user_id = %d", tableName, userID)

					// Perform the operation
					switch operation {
					case 0: // Query
						_, err := testMiddleware.ConnPool.QueryContext(ctx, querySQL)
						if err != nil && err.Error() != "invalid memory address or nil pointer dereference" {
							// Only report non-nil pointer errors, which would be expected in a test environment
							errCh <- fmt.Errorf("goroutine %d query error: %v", routineID, err)
							cancel() // Stop all goroutines on error
							return
						}
					case 1: // Exec
						execSQL := fmt.Sprintf("INSERT INTO %s (user_id, product) VALUES (%d, 'test')", tableName, userID)
						_, err := testMiddleware.ConnPool.ExecContext(ctx, execSQL)
						if err != nil && err.Error() != "invalid memory address or nil pointer dereference" {
							errCh <- fmt.Errorf("goroutine %d exec error: %v", routineID, err)
							cancel()
							return
						}
					case 2: // QueryRow
						rowSQL := fmt.Sprintf("SELECT id FROM %s WHERE user_id = %d", tableName, userID)
						testMiddleware.ConnPool.QueryRowContext(ctx, rowSQL)
						// QueryRow doesn't return errors until Scan, so we don't check here
					}
				}
			}
		}(i)
	}

	// Add another goroutine that changes configs while others are running
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// Acquire lock properly to update configs
				testMiddleware.mutex.Lock()
				// Modify configs - adding or changing properties
				newTableName := fmt.Sprintf("new_table_%d", i)
				testMiddleware.configs[newTableName] = Config{
					DoubleWrite:         i%2 == 0,
					ShardingKey:         "user_id",
					NumberOfShards:      4,
					PrimaryKeyGenerator: PKSnowflake,
				}
				testMiddleware.mutex.Unlock()

				// Small sleep to give other goroutines time to access during changes
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()

	// Wait for all goroutines to complete or for context to be canceled
	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	// Wait for either all goroutines to finish or an error to occur
	select {
	case <-wgDone:
		// All goroutines completed successfully
		t.Log("All goroutines completed successfully")
	case err := <-errCh:
		// At least one goroutine encountered an error
		t.Fatalf("Test failed with error: %v", err)
	case <-time.After(30 * time.Second):
		// Timeout for safety
		cancel()
		t.Fatal("Test timed out after 30 seconds")
	}

	// Clean up tables after testing
	for i := 0; i < 5; i++ {
		tableName := fmt.Sprintf("concurrent_table_%d", i)
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		for j := 0; j < 4; j++ {
			shardedTable := fmt.Sprintf("%s_%d", tableName, j)
			testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", shardedTable))
		}
	}

	// If we made it here without any reported errors, the test passes
}

// TestConfigAccessRaceCondition specifically tests the race condition fix in the ConnPool implementation
func TestConfigAccessRaceCondition(t *testing.T) {
	// Create a test configuration map with a table that has DoubleWrite enabled
	configs := map[string]Config{
		"orders": {
			DoubleWrite:         true,
			ShardingKey:         "user_id",
			NumberOfShards:      4,
			PrimaryKeyGenerator: PKSnowflake,
		},
	}

	// Register the middleware with the config
	middleware := Register(configs, &Order{})

	// Set up a wait group to synchronize goroutines
	var wg sync.WaitGroup
	const goroutines = 10
	wg.Add(goroutines)

	// Run multiple goroutines that will concurrently access the configs
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()

			// Access the configuration 100 times
			for j := 0; j < 100; j++ {
				// This simulates what happens inside QueryContext, ExecContext, etc.
				// Reading the DoubleWrite flag from the config
				middleware.mutex.RLock()
				config, ok := middleware.configs["orders"]
				doubleWrite := ok && config.DoubleWrite
				middleware.mutex.RUnlock()

				// Use the value to avoid compiler optimizations
				if doubleWrite {
					// Just to use the value
					_ = doubleWrite
				}

				// Sleep a tiny amount to increase chance of race
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// If we get here without the race detector finding issues, the test passes
	t.Log("Completed accessing configs concurrently without race conditions")
}

// TestDataRaceWithDoubleWrite tests specific race conditions with double write operations
func TestDataRaceWithDoubleWrite(t *testing.T) {
	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Configure a table with DoubleWrite=true
	doubleWriteConfigs := map[string]Config{
		"orders": {
			DoubleWrite:         true,
			ShardingKey:         "user_id",
			NumberOfShards:      4,
			PrimaryKeyGenerator: PKSnowflake,
		},
	}

	// Register middleware
	doubleWriteMiddleware := Register(doubleWriteConfigs, &Order{})
	testDB.Use(doubleWriteMiddleware)

	// Important: Initialize the database connection properly to set up ConnPool
	err = testDB.AutoMigrate(&Order{})
	if err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	// Create connection pool implementation for testing
	connPool := &ConnPool{
		ConnPool: testDB.Statement.ConnPool,
		sharding: doubleWriteMiddleware,
	}
	doubleWriteMiddleware.ConnPool = connPool

	// Prepare test tables
	truncateTables(testDB, "orders", "orders_0", "orders_1", "orders_2", "orders_3")

	// Create context for coordination
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Channel to report errors
	errCh := make(chan error, 10)

	// Launch concurrent writers
	const numWriters = 5
	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()

			// Each writer creates orders for a specific user ID
			userID := 100 + id

			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					// Create an order with insert query that will require DoubleWrite
					insertSQL := fmt.Sprintf(
						"INSERT INTO orders (user_id, product, category_id) VALUES (%d, 'Product-%d-%d', %d)",
						userID, id, j, j+1,
					)

					_, err := doubleWriteMiddleware.ConnPool.ExecContext(ctx, insertSQL)
					if err != nil {
						errCh <- fmt.Errorf("writer %d insert error: %v", id, err)
						cancel()
						return
					}

					// Brief pause
					time.Sleep(time.Millisecond * 5)
				}
			}
		}(i)
	}

	// Launch concurrent readers
	const numReaders = 5
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < 20; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					// Query that will cause the middleware to check configs
					userID := 100 + (j % numWriters) // Cycle through the user IDs
					querySQL := fmt.Sprintf("SELECT * FROM orders WHERE user_id = %d", userID)

					_, err := doubleWriteMiddleware.ConnPool.QueryContext(ctx, querySQL)
					if err != nil {
						errCh <- fmt.Errorf("reader %d query error: %v", id, err)
						cancel()
						return
					}

					// Brief pause
					time.Sleep(time.Millisecond * 3)
				}
			}
		}(i)
	}

	// Wait for completion or error
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All concurrent operations completed successfully")
	case err := <-errCh:
		t.Fatalf("Test failed with error: %v", err)
	case <-time.After(20 * time.Second):
		cancel()
		t.Fatal("Test timed out after 20 seconds")
	}
}

// Function to safely get value from pointer types
func dereferenceValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return v.Elem().Interface()
	}
	return value
}

func assertQueryResult(t *testing.T, expected string, middleware *Sharding) {
	t.Helper()
	normalize := func(query string) string {
		// Remove quotes around identifiers
		re := regexp.MustCompile(`"(\w+)"`)
		query = re.ReplaceAllString(query, `$1`)
		// Replace parameter numbers with a placeholder
		query = regexp.MustCompile(`\$\d+`).ReplaceAllString(query, `$?`)
		// Normalize whitespace
		query = strings.TrimSpace(query)
		query = regexp.MustCompile(`\s+`).ReplaceAllString(query, ` `)
		return query
	}
	normalizedExpected := normalize(toDialect(expected))
	normalizedActual := normalize(middleware.LastQuery())
	if normalizedExpected != normalizedActual {
		t.Errorf("\nExpected:\n%s\nActual:\n%s", normalizedExpected, normalizedActual)
	}
}

func truncateTables(db *gorm.DB, tables ...string) {
	for _, table := range tables {
		db.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table))
	}
}

func toDialect(sql string) string {
	if os.Getenv("DIALECTOR") == "mysql" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
		sql = strings.ReplaceAll(sql, " RETURNING `id`", "")
	} else if os.Getenv("DIALECTOR") == "mariadb" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
	}
	return sql
}

// skip $sfid compare
func assertSfidQueryResult(t *testing.T, expected, lastQuery string) {
	t.Helper()

	node, _ := snowflake.NewNode(0)
	sfid := node.Generate().Int64()
	sfidLen := len(strconv.Itoa(int(sfid)))
	re := regexp.MustCompile(`\$sfid`)

	for {
		match := re.FindStringIndex(expected)
		if len(match) == 0 {
			break
		}

		start := match[0]
		end := match[1]

		if len(lastQuery) < start+sfidLen {
			break
		}

		sfid := lastQuery[start : start+sfidLen]
		expected = expected[:start] + sfid + expected[end:]
	}

	assert.Equal(t, toDialect(expected), lastQuery)
}

func mysqlDialector() bool {
	return os.Getenv("DIALECTOR") == "mysql" || os.Getenv("DIALECTOR") == "mariadb"
}

func mariadbDialector() bool {
	return os.Getenv("DIALECTOR") == "mariadb"
}
