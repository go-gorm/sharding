package sharding

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/hints"
)

type Order struct {
	ID      int64 `gorm:"primarykey"`
	UserID  int64
	Product string
}

func (Order) TableName() string {
	return "orders"
}

type Category struct {
	ID   int64 `gorm:"primarykey"`
	Name string
}

func (Category) TableName() string {
	return "categories"
}

func databaseURL() string {
	databaseURL := os.Getenv("DATABASE_URL")
	if len(databaseURL) == 0 {
		databaseURL = "postgres://localhost:5432/sharding-test?sslmode=disable"
	}
	return databaseURL
}

var (
	dbConfig = postgres.Config{
		DSN:                  databaseURL(),
		PreferSimpleProtocol: true,
	}
	db, _ = gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})

	shardingConfig = Config{
		DoubleWrite: true,
		ShardingKey: "user_id",
		ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
			userId := 0
			switch value := value.(type) {
			case int:
				userId = value
			case int64:
				userId = int(value)
			case string:
				userId, err = strconv.Atoi(value)
				if err != nil {
					return "", err
				}
			default:
				return "", err
			}
			return fmt.Sprintf("_%02d", userId%4), nil
		},
		ShardingAlgorithmByPrimaryKey: func(id int64) (suffix string) {
			return fmt.Sprintf("_%02d", snowflake.ParseInt64(id).Node())
		},
		PrimaryKeyGenerator: PKSnowflake,
	}

	middleware = Register(shardingConfig, &Order{})

	node, _ = snowflake.NewNode(1)
)

func init() {
	dropTables()
	err := db.AutoMigrate(&Order{}, &Category{})
	if err != nil {
		panic(err)
	}
	stables := []string{"orders_00", "orders_01", "orders_02", "orders_03"}
	for _, table := range stables {
		db.Exec(`CREATE TABLE ` + table + ` (
			id BIGSERIAL PRIMARY KEY,
			user_id bigint,
			product text
		)`)
	}

	db.Use(middleware)
}

func dropTables() {
	tables := []string{"orders", "orders_00", "orders_01", "orders_02", "orders_03", "categories"}
	for _, table := range tables {
		db.Exec("DROP TABLE IF EXISTS " + table)
	}
}

func TestInsert(t *testing.T) {
	tx := db.Create(&Order{ID: 100, UserID: 100, Product: "iPhone"})
	assertQueryResult(t, `INSERT INTO "orders_00" ("user_id", "product", "id") VALUES ($1, $2, $3) RETURNING "id"`, tx)
}

func TestFillID(t *testing.T) {
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	lastQuery := middleware.LastQuery()
	assert.Equal(t, `INSERT INTO "orders_00" ("user_id", "product", "id") VALUES`, lastQuery[0:59])
}

func TestSelect1(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id", 101).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "user_id" = $1 AND "id" = $2`, tx)
}

func TestSelect2(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "id" = $1 AND "user_id" = $2`, tx)
}

func TestSelect3(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "id" = $1 AND "user_id" = 101`, tx)
}

func TestSelect4(t *testing.T) {
	tx := db.Model(&Order{}).Where("product", "iPad").Where("user_id", 100).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_00" WHERE "product" = $1 AND "user_id" = $2`, tx)
}

func TestSelect5(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "user_id" = 101`, tx)
}

func TestSelect6(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "id" = $1`, tx)
}

func TestSelect7(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id", 101).Where("id > ?", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "user_id" = $1 AND "id" > $2`, tx)
}

func TestSelect8(t *testing.T) {
	tx := db.Model(&Order{}).Where("id > ?", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "id" > $1 AND "user_id" = $2`, tx)
}

func TestSelect9(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = 101").First(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "orders_01" WHERE "user_id" = 101 ORDER BY "orders_01"."id" LIMIT 1`, tx)
}

func TestSelect10(t *testing.T) {
	tx := db.Clauses(hints.Comment("select", "nosharding")).Model(&Order{}).Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders"`, tx)
}

func TestSelect11(t *testing.T) {
	tx := db.Clauses(hints.Comment("select", "nosharding")).Model(&Order{}).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders" WHERE "user_id" = $1`, tx)
}

func TestSelect12(t *testing.T) {
	tx := db.Raw(`SELECT * FROM "public"."orders" WHERE "user_id" = 101`).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM "public"."orders" WHERE "user_id" = 101`, tx)
}

func TestSelect13(t *testing.T) {
	tx := db.Raw("SELECT 1").Find(&[]Order{})
	assertQueryResult(t, `SELECT 1`, tx)
}

func TestUpdate(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = ?", 100).Update("product", "new title")
	assertQueryResult(t, `UPDATE "orders_00" SET "product" = $1 WHERE "user_id" = $2`, tx)
}

func TestDelete(t *testing.T) {
	tx := db.Where("user_id = ?", 100).Delete(&Order{})
	assertQueryResult(t, `DELETE FROM "orders_00" WHERE "user_id" = $1`, tx)
}

func TestInsertMissingShardingKey(t *testing.T) {
	err := db.Exec(`INSERT INTO "orders" ("id", "product") VALUES(1, 'iPad')`).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestSelectMissingShardingKey(t *testing.T) {
	err := db.Exec(`SELECT * FROM "orders" WHERE "product" = 'iPad'`).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestSelectNoSharding(t *testing.T) {
	err := db.Exec(`SELECT /* nosharding */ * FROM "orders" WHERE "product" = 'iPad'`).Error
	assert.Equal(t, nil, err)
}

func TestNoEq(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id <> ?", 101).Find([]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestShardingKeyOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id = ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal(t, nil, err)
}

func TestShardingKeyNotOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id > ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestShardingIdOK(t *testing.T) {
	err := db.Model(&Order{}).Where("id = ? and user_id > ?", int64(101), 100).Find(&[]Order{}).Error
	assert.Equal(t, nil, err)
}

func TestNoSharding(t *testing.T) {
	categories := []Category{}
	tx := db.Model(&Category{}).Where("id = ?", 1).Find(&categories)
	assertQueryResult(t, `SELECT * FROM "categories" WHERE id = $1`, tx)
}

func TestPKSnowflake(t *testing.T) {
	db, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	shardingConfig.PrimaryKeyGenerator = PKSnowflake
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO "orders_00" ("user_id", "product", "id") VALUES ($1, $2, 14858`
	assert.Equal(t, expected, middleware.LastQuery()[0:len(expected)])
}

func TestPKPGSequence(t *testing.T) {
	db, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	shardingConfig.PrimaryKeyGenerator = PKPGSequence
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	db.Exec("SELECT setval('gorm_sharding_serial_for_orders', 42)")
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO "orders_00" ("user_id", "product", "id") VALUES ($1, $2, 43) RETURNING "id"`
	assert.Equal(t, expected, middleware.LastQuery())
}

func assertQueryResult(t *testing.T, query string, tx *gorm.DB) {
	t.Helper()
	assert.Equal(t, query, middleware.LastQuery())
}
