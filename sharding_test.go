package sharding

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/assert"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/hints"
	"gorm.io/plugin/dbresolver"
)

type Order struct {
	ID      int64 `gorm:"primarykey"`
	UserID  int64
	Product string
}

type Category struct {
	ID   int64 `gorm:"primarykey"`
	Name string
}

func databaseURL() string {
	databaseURL := os.Getenv("DATABASE_URL")
	if len(databaseURL) == 0 {
		databaseURL = "postgres://localhost:5432/sharding-test?sslmode=disable"
		if os.Getenv("DIALECTOR") == "mysql" {
			databaseURL = "root@tcp(127.0.0.1:3306)/sharding-test?charset=utf8mb4"
		}
	}
	return databaseURL
}

func databaseReadURL() string {
	databaseURL := os.Getenv("DATABASE_READ_URL")
	if len(databaseURL) == 0 {
		databaseURL = "postgres://localhost:5432/sharding-read-test?sslmode=disable"
		if os.Getenv("DIALECTOR") == "mysql" {
			databaseURL = "root@tcp(127.0.0.1:3306)/sharding-read-test?charset=utf8mb4"
		}
	}
	return databaseURL
}

func databaseWriteURL() string {
	databaseURL := os.Getenv("DATABASE_WRITE_URL")
	if len(databaseURL) == 0 {
		databaseURL = "postgres://localhost:5432/sharding-write-test?sslmode=disable"
		if os.Getenv("DIALECTOR") == "mysql" {
			databaseURL = "root@tcp(127.0.0.1:3306)/sharding-write-test?charset=utf8mb4"
		}
	}
	return databaseURL
}

var (
	dbConfig = postgres.Config{
		DSN:                  databaseURL(),
		PreferSimpleProtocol: true,
	}
	dbReadConfig = postgres.Config{
		DSN:                  databaseReadURL(),
		PreferSimpleProtocol: true,
	}
	dbWriteConfig = postgres.Config{
		DSN:                  databaseWriteURL(),
		PreferSimpleProtocol: true,
	}
	db, dbRead, dbWrite *gorm.DB

	shardingConfig Config
	middleware     *Sharding
	node, _        = snowflake.NewNode(1)
)

func init() {
	if os.Getenv("DIALECTOR") == "mysql" {
		db, _ = gorm.Open(mysql.Open(databaseURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbRead, _ = gorm.Open(mysql.Open(databaseReadURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbWrite, _ = gorm.Open(mysql.Open(databaseWriteURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	} else {
		db, _ = gorm.Open(postgres.New(dbConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbRead, _ = gorm.Open(postgres.New(dbReadConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		dbWrite, _ = gorm.Open(postgres.New(dbWriteConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	}

	shardingConfig = Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKSnowflake,
	}

	middleware = Register(shardingConfig, &Order{})

	fmt.Println("Clean only tables ...")
	dropTables()
	fmt.Println("AutoMigrate tables ...")
	err := db.AutoMigrate(&Order{}, &Category{})
	if err != nil {
		panic(err)
	}
	stables := []string{"orders_0", "orders_1", "orders_2", "orders_3"}
	for _, table := range stables {
		db.Exec(`CREATE TABLE ` + table + ` (
			id bigint PRIMARY KEY,
			user_id bigint,
			product text
		)`)
		dbRead.Exec(`CREATE TABLE ` + table + ` (
			id bigint PRIMARY KEY,
			user_id bigint,
			product text
		)`)
		dbWrite.Exec(`CREATE TABLE ` + table + ` (
			id bigint PRIMARY KEY,
			user_id bigint,
			product text
		)`)
	}

	db.Use(middleware)
}

func dropTables() {
	tables := []string{"orders", "orders_0", "orders_1", "orders_2", "orders_3", "categories"}
	for _, table := range tables {
		db.Exec("DROP TABLE IF EXISTS " + table)
		dbRead.Exec("DROP TABLE IF EXISTS " + table)
		dbWrite.Exec("DROP TABLE IF EXISTS " + table)
		db.Exec(("DROP SEQUENCE IF EXISTS gorm_sharding_" + table + "_id_seq"))
	}
}

func TestInsert(t *testing.T) {
	tx := db.Create(&Order{ID: 100, UserID: 100, Product: "iPhone"})
	assertQueryResult(t, `INSERT INTO orders_0 ("user_id", "product", "id") VALUES ($1, $2, $3) RETURNING "id"`, tx)
}

func TestFillID(t *testing.T) {
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO orders_0 ("user_id", "product", id) VALUES`
	lastQuery := middleware.LastQuery()
	assert.Equal(t, toDialect(expected), lastQuery[0:len(expected)])
}

func TestSelect1(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id", 101).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "user_id" = $1 AND "id" = $2`, tx)
}

func TestSelect2(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1 AND "user_id" = $2`, tx)
}

func TestSelect3(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1 AND user_id = 101`, tx)
}

func TestSelect4(t *testing.T) {
	tx := db.Model(&Order{}).Where("product", "iPad").Where("user_id", 100).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_0 WHERE "product" = $1 AND "user_id" = $2`, tx)
}

func TestSelect5(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = 101").Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE user_id = 101`, tx)
}

func TestSelect6(t *testing.T) {
	tx := db.Model(&Order{}).Where("id", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "id" = $1`, tx)
}

func TestSelect7(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id", 101).Where("id > ?", node.Generate().Int64()).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "user_id" = $1 AND id > $2`, tx)
}

func TestSelect8(t *testing.T) {
	tx := db.Model(&Order{}).Where("id > ?", node.Generate().Int64()).Where("user_id", 101).Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE id > $1 AND "user_id" = $2`, tx)
}

func TestSelect9(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = 101").First(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE user_id = 101 ORDER BY "orders_1"."id" LIMIT 1`, tx)
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
	sql := toDialect(`SELECT * FROM "public"."orders" WHERE user_id = 101`)
	tx := db.Raw(sql).Find(&[]Order{})
	assertQueryResult(t, sql, tx)
}

func TestSelect13(t *testing.T) {
	tx := db.Raw("SELECT 1").Find(&[]Order{})
	assertQueryResult(t, `SELECT 1`, tx)
}

func TestUpdate(t *testing.T) {
	tx := db.Model(&Order{}).Where("user_id = ?", 100).Update("product", "new title")
	assertQueryResult(t, `UPDATE orders_0 SET "product" = $1 WHERE user_id = $2`, tx)
}

func TestDelete(t *testing.T) {
	tx := db.Where("user_id = ?", 100).Delete(&Order{})
	assertQueryResult(t, `DELETE FROM orders_0 WHERE user_id = $1`, tx)
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
	sql := toDialect(`SELECT /* nosharding */ * FROM "orders" WHERE "product" = 'iPad'`)
	err := db.Exec(sql).Error
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
	if os.Getenv("DIALECTOR") == "mysql" {
		db, _ = gorm.Open(mysql.Open(databaseURL()), &gorm.Config{
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
	expected := fmt.Sprintf(`INSERT INTO orders_0 ("user_id", "product", id) VALUES ($1, $2, %d`, sfid)[0:68]
	expected = toDialect(expected)

	db.Create(&Order{UserID: 100, Product: "iPhone"})
	assert.Equal(t, expected, middleware.LastQuery()[0:len(expected)])
}

func TestPKPGSequence(t *testing.T) {
	if os.Getenv("DIALECTOR") == "mysql" {
		return
	}

	db, _ := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	shardingConfig.PrimaryKeyGenerator = PKPGSequence
	middleware := Register(shardingConfig, &Order{})
	db.Use(middleware)

	db.Exec("SELECT setval('" + pgSeqName("orders") + "', 42)")
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO orders_0 ("user_id", "product", id) VALUES ($1, $2, 43) RETURNING "id"`
	assert.Equal(t, expected, middleware.LastQuery())
}

func TestReadWriteSplitting(t *testing.T) {
	dbRead.Exec("INSERT INTO orders_0 (id, product, user_id) VALUES(1, 'iPad', 100)")
	dbWrite.Exec("INSERT INTO orders_0 (id, product, user_id) VALUES(1, 'iPad', 100)")

	var db *gorm.DB
	if os.Getenv("DIALECTOR") == "mysql" {
		db, _ = gorm.Open(mysql.Open(databaseURL()), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
	} else {
		db, _ = gorm.Open(postgres.New(dbConfig), &gorm.Config{
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
	db.Table("orders_0").Where("user_id", 100).Find(&order)
	assert.Equal(t, "iPad", order.Product)

	dbWrite.Table("orders_0").Where("user_id", 100).Find(&order)
	assert.Equal(t, "iPhone", order.Product)
}

func assertQueryResult(t *testing.T, expected string, tx *gorm.DB) {
	t.Helper()
	assert.Equal(t, toDialect(expected), middleware.LastQuery())
}

func toDialect(sql string) string {
	if os.Getenv("DIALECTOR") == "mysql" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
		sql = strings.ReplaceAll(sql, " RETURNING `id`", "")
	}
	return sql
}
