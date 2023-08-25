package sharding

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
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

func dbURL() string {
	dbURL := os.Getenv("DB_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://localhost:5432/sharding-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbNoIDURL() string {
	dbURL := os.Getenv("DB_NOID_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://localhost:5432/sharding-noid-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-noid-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbReadURL() string {
	dbURL := os.Getenv("DB_READ_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://localhost:5432/sharding-read-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-read-test?charset=utf8mb4"
		}
	}
	return dbURL
}

func dbWriteURL() string {
	dbURL := os.Getenv("DB_WRITE_URL")
	if len(dbURL) == 0 {
		dbURL = "postgres://localhost:5432/sharding-write-test?sslmode=disable"
		if mysqlDialector() {
			dbURL = "root@tcp(127.0.0.1:3306)/sharding-write-test?charset=utf8mb4"
		}
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
	db, dbNoID, dbRead, dbWrite *gorm.DB

	shardingConfig, shardingConfigNoID Config
	middleware, middlewareNoID         *Sharding
	node, _                            = snowflake.NewNode(1)
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
		})
		dbNoID, _ = gorm.Open(postgres.New(dbNoIDConfig), &gorm.Config{
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

	shardingConfigNoID = Config{
		DoubleWrite:         true,
		ShardingKey:         "user_id",
		NumberOfShards:      4,
		PrimaryKeyGenerator: PKCustom,
		PrimaryKeyGeneratorFn: func(_ int64) int64 {
			return 0
		},
	}

	middleware = Register(shardingConfig, &Order{})
	middlewareNoID = Register(shardingConfigNoID, &Order{})

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
		dbNoID.Exec(`CREATE TABLE ` + table + ` (
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
	dbNoID.Use(middlewareNoID)
}

func dropTables() {
	tables := []string{"orders", "orders_0", "orders_1", "orders_2", "orders_3", "categories"}
	for _, table := range tables {
		db.Exec("DROP TABLE IF EXISTS " + table)
		dbNoID.Exec("DROP TABLE IF EXISTS " + table)
		dbRead.Exec("DROP TABLE IF EXISTS " + table)
		dbWrite.Exec("DROP TABLE IF EXISTS " + table)
		db.Exec(("DROP SEQUENCE IF EXISTS gorm_sharding_" + table + "_id_seq"))
	}
}

func TestMigrate(t *testing.T) {
	targetTables := []string{"orders", "orders_0", "orders_1", "orders_2", "orders_3", "categories"}
	sort.Strings(targetTables)

	// origin tables
	tables, _ := db.Migrator().GetTables()
	sort.Strings(tables)
	assert.Equal(t, tables, targetTables)

	// drop table
	db.Migrator().DropTable(Order{}, &Category{})
	tables, _ = db.Migrator().GetTables()
	assert.Equal(t, len(tables), 0)

	// auto migrate
	db.AutoMigrate(&Order{}, &Category{})
	tables, _ = db.Migrator().GetTables()
	sort.Strings(tables)
	assert.Equal(t, tables, targetTables)

	// auto migrate again
	err := db.AutoMigrate(&Order{}, &Category{})
	assert.Equal[error, error](t, err, nil)
}

func TestInsert(t *testing.T) {
	tx := db.Create(&Order{ID: 100, UserID: 100, Product: "iPhone"})
	assertQueryResult(t, `INSERT INTO orders_0 ("user_id", "product", "id") VALUES ($1, $2, $3) RETURNING "id"`, tx)
}

func TestInsertNoID(t *testing.T) {
	dbNoID.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO orders_0 ("user_id", "product") VALUES ($1, $2) RETURNING "id"`
	assert.Equal(t, toDialect(expected), middlewareNoID.LastQuery())
}

func TestFillID(t *testing.T) {
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO orders_0 ("user_id", "product", id) VALUES`
	lastQuery := middleware.LastQuery()
	assert.Equal(t, toDialect(expected), lastQuery[0:len(expected)])
}

func TestInsertManyWithFillID(t *testing.T) {
	err := db.Create([]Order{{UserID: 100, Product: "Mac"}, {UserID: 100, Product: "Mac Pro"}}).Error
	assert.Equal[error, error](t, err, nil)

	expected := `INSERT INTO orders_0 ("user_id", "product", id) VALUES ($1, $2, $sfid), ($3, $4, $sfid) RETURNING "id"`
	lastQuery := middleware.LastQuery()
	assertSfidQueryResult(t, toDialect(expected), lastQuery)
}

func TestInsertDiffSuffix(t *testing.T) {
	err := db.Create([]Order{{UserID: 100, Product: "Mac"}, {UserID: 101, Product: "Mac Pro"}}).Error
	assert.Equal(t, ErrInsertDiffSuffix, err)
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
	var n int
	tx := db.Raw("SELECT 1").Find(&n)
	assertQueryResult(t, `SELECT 1`, tx)
}

func TestSelect14(t *testing.T) {
	dbNoID.Model(&Order{}).Where("user_id = 101").Find(&[]Order{})
	expected := `SELECT * FROM orders_1 WHERE user_id = 101`
	assert.Equal(t, toDialect(expected), middlewareNoID.LastQuery())
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
	assert.Equal[error](t, nil, err)
}

func TestNoEq(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id <> ?", 101).Find([]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestShardingKeyOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id = ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

func TestShardingKeyNotOK(t *testing.T) {
	err := db.Model(&Order{}).Where("user_id > ? and id > ?", 101, int64(100)).Find(&[]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

func TestShardingIdOK(t *testing.T) {
	err := db.Model(&Order{}).Where("id = ? and user_id > ?", int64(101), 100).Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

func TestNoSharding(t *testing.T) {
	categories := []Category{}
	tx := db.Model(&Category{}).Where("id = ?", 1).Find(&categories)
	assertQueryResult(t, `SELECT * FROM "categories" WHERE id = $1`, tx)
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
	expected := fmt.Sprintf(`INSERT INTO orders_0 ("user_id", "product", id) VALUES ($1, $2, %d`, sfid)[0:68]
	expected = toDialect(expected)

	db.Create(&Order{UserID: 100, Product: "iPhone"})
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
	db.Create(&Order{UserID: 100, Product: "iPhone"})
	expected := `INSERT INTO orders_0 ("user_id", "product", id) VALUES ($1, $2, 43) RETURNING "id"`
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
