package sharding

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
		if mysqlDialector() {
			db.Exec(("DROP TABLE IF EXISTS gorm_sharding_" + table + "_id_seq"))
		} else {
			db.Exec(("DROP SEQUENCE IF EXISTS gorm_sharding_" + table + "_id_seq"))
		}
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

func mariadbDialector() bool {
	return os.Getenv("DIALECTOR") == "mariadb"
}

// TestSelect1_withQualifiedRef tests whether a query with both `user_id` and `id` conditions
// is routed to the appropriate shard (`orders_1`). The SQL is expected to be rewritten with
// fully qualified table references.
func TestSelect1_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.user_id", 101).
		Where("orders.id", node.Generate().Int64()).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "orders_1"."user_id" = $1 AND "orders_1"."id" = $2`, tx)
}

// TestSelect2_withQualifiedRef ensures that the order of conditions (`id` and `user_id`)
// does not affect the query routing. The middleware should rewrite the query to target
// the correct shard (`orders_1`).
func TestSelect2_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.id", node.Generate().Int64()).
		Where("orders.user_id", 101).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "orders_1"."id" = $1 AND "orders_1"."user_id" = $2`, tx)
}

// TestSelect3_withQualifiedRef verifies whether queries with mixed parameterized (`id`)
// and inline (`user_id = 101`) conditions are correctly routed to `orders_1` and rewritten
// with appropriate table references.
func TestSelect3_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.id", node.Generate().Int64()).
		Where("orders.user_id = 101").
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "orders_1"."id" = $1 AND orders_1.user_id = 101`, tx)
}

// TestSelect4_withQualifiedRef verifies that queries filtering by `product` and `user_id`
// correctly route to the shard `orders_0`. The SQL is expected to be rewritten with fully
// qualified table references.
func TestSelect4_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.product", "iPad").
		Where("orders.user_id", 100).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_0 WHERE "orders_0"."product" = $1 AND "orders_0"."user_id" = $2`, tx)
}

// TestSelect5_withQualifiedRef ensures that queries with only `user_id` conditions
// are routed to the correct shard (`orders_1`).
func TestSelect5_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.user_id = 101").
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE orders_1.user_id = 101`, tx)
}

// TestSelect6_withQualifiedRef verifies that queries filtering only by `id`
// are routed to the correct shard (`orders_1`).
func TestSelect6_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.id", node.Generate().Int64()).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "orders_1"."id" = $1`, tx)
}

// TestSelect7_withQualifiedRef checks if combined conditions (`user_id` and `id > ?`)
// are correctly rewritten and routed to `orders_1`.
func TestSelect7_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.user_id", 101).
		Where("orders.id > ?", node.Generate().Int64()).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE "orders_1"."user_id" = $1 AND orders_1.id > $2`, tx)
}

// TestSelect8_withQualifiedRef verifies that reversing the order of conditions
// (`id > ?` and `user_id`) does not affect routing to the correct shard (`orders_1`).
func TestSelect8_withQualifiedRef(t *testing.T) {
	tx := db.Table("orders").
		Where("orders.id > ?", node.Generate().Int64()).
		Where("orders.user_id", 101).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE orders_1.id > $1 AND "orders_1"."user_id" = $2`, tx)
}

// TestSelect9_withQualifiedRef ensures that `First` queries with `user_id`
// are routed to the correct shard (`orders_1`) with proper ordering and limit applied.
func TestSelect9_withQualifiedRef(t *testing.T) {
	tx := db.Model(&Order{}).
		Where("orders.user_id = 101").
		First(&[]Order{})
	t.Logf("last query: %s", middleware.LastQuery())
	assertQueryResult(t, `SELECT * FROM orders_1 WHERE orders_1.user_id = 101 ORDER BY "orders_1"."id" LIMIT 1`, tx)
}

// TestSelect10_withQualifiedRef verifies that queries with the `nosharding` comment
// bypass sharding middleware and target the base table `orders`.
func TestSelect10_withQualifiedRef(t *testing.T) {
	tx := db.Clauses(hints.Comment("select", "nosharding")).
		Table("orders").
		Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders"`, tx)
}

// TestSelect11_withQualifiedRef ensures that `nosharding` comments apply even with
// filtering conditions (`user_id`), allowing the query to bypass the middleware.
func TestSelect11_withQualifiedRef(t *testing.T) {
	tx := db.Clauses(hints.Comment("select", "nosharding")).
		Table("orders").
		Where("orders.user_id", 101).
		Find(&[]Order{})
	assertQueryResult(t, `SELECT /* nosharding */ * FROM "orders" WHERE "orders"."user_id" = $1`, tx)
}

// TestSelect12_withQualifiedRef tests raw SQL queries with qualified schema names
// (`public.orders`) to ensure they are executed as-is without middleware interference.
func TestSelect12_withQualifiedRef(t *testing.T) {
	sql := toDialect(`SELECT * FROM "public"."orders" WHERE orders.user_id = 101`)
	tx := db.Raw(sql).Find(&[]Order{})
	assertQueryResult(t, sql, tx)
}

// TestSelect13_withQualifiedRef verifies that raw SQL queries (e.g., `SELECT 1`)
// are executed correctly and unaffected by sharding middleware.
func TestSelect13_withQualifiedRef(t *testing.T) {
	var n int
	tx := db.Raw("SELECT 1").Find(&n)
	assertQueryResult(t, `SELECT 1`, tx)
}

// TestSelect14_withQualifiedRef ensures that queries on a database without primary keys
// still route correctly to the appropriate shard (`orders_1`).
func TestSelect14_withQualifiedRef(t *testing.T) {
	dbNoID.Table("orders").
		Where("orders.user_id = 101").
		Find(&[]Order{})
	expected := `SELECT * FROM orders_1 WHERE orders_1.user_id = 101`
	assert.Equal(t, toDialect(expected), middlewareNoID.LastQuery())
}

// TestUpdate_withQualifiedRef tests updating the `product` field for orders with a
// specific `user_id`. It ensures that the SQL query is correctly rewritten with table
// prefixes to target the appropriate sharded table (`orders_0`).
func TestUpdate_withQualifiedRef(t *testing.T) {
	tx := db.Model(&Order{}).
		Where("orders.user_id = ?", 100).
		Update("product", "new title")
	assertQueryResult(t, `UPDATE orders_0 SET "product" = $1 WHERE orders_0.user_id = $2`, tx)
}

// TestDelete_withQualifiedRef tests deleting orders with a specific `user_id`.
// It verifies that the SQL query is correctly rewritten with table prefixes
// to target the appropriate sharded table (`orders_0`).
func TestDelete_withQualifiedRef(t *testing.T) {
	tx := db.
		Where("orders.user_id = ?", 100).
		Delete(&Order{})
	assertQueryResult(t, `DELETE FROM orders_0 WHERE orders_0.user_id = $1`, tx)
}

// TestSelectMissingShardingKey_withQualifiedRef tests selecting orders without specifying
// the sharding key (`user_id`). It ensures that the sharding middleware detects the missing
// sharding key and returns the appropriate error.
func TestSelectMissingShardingKey_withQualifiedRef(t *testing.T) {
	err := db.Exec(`SELECT * FROM "orders" WHERE "orders"."product" = 'iPad'`).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

// TestSelectNoSharding_withQualifiedRef tests selecting orders with a `nosharding` comment.
// It ensures that the sharding middleware does not rewrite the SQL query when the hint is present.
func TestSelectNoSharding_withQualifiedRef(t *testing.T) {
	sql := toDialect(`SELECT /* nosharding */ * FROM "orders" WHERE "orders"."product" = 'iPad'`)
	err := db.Exec(sql).Error
	assert.Equal[error](t, nil, err)
}

// TestNoEq_withQualifiedRef tests selecting orders with a non-equal condition on the sharding key "user_id".
// It verifies that the sharding middleware detects the unsupported condition and returns the appropriate error.
func TestNoEq_withQualifiedRef(t *testing.T) {
	err := db.Model(&Order{}).Where("orders.user_id <> ?", 101).Find([]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

// TestShardingKeyOK_withQualifiedRef tests selecting orders with valid sharding key conditions "user_id = ?" and "id > ?".
// It ensures that the sharding middleware successfully rewrites the SQL query to target the correct shard.
func TestShardingKeyOK_withQualifiedRef(t *testing.T) {
	err := db.Model(&Order{}).
		Where("orders.user_id = ?", 101).
		Where("orders.id > ?", int64(100)).
		Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

// TestShardingKeyNotOK_withQualifiedRef tests selecting orders with invalid sharding key conditions "user_id > ?" and "id > ?".
// It verifies that the sharding middleware detects the unsupported conditions and returns the appropriate error.
func TestShardingKeyNotOK_withQualifiedRef(t *testing.T) {
	err := db.Model(&Order{}).
		Where("orders.user_id > ?", 101).
		Where("orders.id > ?", int64(100)).
		Find(&[]Order{}).Error
	assert.Equal(t, ErrMissingShardingKey, err)
}

// TestShardingIdOK_withQualifiedRef tests selecting orders with a valid sharding key condition on "id = ?" and "user_id > ?".
// It ensures that the sharding middleware successfully rewrites the SQL query to target the correct shard.
func TestShardingIdOK_withQualifiedRef(t *testing.T) {
	err := db.Model(&Order{}).
		Where("orders.id = ?", int64(101)).
		Where("orders.user_id > ?", 100).
		Find(&[]Order{}).Error
	assert.Equal[error](t, nil, err)
}

// TestNoShardingCategory_withQualifiedRef tests selecting categories without using sharding keys.
// It ensures that the sharding middleware does not interfere with tables that are not sharded.
func TestNoShardingCategory_withQualifiedRef(t *testing.T) {
	categories := []Category{}
	tx := db.Model(&Category{}).Where("id = ?", 1).Find(&categories)
	assertQueryResult(t, `SELECT * FROM "categories" WHERE id = $1`, tx)
}

// TestPKPGSequence_withQualifiedRef tests inserting an order using the PostgreSQL sequence primary key generator.
// It ensures that the sharding middleware correctly increments the sequence and rewrites the SQL query to target the appropriate shard.
func TestPKPGSequence_withQualifiedRef(t *testing.T) {
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

// TestPKMySQLSequence_withQualifiedRef tests inserting an order using the MySQL sequence primary key generator.
// It verifies that the sharding middleware correctly updates the sequence and rewrites the SQL query to target the appropriate shard.
func TestPKMySQLSequence_withQualifiedRef(t *testing.T) {
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

// TestDataRace_withQualifiedRef tests for data races in concurrent operations.
// It ensures that the sharding middleware handles concurrent queries without causing race conditions.
func TestDataRace_withQualifiedRef(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error)

	for i := 0; i < 2; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					err := db.Model(&Order{}).Where("orders.user_id", 100).Find(&[]Order{}).Error
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
