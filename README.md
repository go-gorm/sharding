# Gorm Sharding

[![Go](https://github.com/go-gorm/sharding/actions/workflows/go.yml/badge.svg)](https://github.com/go-gorm/sharding/actions/workflows/go.yml)

Gorm Sharding plugin using SQL parser and replace for splits large tables into smaller ones, redirects Query into sharding tables. Give you a high performance database access.

Gorm Sharding 是一个高性能的数据库分表中间件。

它基于 Conn 层做 SQL 拦截、AST 解析、分表路由、自增主键填充，带来的额外开销极小。对开发者友好、透明，使用上与普通 SQL、Gorm 查询无差别，只需要额外注意一下分表键条件。

## Features

- Non-intrusive design. Load the plugin, specify the config, and all done.
- Lighting-fast. No network based middlewares, as fast as Go.
- Multiple database support. PostgreSQL tested, MySQL and SQLite is coming.
- Integrated primary key generator (Snowflake, PostgreSQL Sequence, Custom, ...).

## Sharding process

This graph show up how Gorm Sharding works.

![Example](./docs/query.svg)

## Install

```bash
go get -u gorm.io/sharding
```

## Usage

Open a db session.

```go
dsn := "postgres://localhost:5432/sharding-db?sslmode=disable"
db, err := gorm.Open(postgres.New(postgres.Config{DSN: dsn}))
```

Config the sharding middleware, register the tables which you want to shard. See [Godoc](https://pkg.go.dev/github.com/go-gorm/sharding) for config details.

```go
db.Use(sharding.Register(sharding.Config{
    ShardingKey: "user_id",
    ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
        if user_id, ok := value.(int64); ok {
            return fmt.Sprintf("_%02d", user_id%64), nil
        }
        return "", errors.New("invalid user_id")
    },
    PrimaryKeyGenerator: sharding.PKSnowflake,
}, "orders").Register(sharding.Config{
    ShardingKey: "user_id",
    ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
        if user_id, ok := value.(int64); ok {
            return fmt.Sprintf("_%02d", user_id%256), nil
        }
        return "", errors.New("invalid user_id")
    },
    PrimaryKeyGenerate: func(tableIdx int64) int64 {
        return snowflake_node.Generate().Int64()
    }
    // This case for show up give notifications, audit_logs table use same sharding rule.
}, Notification{}, AuditLog{}))
```

Use the db session as usual. Just note that the query should have the `Sharding Key` when operate sharding tables.

```go
// Gorm create example, this will insert to orders_02
db.Create(&Order{UserID: 2})
// sql: INSERT INTO orders_2 ...

// Show have use Raw SQL to insert, this will insert into orders_03
db.Exec("INSERT INTO orders(user_id) VALUES(?)", int64(3))

// This will throw ErrMissingShardingKey error, because there not have sharding key presented.
db.Create(&Order{Amount: 10, ProductID: 100})
fmt.Println(err)

// Find, this will redirect query to orders_02
var orders []Order
db.Model(&Order{}).Where("user_id", int64(2)).Find(&orders)
fmt.Printf("%#v\n", orders)

// Raw SQL also supported
db.Raw("SELECT * FROM orders WHERE user_id = ?", int64(3)).Scan(&orders)
fmt.Printf("%#v\n", orders)

// This will throw ErrMissingShardingKey error, because WHERE conditions not included sharding key
err = db.Model(&Order{}).Where("product_id", "1").Find(&orders).Error
fmt.Println(err)

// Update and Delete are similar to create and query
db.Exec("UPDATE orders SET product_id = ? WHERE user_id = ?", 2, int64(3))
err = db.Exec("DELETE FROM orders WHERE product_id = 3").Error
fmt.Println(err) // ErrMissingShardingKey
```

The full example is [here](./examples/order.go).

## Primary Key

When you sharding tables, you need consider how the primary key generate.

Recommend options:

- [Snowflake](https://github.com/bwmarrin/snowflake)
- [Database sequence by manully](https://www.postgresql.org/docs/current/sql-createsequence.html)

## License

MIT license.

Original fork from [Longbridge](https://github.com/longbridgeapp/gorm-sharding).
