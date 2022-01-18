# Gorm Sharding

[![Go](https://github.com/longbridgeapp/gorm-sharding/actions/workflows/go.yml/badge.svg)](https://github.com/longbridgeapp/gorm-sharding/actions/workflows/go.yml)
[![GoDoc](https://godoc.org/github.com/longbridgeapp/gorm-sharding?status.svg)](https://godoc.org/github.com/longbridgeapp/gorm-sharding)

English | [简体中文](./README.zh-CN.md)

Gorm Sharding plugin using SQL parser and replace for splits large tables into smaller ones, redirects Query into sharding tables. Give you a high performance database access.

Gorm Sharding 是一个业务污染小，高性能的数据库分表方案。通过 SQL 解析和替换，实现分表逻辑，让查询正确的根据规则执行到分表里面。

## Features

- Non-intrusive design. Load the plugin, specify the config, and all done.
- Lighting-fast. No network based middlewares, as fast as Go.
- Multiple database support. PostgreSQL tested, MySQL and SQLite is coming.
- Allows you custom the Primary Key generator (Sequence, UUID, Snowflake ...).

## Sharding process

This graph show up how Gorm Sharding works.

![Example](./docs/query.svg)

## Install

```bash
go get -u github.com/longbridgeapp/gorm-sharding
```

## Usage

After the database connection opened, use the sharding plugin that registered the tables which you want to shard.

The `Register` function takes a map, the key is the **original table name** and the value is a **resolver** which is composed by five configurable fields.

For config detail info, see [Godoc](https://pkg.go.dev/github.com/longbridge/gorm-sharding).

## A Full Usage Example

```go

package main

import (
	"errors"
	"fmt"

	sharding "github.com/longbridgeapp/gorm-sharding"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// IncID used for demonstrate auto-incremented ID, do not use in production.
var IncID int64

type Order struct {
	ID        int64 `gorm:"primarykey"`
	UserID    int64
	ProductID int64
}

func main() {
	dsn := "postgres://localhost:5432/sharding-db?sslmode=disable"
	db, err := gorm.Open(postgres.New(postgres.Config{DSN: dsn}))
	if err != nil {
		panic(err)
	}

	tables := []string{"orders_00", "orders_01", "orders_02", "orders_03"}
	for _, table := range tables {
		db.Exec(`DROP TABLE IF EXISTS ` + table)
		db.Exec(`CREATE TABLE ` + table + ` (
			id BIGSERIAL PRIMARY KEY,
			user_id bigint,
			product_id bigint
		)`)
	}

	middleware := sharding.Register(map[string]sharding.Resolver{
		"orders": {
			ShardingColumn: "user_id",
			ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
				if uid, ok := value.(int64); ok {
					return fmt.Sprintf("_%02d", uid%4), nil
				}
				return "", errors.New("invalid user_id")
			},
			PrimaryKeyGenerate: func(tableIdx int64) int64 {
				IncID += 1
				return IncID
			},
		},
	})
	db.Use(&middleware)

	// insert to orders_01
	err = db.Create(&Order{ID: 100, UserID: 1}).Error
	if err != nil {
		fmt.Println(err)
	}

	// insert to orders_02 and auto fill id
	err = db.Create(&Order{UserID: 2}).Error
	if err != nil {
		fmt.Println(err)
	}

	// insert to orders_03 and auto fill id
	err = db.Create(&Order{UserID: 7}).Error
	if err != nil {
		fmt.Println(err)
	}

	// find user_id 2
	var orders []Order
	err = db.Model(&Order{}).Where("user_id", int64(2)).Find(&orders).Error
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v\n", orders)

	// no sharding error
	err = db.Model(&Order{}).Where("product_id", "1").Find(&orders).Error
	fmt.Println(err)
}


```

## License

This project under MIT license.
