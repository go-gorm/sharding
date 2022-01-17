# Gorm Sharding

[![Go](https://github.com/longbridgeapp/gorm-sharding/actions/workflows/go.yml/badge.svg)](https://github.com/longbridgeapp/gorm-sharding/actions/workflows/go.yml)
[![GoDoc](https://godoc.org/github.com/longbridgeapp/gorm-sharding?status.svg)](https://godoc.org/github.com/longbridgeapp/gorm-sharding)

English | [简体中文](./README.zh-CN.md)

Gorm Sharding plugin using SQL parser and replace for splits large tables into smaller ones, redirects Query into sharding tables. Give you a high performance database access.

Gorm Sharding 是一个业务污染小，高性能的数据库分表方案。通过 SQL 解析和替换，实现分表逻辑，让查询正确的根据规则执行到分表里面。

![Example](./docs/query.svg)

## Features

- Non-intrusive design. Load the plugin, specify the config, and all done.
- Lighting-fast. No network based middlewares, as fast as Go.
- Multiple database support. PostgreSQL tested, MySQL and SQLite is coming.
- Allows you custom the Primary Key generator (Sequence, UUID, Snowflake ...).

## Install

```bash
go get -u github.com/longbridgeapp/gorm-sharding
```

## Usage

After the database connection opened, use the sharding plugin that registered the tables which you want to shard.

The `Register` function takes a map, the key is the **original table name** and the value is a **resolver** which is composed by five configurable fields.

For config detail info, see [Godoc](https://pkg.go.dev/github.com/longbridge/gorm-sharding).

## Usage Example

```go
middleware := sharding.Register(map[string]sharding.Resolver{
	"orders": {
		EnableFullTable: true,
		ShardingColumn: "user_id",
		ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
			switch user_id := value.(type) {
			case int64:
				return fmt.Sprintf("_%02d", user_id % 64), nil
			default:
				return "", errors.New("invalid user_id")
			}
		},
		ShardingAlgorithmByPrimaryKey: func(id int64) (suffix string) {
			return fmt.Sprintf("_%02d", keygen.TableIdx(id))
		},
		PrimaryKeyGenerate: func(tableIdx int64) int64 {
			keygen.Snowflake()
			keygen.UUID(),
			keygen.Sequence("orders_id_seq"),
		}
	},
})
db.Use(middleware)
```

## License

This project under MIT license.
