package main

import (
	"errors"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/sharding"
)

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

	for i := 0; i < 64; i += 1 {
		table := fmt.Sprintf("orders_%02d", i)
		db.Exec(`DROP TABLE IF EXISTS ` + table)
		db.Exec(`CREATE TABLE ` + table + ` (
			id BIGSERIAL PRIMARY KEY,
			user_id bigint,
			product_id bigint
		)`)
	}

	middleware := sharding.Register(sharding.Config{
		ShardingKey: "user_id",
		ShardingAlgorithm: func(value interface{}) (suffix string, err error) {
			if uid, ok := value.(int64); ok {
				return fmt.Sprintf("_%02d", uid%64), nil
			}
			return "", errors.New("invalid user_id")
		},
		PrimaryKeyGenerator: sharding.PKLongKey,
	}, "orders")
	db.Use(middleware)

	// this record will insert to orders_02
	err = db.Create(&Order{UserID: 2}).Error
	if err != nil {
		fmt.Println(err)
	}

	// this record will insert to orders_03
	err = db.Exec("INSERT INTO orders(user_id) VALUES(?)", int64(3)).Error
	if err != nil {
		fmt.Println(err)
	}

	// this will throw ErrMissingShardingKey error
	err = db.Exec("INSERT INTO orders(product_id) VALUES(1)").Error
	fmt.Println(err)

	// this will redirect query to orders_02
	var orders []Order
	err = db.Model(&Order{}).Where("user_id", int64(2)).Find(&orders).Error
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%#v\n", orders)

	// Raw SQL also supported
	db.Raw("SELECT * FROM orders WHERE user_id = ?", int64(3)).Scan(&orders)
	fmt.Printf("%#v\n", orders)

	// this will throw ErrMissingShardingKey error
	err = db.Model(&Order{}).Where("product_id", "1").Find(&orders).Error
	fmt.Println(err)

	// Update and Delete are similar to create and query
	err = db.Exec("UPDATE orders SET product_id = ? WHERE user_id = ?", 2, int64(3)).Error
	fmt.Println(err) // nil
	err = db.Exec("DELETE FROM orders WHERE product_id = 3").Error
	fmt.Println(err) // ErrMissingShardingKey
}
