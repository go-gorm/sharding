package sharding

import (
	"fmt"
)

const DefaultShardingNumber = 128

type ShardingInterface interface {
	TableName() string

	NumberOfShards() uint
	Sharding() Config
	TableSuffix() string
}

type BaseSharding struct {
	// 存储子类设置的默认分片数
	DefaultShards uint `gorm:"-"`
}

func (b *BaseSharding) TableName() string {
	return ""
}

func (b *BaseSharding) NumberOfShards() uint {
	if b.DefaultShards == 0 {
		return DefaultShardingNumber
	}

	// 回退到默认值
	return b.DefaultShards
}

func (b *BaseSharding) Sharding() Config {
	return Config{
		DoubleWrite:    false,
		ShardingKey:    "user_id",
		NumberOfShards: b.NumberOfShards(),
		ShardingSuffixs: func() []string {
			suffixLists := make([]string, b.NumberOfShards())
			var i uint = 0
			for ; i < b.NumberOfShards(); i += 1 {
				suffixLists[i] = fmt.Sprintf(b.TableSuffix(), i)
			}
			return suffixLists
		},
		ShardingAlgorithm: func(columnValue any) (suffix string, err error) {
			userID, ok := columnValue.(uint64)
			if !ok {
				return "", fmt.Errorf("invalid ID type, expected uint64")
			}
			// 根据 id 计算分表后缀
			return fmt.Sprintf(b.TableSuffix(), userID&uint64(b.NumberOfShards()-1)), nil
		},
		PrimaryKeyGenerator: PKCustom,
		PrimaryKeyGeneratorFn: func(_ int64) int64 {
			return 0
		},
	}
}

func (b *BaseSharding) TableSuffix() string {
	var tableFormat string
	if b.NumberOfShards() < 10 {
		tableFormat = "_%01d"
	} else if b.NumberOfShards() < 100 {
		tableFormat = "_%02d"
	} else if b.NumberOfShards() < 1000 {
		tableFormat = "_%03d"
	} else if b.NumberOfShards() < 10000 {
		tableFormat = "_%04d"
	}

	return tableFormat
}
