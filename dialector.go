package sharding

import (
	"fmt"

	"gorm.io/gorm"
)

type ShardingDialector struct {
	gorm.Dialector
	sharding *Sharding
}

type ShardingMigrator struct {
	gorm.Migrator
	sharding  *Sharding
	dialector gorm.Dialector
}

func NewShardingDialector(d gorm.Dialector, s *Sharding) ShardingDialector {
	return ShardingDialector{
		Dialector: d,
		sharding:  s,
	}
}

func (d ShardingDialector) Migrator(db *gorm.DB) gorm.Migrator {
	m := d.Dialector.Migrator(db)
	return ShardingMigrator{
		Migrator:  m,
		sharding:  d.sharding,
		dialector: d.Dialector,
	}
}

func (m ShardingMigrator) AutoMigrate(dst ...interface{}) error {
	noShardingDsts := make([]interface{}, 0)
	for _, model := range dst {
		stmt := &gorm.Statement{DB: m.sharding.DB}
		if err := stmt.Parse(model); err == nil {
			if cfg, ok := m.sharding.configs[stmt.Table]; ok {
				// support sharding table
				suffixs := cfg.ShardingSuffixs()
				if len(suffixs) == 0 {
					return fmt.Errorf("sharding table:%s suffixs is empty", stmt.Table)
				}

				for _, suffix := range suffixs {
					shardingTable := stmt.Table + suffix
					tx := stmt.DB.Session(&gorm.Session{}).Table(shardingTable)
					if err := m.dialector.Migrator(tx).AutoMigrate(model); err != nil {
						return err
					}
				}

				if cfg.DoubleWrite {
					noShardingDsts = append(noShardingDsts, model)
				}
			} else {
				noShardingDsts = append(noShardingDsts, model)
			}
		} else {
			return err
		}
	}

	if len(noShardingDsts) > 0 {
		if err := m.Migrator.AutoMigrate(noShardingDsts...); err != nil {
			return err
		}
	}
	return nil
}

// TODO: DropTable drop sharding table
// func (m ShardingMigrator) DropTable(dst ...interface{}) error {
// }
