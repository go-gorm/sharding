package sharding

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
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

// BuildIndexOptions build index options
func (m ShardingDialector) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	for _, opt := range opts {
		str := stmt.Quote(opt.DBName)
		if opt.Expression != "" {
			str = opt.Expression
		} else if opt.Length > 0 {
			str += fmt.Sprintf("(%d)", opt.Length)
		}

		if opt.Collate != "" {
			str += " COLLATE " + opt.Collate
		}

		if opt.Sort != "" {
			str += " " + opt.Sort
		}
		results = append(results, clause.Expr{SQL: str})
	}
	return
}

func (m ShardingMigrator) AutoMigrate(dst ...interface{}) error {
	shardingDsts, noShardingDsts, err := m.splitShardingDsts(dst...)
	if err != nil {
		return err
	}

	stmt := &gorm.Statement{DB: m.sharding.DB}
	for _, sd := range shardingDsts {
		tx := stmt.DB.Session(&gorm.Session{}).Table(sd.table)
		if err := m.dialector.Migrator(tx).AutoMigrate(sd.dst); err != nil {
			return err
		}

	}

	if len(noShardingDsts) > 0 {
		tx := stmt.DB.Session(&gorm.Session{})
		tx.Statement.Settings.Store(ShardingIgnoreStoreKey, nil)
		defer tx.Statement.Settings.Delete(ShardingIgnoreStoreKey)

		if err := m.dialector.Migrator(tx).AutoMigrate(noShardingDsts...); err != nil {
			return err
		}
	}

	return nil
}

func (m ShardingMigrator) DropTable(dst ...interface{}) error {
	shardingDsts, noShardingDsts, err := m.splitShardingDsts(dst...)
	if err != nil {
		return err
	}

	for _, sd := range shardingDsts {
		if err := m.Migrator.DropTable(sd.table); err != nil {
			return err
		}
	}

	if len(noShardingDsts) > 0 {
		if err := m.Migrator.DropTable(noShardingDsts...); err != nil {
			return err
		}
	}

	return nil
}

type shardingDst struct {
	table string
	dst   interface{}
}

// splite sharding or normal dsts
func (m ShardingMigrator) splitShardingDsts(dsts ...interface{}) (shardingDsts []shardingDst,
	noShardingDsts []interface{}, err error) {

	shardingDsts = make([]shardingDst, 0)
	noShardingDsts = make([]interface{}, 0)
	for _, model := range dsts {
		stmt := &gorm.Statement{DB: m.sharding.DB}
		err = stmt.Parse(model)
		if err != nil {
			return
		}

		if cfg, ok := m.sharding.configs[stmt.Table]; ok {
			// support sharding table
			suffixs := cfg.ShardingSuffixs()
			if len(suffixs) == 0 {
				err = fmt.Errorf("sharding table:%s suffixs is empty", stmt.Table)
				return
			}

			for _, suffix := range suffixs {
				shardingTable := stmt.Table + suffix
				shardingDsts = append(shardingDsts, shardingDst{
					table: shardingTable,
					dst:   model,
				})
			}

			if cfg.DoubleWrite {
				noShardingDsts = append(noShardingDsts, model)
			}
		} else {
			noShardingDsts = append(noShardingDsts, model)
		}
	}
	return
}
