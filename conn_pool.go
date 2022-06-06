package sharding

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

// ConnPool Implement a ConnPool for replace db.Statement.ConnPool in Gorm
type ConnPool struct {
	// db, This is global db instance
	sharding *Sharding
	gorm.ConnPool
}

func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

func (pool ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok {
			if r.DoubleWrite {
				pool.ConnPool.ExecContext(ctx, ftQuery, args...)
			}
		}
	}

	return pool.ConnPool.ExecContext(ctx, stQuery, args...)
}

// https://github.com/go-gorm/gorm/blob/v1.21.11/callbacks/query.go#L18
func (pool ConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok {
			if r.DoubleWrite {
				pool.ConnPool.ExecContext(ctx, ftQuery, args...)
			}
		}
	}

	return pool.ConnPool.QueryContext(ctx, stQuery, args...)
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	_, query, _, _ = pool.sharding.resolve(query, args...)
	pool.sharding.querys.Store("last_query", query)

	return pool.ConnPool.QueryRowContext(ctx, query, args...)
}

// BeginTx Implement ConnPoolBeginner.BeginTx
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	switch basePool := pool.ConnPool.(type) {
	case gorm.ConnPoolBeginner:
		return basePool.BeginTx(ctx, opt)
	case gorm.TxBeginner:
		tx, err := basePool.BeginTx(ctx, opt)
		if err != nil {
			return nil, err
		}
		return &ConnPool{pool.sharding, tx}, nil
	}

	return pool, gorm.ErrInvalidTransaction
}

// Commit Implement TxCommitter.Commit
func (pool *ConnPool) Commit() error {
	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	return gorm.ErrInvalidTransaction
}

// Rollback Implement TxCommitter.Rollback
func (pool *ConnPool) Rollback() error {
	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return gorm.ErrInvalidTransaction
}

func (pool *ConnPool) Ping() error {
	return nil
}
