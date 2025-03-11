package sharding

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

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

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	_, stQuery, table, err := pool.sharding.resolve(query, args...)

	// Handle errors with DoubleWrite fallback
	if err != nil {
		if err == ErrMissingShardingKey && table != "" {
			if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
				// Execute on the original table
				result, execErr := pool.ConnPool.ExecContext(ctx, query, args...)
				// Log and return
				return result, execErr
			}
		}
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	var ftResult sql.Result
	// Execute the main table query FIRST if DoubleWrite is enabled
	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			// Key change here - execute on the main table with the original query
			ftResult, _ = pool.ConnPool.ExecContext(ctx, query, args...)
		}
	}

	// Then execute the sharded query
	stResult, err := pool.ConnPool.ExecContext(ctx, stQuery, args...)
	if err != nil {
		// If sharded query fails but main table worked, return main result
		if ftResult != nil {
			return ftResult, nil
		}
		return nil, err
	}

	return stResult, err
}

// https://github.com/go-gorm/gorm/blob/v1.21.11/callbacks/query.go#L18
func (pool ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var (
		curTime = time.Now()
	)

	_, stQuery, table, err := pool.sharding.resolve(query, args...)

	// Check if we got ErrMissingShardingKey but DoubleWrite is enabled
	if err != nil {
		// If it's a missing sharding key error and the table has DoubleWrite enabled,
		// proceed with the original query
		if err == ErrMissingShardingKey && table != "" {
			if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
				// Query from the original table using the original query
				pool.sharding.querys.Store("last_query", query)
				rows, queryErr := pool.ConnPool.QueryContext(ctx, query, args...)
				pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
					return pool.sharding.Explain(query, args...), 0
				}, pool.sharding.Error)
				return rows, queryErr
			}
		}
		return nil, err
	}

	pool.sharding.querys.Store("last_query", stQuery)

	// Check if this is an INSERT operation by looking for 'INSERT INTO' in the query
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")

	// Handle double-write for INSERT operations
	if isInsert && table != "" {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			// Execute the INSERT on the main table first
			// For inserts that use QueryContext (with RETURNING clause), we need to
			// execute on the main table with ExecContext since we don't need the returned values
			_, err := pool.ConnPool.ExecContext(ctx, query, args...)
			if err != nil {
				log.Printf("Error double-writing to main table: %v", err)
				// Continue anyway with the sharded table operation
			} else {
				log.Printf("Successfully double-wrote to main table %s", table)
			}
		}
	}

	// Then execute the query on the sharded table
	rows, err := pool.ConnPool.QueryContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		return pool.sharding.Explain(stQuery, args...), 0
	}, pool.sharding.Error)

	return rows, err
}
func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	_, stQuery, table, err := pool.sharding.resolve(query, args...)

	// Check if this is an INSERT operation for double write
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")

	// If error and DoubleWrite is enabled, use original query
	if err != nil && err == ErrMissingShardingKey && table != "" {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			pool.sharding.querys.Store("last_query", query)
			return pool.ConnPool.QueryRowContext(ctx, query, args...)
		}
		// For other errors, we can't return an error from this method, but the Row will error when used
	}

	// Handle double-write for INSERT operations
	if isInsert && table != "" && err == nil {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			// Execute the INSERT on the main table first
			pool.ConnPool.ExecContext(ctx, query, args...)
			// We don't check for errors because QueryRowContext can't return them
		}
	}

	pool.sharding.querys.Store("last_query", stQuery)
	return pool.ConnPool.QueryRowContext(ctx, stQuery, args...)
}

// BeginTx Implement ConnPoolBeginner.BeginTx
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		return basePool.BeginTx(ctx, opt)
	}

	return pool, nil
}

// Implement TxCommitter.Commit
func (pool *ConnPool) Commit() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	return nil
}

// Implement TxCommitter.Rollback
func (pool *ConnPool) Rollback() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return nil
}

func (pool *ConnPool) Ping() error {
	return nil
}
