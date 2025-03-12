package sharding

import (
	"context"
	"database/sql"
	"errors"
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
	var (
		curTime = time.Now()
	)
	// Get the query resolution without holding a lock
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	log.Printf("ExecContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)
	// Store the last query safely
	pool.sharding.querys.Store("last_query", stQuery)
	// Handle errors with DoubleWrite fallback
	if err != nil {
		if errors.Is(err, ErrMissingShardingKey) && table != "" {
			if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
				// Execute on the original table
				var result sql.Result
				pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
					result, err = pool.ConnPool.ExecContext(ctx, ftQuery, args...)
					rowsAffected, _ = result.RowsAffected()
					return pool.sharding.Explain(ftQuery, args...), rowsAffected
				}, pool.sharding.Error)
				// Log and return
				return result, err
			}
		}
		return nil, err
	}

	// Execute the main table query FIRST if DoubleWrite is enabled
	if table != "" {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			// Key change here - execute on the main table with the original query
			pool.ConnPool.ExecContext(ctx, ftQuery, args...)
		}
	}

	// Then execute the sharded query
	var result sql.Result
	result, err = pool.ConnPool.ExecContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		rowsAffected, _ = result.RowsAffected()
		return pool.sharding.Explain(stQuery, args...), rowsAffected
	}, pool.sharding.Error)

	return result, err
}

func (pool *ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var curTime = time.Now()

	// Get the query resolution without locking
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	log.Printf("QueryContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)
	// Check for ErrInsertDiffSuffix first and return it immediately
	if err != nil && errors.Is(err, ErrInsertDiffSuffix) {
		return nil, err
	}

	// Store the query safely using sync.Map
	pool.sharding.querys.Store("last_query", stQuery)
	// Thread-safe access to configs
	if table != "" && err != nil && errors.Is(err, ErrMissingShardingKey) {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			pool.sharding.querys.Store("last_query", query)
			// Query from the original table using the original query
			rows, queryErr := pool.ConnPool.QueryContext(ctx, query, args...)
			pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
				return pool.sharding.Explain(query, args...), 0
			}, pool.sharding.Error)
			return rows, queryErr
		}
		return nil, err
	}

	// If there's any other error, return it
	if err != nil {
		return nil, err
	}

	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")
	if isInsert && table != "" {
		if r, ok := pool.sharding.configs[table]; ok && r.DoubleWrite {
			// Execute the INSERT on the main table first
			// For inserts that use QueryContext (with RETURNING clause), we need to
			// execute on the main table with ExecContext since we don't need the returned values
			_, err := pool.ConnPool.QueryContext(ctx, query, args...)
			if err != nil {
				log.Printf("Error double-writing to main table: %v", err)
				// Continue anyway with the sharded table operation
			} else {
				log.Printf("Successfully double-wrote to main table %s", table)
			}
		}
	}

	// Execute the query
	rows, err := pool.ConnPool.QueryContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		return pool.sharding.Explain(stQuery, args...), 0
	}, pool.sharding.Error)

	return rows, err
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// Get the query resolution without holding a lock
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	log.Printf("QueryRowContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)
	pool.sharding.querys.Store("last_query", stQuery)

	// Check if this is an INSERT operation for double write
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")

	// If error and DoubleWrite is enabled, use original query
	if err != nil && errors.Is(err, ErrMissingShardingKey) && table != "" {
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
			pool.ConnPool.QueryRowContext(ctx, query, args...)
			// We don't check for errors because QueryRowContext can't return them
		}
	}

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
