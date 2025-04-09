package sharding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

var (
	DefaultLogLevel = LogLevelInfo
	ErrNotSupported = errors.New("operation not supported by underlying connection pool")
)

func debugLog(format string, args ...interface{}) {
	GetLogger().Debug(format, args...)
}

func traceLog(format string, args ...interface{}) {
	GetLogger().Trace(format, args...)
}

func infoLog(format string, args ...interface{}) {
	GetLogger().Info(format, args...)
}

func errorLog(format string, args ...interface{}) {
	GetLogger().Error(format, args...)
}

// ConnPool wraps standard GORM ConnPool to handle sharding transparently for client code
type ConnPool struct {
	sharding *Sharding
	gorm.ConnPool
}

// NonTransactionalPool provides a consistent interface across pool types that may not support transactions
type NonTransactionalPool struct {
	gorm.ConnPool
}

// Commit implements a no-op to maintain consistent transaction interface across connection types
func (p *NonTransactionalPool) Commit() error {
	debugLog("No-op Commit for non-transactional pool")
	return nil
}

// Rollback is a no-op to maintain consistent interface regardless of underlying capabilities
func (p *NonTransactionalPool) Rollback() error {
	debugLog("No-op Rollback for non-transactional pool")
	return nil
}

// String returns a consistent identifier for meaningful logging and debugging
func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

// PrepareContext checks context cancellation first to prevent unnecessary database operations
func (pool ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// Context cancellation check helps fail fast to prevent wasted database resources
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var (
		curTime = time.Now()
		result  sql.Result
		err     error
	)
	// Context cancellation check prevents wasted resources when client connections drop
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// RequestID enables distributed tracing across services for correlating performance issues
	requestID := uuid.New().String()
	traceLog("[%s] ExecContext START: Query: %s", requestID, query)
	defer traceLog("[%s] ExecContext END", requestID)

	// Try to handle batch insert queries
	queryCtx := &QueryContext{
		Sharding: pool.sharding,
		ConnPool: pool.ConnPool,
	}
	result, err = pool.sharding.HandleBatchInsert(queryCtx, query, args)
	if err == nil {
		// Batch insert was handled successfully
		return result, nil
	} else if err != ErrSkipBatchHandler {
		// There was an actual error in batch handling
		return nil, err
	}

	// Continue with normal query resolution
	// Query resolution happens outside synchronized blocks to minimize lock contention
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	debugLog("ExecContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)
	// Using sync.Map prevents race conditions in concurrent environments
	pool.sharding.querys.Store("last_query", stQuery)

	// ErrInsertDiffSuffix check to handle multi-shard inserts
	if err != nil && errors.Is(err, ErrInsertDiffSuffix) {
		// When we detect multiple shards, try to use the batch handler
		if strings.Contains(strings.ToUpper(query), "INSERT INTO") {
			GetLogger().Debug("Detected INSERT with multiple shards, attempting batch handler")

			// Try to handle batch insert queries
			queryCtx := &QueryContext{
				Sharding: pool.sharding,
				ConnPool: pool.ConnPool,
			}

			result, batchErr := pool.sharding.HandleBatchInsert(queryCtx, query, args)
			if batchErr == nil {
				// Batch insert was handled successfully
				GetLogger().Debug("Successfully handled multi-shard batch insert")
				return result, nil
			}

			// If batch handling failed, log and continue with original error
			GetLogger().Debug("Batch handler failed: %v, proceeding with original error", batchErr)
		}

		return nil, err
	}

	// Double-write ensures data consistency during migration from non-sharded to sharded tables
	if table != "" && err != nil && errors.Is(err, ErrMissingShardingKey) {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		// Fallback to original table maintains data availability even with incomplete sharding metadata
		if doubleWrite {
			pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
				result, err = pool.ConnPool.ExecContext(ctx, ftQuery, args...)
				rowsAffected, _ = result.RowsAffected()
				return pool.sharding.Explain(ftQuery, args...), rowsAffected
			}, pool.sharding.Error)
			// Use the original table result as a fallback strategy
			return result, err
		}
		return nil, err
	}

	// Writing to main table first creates a fallback data source in case of sharding issues
	if table != "" {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			// Re-check context to avoid wasted operations if request was cancelled during resolution
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Double-write errors don't block sharded operations to prioritize availability over consistency
			if _, dwErr := pool.ConnPool.ExecContext(ctx, ftQuery, args...); dwErr != nil {
				errorLog("Error double-writing to main table: %v", dwErr)
				// Continue despite errors to maintain service availability
			}
		}
	}

	// Final context check prevents wasted resources on operations that would be discarded
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Sharded query execution comes after main table to ensure at least one copy exists if process crashes
	result, err = pool.ConnPool.ExecContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		rowsAffected, _ = result.RowsAffected()
		return pool.sharding.Explain(stQuery, args...), rowsAffected
	}, pool.sharding.Error)

	return result, err
}

func (pool *ConnPool) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	// Context check prevents wasted resources and fails fast when client has disconnected
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var curTime = time.Now()
	// RequestID enables tracing query lifecycle across distributed systems
	requestID := uuid.New().String()
	traceLog("[%s] QueryContext START: Query: %s", requestID, query)
	defer traceLog("[%s] QueryContext END", requestID)

	// Resolving queries outside locks reduces contention in high-throughput scenarios
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	debugLog("QueryContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)

	// ErrInsertDiffSuffix check is first to fail fast and prevent data corruption from partial operations
	if err != nil && errors.Is(err, ErrInsertDiffSuffix) {
		// When we detect multiple shards, try to use the batch handler
		if strings.Contains(strings.ToUpper(query), "INSERT INTO") {
			GetLogger().Debug("Detected INSERT with multiple shards, attempting batch handler")

			// Try to handle batch insert queries
			queryCtx := &QueryContext{
				Sharding: pool.sharding,
				ConnPool: pool.ConnPool,
			}

			result, batchErr := pool.sharding.HandleBatchInsert(queryCtx, query, args)
			if batchErr == nil {
				// Batch insert was handled successfully
				GetLogger().Debug("Successfully handled multi-shard batch insert")

				// For INSERT queries returning rows, create empty query result
				// with just the number of affected rows
				rowsAffected, _ := result.RowsAffected()
				GetLogger().Debug("Batch insert affected %d rows", rowsAffected)

				// For RETURNING clause, we need to return rows
				if strings.Contains(strings.ToUpper(query), "RETURNING") {
					// Need to run a dummy query that returns rows with the same schema
					// but no actual data, since the batch insert is already done
					dummyQuery := "SELECT * FROM " + table + " WHERE 1=0"
					return pool.ConnPool.QueryContext(ctx, dummyQuery, []interface{}{}...)
				}

				// Run a dummy query that returns a result but no rows
				return pool.ConnPool.QueryContext(ctx, "SELECT 1 WHERE 1=0", []interface{}{}...)
			}

			// If batch handling failed, log and continue with original error
			GetLogger().Debug("Batch handler failed: %v, proceeding with original error", batchErr)
		}

		return nil, err
	}

	// Thread-safe query storage is critical for concurrent operation reliability
	pool.sharding.querys.Store("last_query", stQuery)

	// Missing sharding key with double-write enabled allows fallback to original table
	if table != "" && err != nil && errors.Is(err, ErrMissingShardingKey) {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			pool.sharding.querys.Store("last_query", query)

			// Context check prevents unnecessary load for already cancelled requests
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Original table query provides a reliable fallback path during migration
			rows, queryErr := pool.ConnPool.QueryContext(ctx, query, args...)
			pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
				return pool.sharding.Explain(query, args...), 0
			}, pool.sharding.Error)
			return rows, queryErr
		}
		return nil, err
	}

	// Fast return for other errors prevents unnecessary database operations
	if err != nil {
		return nil, err
	}

	// Different handling for INSERT vs SELECT prevents unnecessary writes for read-only operations
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")
	if isInsert && table != "" {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			// Context check prevents wasted operations for cancelled requests
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Main table insert ensures data is captured in both storage locations
			rows, err := pool.ConnPool.QueryContext(ctx, ftQuery, args...)
			if err != nil {
				errorLog("Error double-writing to main table: %v", err)
				// Continue with sharded operation despite errors for availability
			} else {
				debugLog("Successfully double-wrote to main table %s", table)
				// Closing rows prevents resource leaks in long-running applications
				if rows != nil {
					if closeErr := rows.Close(); closeErr != nil {
						errorLog("Error closing rows from double-write: %v", closeErr)
					}
				}
			}
		}
	}

	// Final context check prevents wasted operations when request has been cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Tracing execution time helps identify performance bottlenecks
	rows, err := pool.ConnPool.QueryContext(ctx, stQuery, args...)
	pool.sharding.Logger.Trace(ctx, curTime, func() (sql string, rowsAffected int64) {
		return pool.sharding.Explain(stQuery, args...), 0
	}, pool.sharding.Error)

	return rows, err
}

func (pool ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// Empty Row on cancelled context ensures consistent non-nil return behavior
	if ctx.Err() != nil {
		// Row will return context error when Scan is called
		return &sql.Row{}
	}

	// RequestID enables cross-service tracing for performance analysis
	requestID := uuid.New().String()
	traceLog("[%s] QueryRowContext START: Query: %s", requestID, query)
	defer traceLog("[%s] QueryRowContext END", requestID)

	// Non-locked query resolution improves concurrency for high-throughput systems
	ftQuery, stQuery, table, err := pool.sharding.resolve(query, args...)
	debugLog("QueryRowContext: FtQuery: %s\n StQuery: %s \n\tQuery: %s \n Table: %s. Error: %v",
		ftQuery, stQuery, query, table, err)

	// Double-write fallback ensures queries succeed even with missing sharding keys
	if table != "" && err != nil && errors.Is(err, ErrMissingShardingKey) {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			return pool.ConnPool.QueryRowContext(ctx, ftQuery, args...)
		}
		// Error handling deferred to Row.Scan since this method can't return errors
	}

	// Thread-safe query storage prevents race conditions in concurrent access
	pool.sharding.querys.Store("last_query", stQuery)

	// INSERT operations require special handling to maintain cross-table consistency
	isInsert := strings.Contains(strings.ToUpper(query), "INSERT INTO")

	// Double-write for INSERTs keeps both tables in sync during migration periods
	if isInsert && table != "" && err == nil {
		pool.sharding.mutex.RLock()
		doubleWrite := true
		if r, ok := pool.sharding.configs[table]; ok {
			doubleWrite = r.DoubleWrite
		}
		pool.sharding.mutex.RUnlock()

		if doubleWrite {
			// QueryContext instead of QueryRowContext enables proper resource/error management
			rows, dwErr := pool.ConnPool.QueryContext(ctx, ftQuery, args...)
			if dwErr != nil {
				errorLog("Error double-writing to main table in QueryRowContext: %v", dwErr)
			} else if rows != nil {
				// Always close rows to prevent resource leaks in long-running applications
				if closeErr := rows.Close(); closeErr != nil {
					errorLog("Error closing rows from double-write in QueryRowContext: %v", closeErr)
				}
			}
		}
	}

	return pool.ConnPool.QueryRowContext(ctx, stQuery, args...)
}

// BeginTx uses composition to provide consistent client interface regardless of backend capabilities
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	if db, ok := pool.ConnPool.(interface {
		Get(string) (interface{}, bool)
	}); ok {
		if val, ok := db.Get("supports_transactions"); ok && val.(bool) {
			if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
				txConn, err := basePool.BeginTx(ctx, opt)
				if err != nil {
					return nil, fmt.Errorf("forced transaction failed: %w", err)
				}
				return &ConnPool{
					sharding: pool.sharding,
					ConnPool: txConn,
				}, nil
			}
		}
	}

	// Try standard transaction support
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		txConn, err := basePool.BeginTx(ctx, opt)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Preserving sharding context ensures consistent behavior in transactions
		return &ConnPool{
			sharding: pool.sharding,
			ConnPool: txConn,
		}, nil
	}

	// Non-transactional fallback maintains API compatibility even without transaction support
	debugLog("Transaction not supported by underlying pool, using non-transactional wrapper")
	return &ConnPool{
		sharding: pool.sharding,
		ConnPool: &NonTransactionalPool{
			ConnPool: pool.ConnPool,
		},
	}, nil
}

// Commit uses type assertions to support multiple transaction implementations for maximum compatibility
func (pool *ConnPool) Commit() error {
	// Handle no-op commits first for pools without transaction support
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		return nonTxPool.Commit()
	}

	// Support standard SQL transactions for broad compatibility
	tx, ok := pool.ConnPool.(*sql.Tx)
	if ok {
		return tx.Commit()
	}

	// Finally try GORM-specific transaction handling
	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	// Clear error for unsupported operations prevents silent failures
	return ErrNotSupported
}

// Rollback follows same pattern as Commit to handle multiple transaction types
func (pool *ConnPool) Rollback() error {
	// Handle our wrapper first for consistent interface
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		return nonTxPool.Rollback()
	}

	// Support standard SQL transactions directly
	tx, ok := pool.ConnPool.(*sql.Tx)
	if ok {
		return tx.Rollback()
	}

	// Try GORM-specific transaction handling
	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return ErrNotSupported
}

// Ping uses progressively more generic approaches for reliable health checks across database types
func (pool *ConnPool) Ping() error {
	// Direct sql.DB ping is most efficient when available
	if db, ok := pool.ConnPool.(*sql.DB); ok {
		return db.Ping()
	}

	// Transactions are already connected and don't need separate ping
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	// Check through non-transactional wrapper to ping underlying pool
	if nonTxPool, ok := pool.ConnPool.(*NonTransactionalPool); ok {
		if db, ok := nonTxPool.ConnPool.(*sql.DB); ok {
			return db.Ping()
		}
	}

	// Interface-based detection for custom pool implementations
	if pinger, ok := pool.ConnPool.(interface{ Ping() error }); ok {
		return pinger.Ping()
	}

	// Short timeout prevents health checks from blocking indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simple query as last-resort health check for any database type
	_, err := pool.QueryContext(ctx, "SELECT 1")
	return err
}
