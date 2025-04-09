// Package sharding provides database sharding capabilities for GORM.
package sharding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// insertRegex is a regular expression pattern used to parse INSERT statements.
	// It captures the table name, column names, and values portion.
	insertRegex = regexp.MustCompile(`(?i)INSERT\s+INTO\s+"?([a-zA-Z0-9_]+)"?\s+\((.*?)\)\s+VALUES\s+(.*)`)

	// shardingKeyRegex is a regular expression pattern used to find column names in quoted format.
	shardingKeyRegex = regexp.MustCompile(`"([^"]+)"`)

	// ErrSkipBatchHandler is returned when a query should be processed by the standard handler
	// rather than the batch handler. This is not an error condition, but a control flow signal.
	ErrSkipBatchHandler = errors.New("skip batch handler")

	// Additional error types for better error handling

	// ErrInvalidInsertFormat indicates that the provided SQL doesn't match the expected INSERT format.
	ErrInvalidInsertFormat = errors.New("invalid INSERT statement format")

	// ErrNoShardingKey indicates that the sharding key column wasn't found in the query.
	ErrNoShardingKey = errors.New("sharding key not found in columns")

	// ErrShardingKeyExtract indicates a failure to extract the sharding key value from parameters.
	ErrShardingKeyExtract = errors.New("failed to extract sharding key value")

	// ErrShardResolution indicates a failure to determine the appropriate shard for a key value.
	ErrShardResolution = errors.New("failed to resolve shard for key value")

	// ErrParameterMismatch indicates a mismatch between the parameters expected by the query
	// and the parameters provided.
	ErrParameterMismatch = errors.New("parameter count mismatch")
)

// QueryCacheEntry represents a cached query and its parsed components
type QueryCacheEntry struct {
	// Original query and its parsed components
	tableName  string
	columnsStr string
	valuesStr  string

	// Position of the sharding key in the columns list
	shardingKeyIndex int

	// ON CONFLICT clause if present
	conflictClause string

	// Last access time for cache eviction
	lastAccess time.Time
}

// QueryCache caches parsed queries to avoid repeated parsing
type QueryCache struct {
	mu       sync.RWMutex
	entries  map[string]*QueryCacheEntry
	maxSize  int
	hits     int64
	misses   int64
	disabled bool
}

// NewQueryCache creates a new query cache with the specified size
func NewQueryCache(maxSize int) *QueryCache {
	return &QueryCache{
		entries:  make(map[string]*QueryCacheEntry),
		maxSize:  maxSize,
		disabled: maxSize <= 0,
	}
}

// Get retrieves a cached query entry if available
func (cache *QueryCache) Get(query string) (*QueryCacheEntry, bool) {
	if cache.disabled {
		return nil, false
	}

	cache.mu.RLock()
	entry, ok := cache.entries[query]
	cache.mu.RUnlock()

	if ok {
		// Update last access time and hit count
		cache.mu.Lock()
		entry.lastAccess = time.Now()
		cache.hits++
		cache.mu.Unlock()
		return entry, true
	}

	// Update miss count
	cache.mu.Lock()
	cache.misses++
	cache.mu.Unlock()

	return nil, false
}

// Put adds a query to the cache
func (cache *QueryCache) Put(query string, entry *QueryCacheEntry) {
	if cache.disabled {
		return
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Check if we need to evict an entry
	if len(cache.entries) >= cache.maxSize {
		cache.evictLRU()
	}

	// Add the new entry
	cache.entries[query] = entry
}

// evictLRU removes the least recently used cache entry
func (cache *QueryCache) evictLRU() {
	var oldestQuery string
	var oldestTime time.Time

	// Find the oldest entry
	for query, entry := range cache.entries {
		if oldestQuery == "" || entry.lastAccess.Before(oldestTime) {
			oldestQuery = query
			oldestTime = entry.lastAccess
		}
	}

	// Remove the oldest entry
	if oldestQuery != "" {
		delete(cache.entries, oldestQuery)
	}
}

// GetStats returns cache statistics
func (cache *QueryCache) GetStats() map[string]interface{} {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	hitRate := 0.0
	total := cache.hits + cache.misses
	if total > 0 {
		hitRate = float64(cache.hits) / float64(total) * 100.0
	}

	return map[string]interface{}{
		"size":     len(cache.entries),
		"max_size": cache.maxSize,
		"hits":     cache.hits,
		"misses":   cache.misses,
		"hit_rate": hitRate,
	}
}

// queryCache is the global query cache instance
var (
	queryCache     *QueryCache
	queryCacheOnce sync.Once
)

// getQueryCache returns the global query cache, initializing it if necessary
func getQueryCache() *QueryCache {
	queryCacheOnce.Do(func() {
		// Default cache size of 1000 entries
		cacheSize := 1000

		// Use environment variable if set
		if sizeStr := os.Getenv("GORM_SHARDING_CACHE_SIZE"); sizeStr != "" {
			if size, err := strconv.Atoi(sizeStr); err == nil {
				cacheSize = size
			}
		}

		queryCache = NewQueryCache(cacheSize)
	})

	return queryCache
}

// DisableQueryCache disables the query cache
func DisableQueryCache() {
	getQueryCache().disabled = true
}

// EnableQueryCache enables the query cache
func EnableQueryCache() {
	getQueryCache().disabled = false
}

// GetQueryCacheStats returns statistics about the query cache
func GetQueryCacheStats() map[string]interface{} {
	return getQueryCache().GetStats()
}

// SplitBatchInsertByShards splits a batch INSERT statement by shards if records
// map to different shards. It analyzes the INSERT statement, extracts the sharding key values
// for each record, and groups the records by the shard they belong to.
//
// The function returns ErrSkipBatchHandler in cases where batch handling isn't applicable, such as:
// - Non-INSERT queries
// - INSERT queries without multiple value groups
// - INSERT queries for non-sharded tables
//
// This function now always creates separate queries for each unique sharding key value,
// preventing the "can not insert different suffix table in one query" error
// by ensuring all values in each resulting query have the same sharding key.
func (s *Sharding) SplitBatchInsertByShards(query string, args []interface{}) ([]string, [][]interface{}, error) {
	// Only process INSERT statements
	if !strings.Contains(strings.ToUpper(query), "INSERT INTO") {
		return nil, nil, ErrSkipBatchHandler
	}

	var tableName, columnsStr, valuesStr string
	var shardingKeyIndex int
	var conflictClause string

	// Try to get cached query information
	cache := getQueryCache()
	cacheEntry, hit := cache.Get(query)

	if hit {
		// Use cached information
		tableName = cacheEntry.tableName
		columnsStr = cacheEntry.columnsStr
		valuesStr = cacheEntry.valuesStr
		shardingKeyIndex = cacheEntry.shardingKeyIndex
		conflictClause = cacheEntry.conflictClause

		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Query cache hit for query: %s", query)
		}
	} else {
		// Parse the query
		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Query cache miss for query: %s", query)
		}

		// Extract table name and columns from the query
		matches := insertRegex.FindStringSubmatch(query)
		if len(matches) < 4 {
			return nil, nil, fmt.Errorf("%w: could not parse query - %s", ErrInvalidInsertFormat, query)
		}

		tableName = matches[1]
		columnsStr = matches[2]
		valuesStr = matches[3]

		// Check if this table is configured for sharding
		s.mutex.RLock()
		config, exists := s.configs[tableName]
		s.mutex.RUnlock()
		if !exists {
			return nil, nil, ErrSkipBatchHandler
		}

		// Find position of sharding key in columns
		columns := strings.Split(columnsStr, ",")
		shardingKeyIndex = -1
		for i, col := range columns {
			colMatches := shardingKeyRegex.FindStringSubmatch(strings.TrimSpace(col))
			if len(colMatches) > 1 && colMatches[1] == config.ShardingKey {
				shardingKeyIndex = i
				break
			}
		}

		if shardingKeyIndex == -1 {
			return nil, nil, fmt.Errorf("%w: column '%s' not found in query", ErrNoShardingKey, config.ShardingKey)
		}

		// Get ON CONFLICT clause if present
		if strings.Contains(strings.ToUpper(query), "ON CONFLICT") {
			conflictClause = ExtractOnConflictClause(query)
		}

		// Cache the parsed query
		cache.Put(query, &QueryCacheEntry{
			tableName:        tableName,
			columnsStr:       columnsStr,
			valuesStr:        valuesStr,
			shardingKeyIndex: shardingKeyIndex,
			conflictClause:   conflictClause,
			lastAccess:       time.Now(),
		})
	}

	// Parse the values - this is the complex part
	valueGroups := ParseValueGroups(valuesStr)
	if len(valueGroups) <= 1 {
		return nil, nil, ErrSkipBatchHandler // Not a batch insert
	}

	// Check if this table is configured for sharding (needed if using cache)
	s.mutex.RLock()
	config, exists := s.configs[tableName]
	s.mutex.RUnlock()
	if !exists {
		return nil, nil, ErrSkipBatchHandler
	}

	// New approach: Group by the sharding key value explicitly
	// This ensures values with same sharding key stay together
	keyValueGroups := make(map[interface{}]struct {
		valueGroups []string
		params      []interface{}
		suffix      string
	})

	paramIndex := 0
	for _, group := range valueGroups {
		// Extract parameters for this group
		paramCount := CountParams(group)
		if paramIndex+paramCount > len(args) {
			return nil, nil, fmt.Errorf("%w: expected %d parameters but got %d",
				ErrParameterMismatch, paramIndex+paramCount, len(args))
		}

		// Get the sharding key value
		shardingKeyParamIndex := getParamIndexFromGroup(group, shardingKeyIndex)
		if shardingKeyParamIndex < 0 || shardingKeyParamIndex >= len(args) {
			return nil, nil, fmt.Errorf("%w: invalid parameter index %d",
				ErrShardingKeyExtract, shardingKeyParamIndex)
		}

		shardingKeyValue := args[shardingKeyParamIndex]

		// Get the suffix for this value
		suffix, err := getSuffix(shardingKeyValue, 0, true, config)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrShardResolution, err)
		}

		// Get or create the group for this key value
		keyGroup, exists := keyValueGroups[shardingKeyValue]
		if !exists {
			keyGroup = struct {
				valueGroups []string
				params      []interface{}
				suffix      string
			}{
				valueGroups: []string{},
				params:      []interface{}{},
				suffix:      suffix,
			}
		}

		// Add the value group and parameters to this key group
		keyGroup.valueGroups = append(keyGroup.valueGroups, group)
		groupParams := args[paramIndex : paramIndex+paramCount]
		keyGroup.params = append(keyGroup.params, groupParams...)
		keyValueGroups[shardingKeyValue] = keyGroup

		paramIndex += paramCount
	}

	// If we have only one group and it's a small batch, let the standard handler process it
	// This is an optimization for simple cases
	if len(keyValueGroups) == 1 && len(valueGroups) <= 10 {
		return nil, nil, ErrSkipBatchHandler
	}

	// Generate a query for each unique sharding key value
	var queries []string
	var queryParams [][]interface{}

	for keyValue, keyGroup := range keyValueGroups {
		shardTableName := tableName + keyGroup.suffix

		// Construct a query for this key value
		shardQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			s.quoteIdent(shardTableName),
			columnsStr,
			strings.Join(keyGroup.valueGroups, ","))

		// If there's an ON CONFLICT clause, include it
		if conflictClause != "" {
			shardQuery += " " + conflictClause
		}

		queries = append(queries, shardQuery)
		queryParams = append(queryParams, keyGroup.params)

		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Created batch insert query for key value %v: %s", keyValue, shardQuery)
		}
	}

	return queries, queryParams, nil
}

// ParseValueGroups extracts individual VALUE groups from the VALUES portion of an INSERT statement.
// It properly handles nested parentheses and string literals to ensure correct parsing.
//
// Parameters:
//   - valuesStr: The VALUES portion of an INSERT statement (e.g., "($1, $2), ($3, $4)")
//
// Returns:
//   - []string: Array of value group strings (e.g., ["($1, $2)", "($3, $4)"])
func ParseValueGroups(valuesStr string) []string {
	var groups []string
	depth := 0
	start := 0
	inString := false

	for i, char := range valuesStr {
		// Skip characters in string literals
		if char == '\'' {
			if i == 0 || valuesStr[i-1] != '\\' { // Not an escaped quote
				inString = !inString
			}
			continue
		}

		if inString {
			continue // Skip processing while inside a string
		}

		switch char {
		case '(':
			if depth == 0 {
				start = i
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				groups = append(groups, valuesStr[start:i+1])
			}
		}
	}

	return groups
}

// CountParams counts the number of parameter placeholders (e.g., $1, $2) in a value group.
// It handles string literals correctly to avoid counting dollar signs within quoted strings.
//
// Parameters:
//   - group: A value group string (e.g., "($1, 'text', $2)")
//
// Returns:
//   - int: The number of parameter placeholders found
func CountParams(group string) int {
	count := 0
	inString := false

	for i, char := range group {
		// Ignore parameter references inside string literals
		if char == '\'' {
			if i == 0 || group[i-1] != '\\' { // Not an escaped quote
				inString = !inString
			}
			continue
		}

		if inString {
			continue
		}

		// Count dollar-sign parameters
		if char == '$' {
			// Make sure it's actually a parameter and not part of a string
			if i+1 < len(group) && group[i+1] >= '0' && group[i+1] <= '9' {
				count++
			}
		}
	}

	return count
}

// getParamIndexFromGroup extracts the parameter index at the specified position within a value group.
// It returns the 0-based index of the parameter in the args array.
//
// Parameters:
//   - group: A value group string (e.g., "($1, $2, $3)")
//   - position: The position of the parameter to extract (0-based index in the group)
//
// Returns:
//   - int: The 0-based index of the parameter in the args array, or -1 if invalid
func getParamIndexFromGroup(group string, position int) int {
	parts := strings.Split(strings.Trim(group, "()"), ",")
	if position >= len(parts) {
		return -1
	}

	paramStr := strings.TrimSpace(parts[position])
	if !strings.HasPrefix(paramStr, "$") {
		return -1
	}

	// Extract the number after $
	paramIndexStr := paramStr[1:]
	paramIndex := 0
	_, err := fmt.Sscanf(paramIndexStr, "%d", &paramIndex)
	if err != nil {
		return -1
	}

	return paramIndex - 1 // Convert from 1-based to 0-based
}

// ExtractOnConflictClause extracts the ON CONFLICT clause from a query if present.
// This allows the clause to be preserved when splitting queries.
//
// Parameters:
//   - query: The SQL query to extract from
//
// Returns:
//   - string: The ON CONFLICT clause, or empty string if not present
func ExtractOnConflictClause(query string) string {
	upperQuery := strings.ToUpper(query)
	conflictPos := strings.Index(upperQuery, "ON CONFLICT")
	if conflictPos == -1 {
		return ""
	}

	return query[conflictPos:]
}

// QueryContext wraps the execution context for a batch insert operation.
// It holds a reference to the sharding instance and the connection pool.
type QueryContext struct {
	// Sharding is the sharding instance handling the operation
	Sharding *Sharding

	// ConnPool is the connection pool used to execute queries
	ConnPool ConnPoolExecer
}

// ConnPoolExecer defines the interface for executing SQL queries.
// This is a subset of the gorm.ConnPool interface that only includes
// the methods needed for batch insert operations.
type ConnPoolExecer interface {
	// ExecContext executes a query with context and returns the result
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Debug outputs a debug message if the log level is high enough.
func (ctx *QueryContext) Debug(format string, args ...interface{}) {
	if DefaultLogLevel >= LogLevelDebug {
		debugLog(format, args...)
	}
}

// execContext executes a query using the context's connection pool.
func (ctx *QueryContext) execContext(query string, args ...interface{}) (sql.Result, error) {
	return ctx.ConnPool.ExecContext(context.Background(), query, args...)
}

// HandleBatchInsert processes a batch INSERT statement and splits it into multiple shard-specific
// queries if necessary. It executes each query separately and combines the results.
//
// Parameters:
//   - ctx: The query context containing the sharding instance and connection pool
//   - query: The SQL INSERT statement to process
//   - args: The parameter values for the query
//
// Returns:
//   - sql.Result: A combined result representing all executed queries
//   - error: Error if any occurred during processing or execution
//
// If the query is not eligible for batch handling (e.g., not an INSERT statement or all records
// belong to the same shard), ErrSkipBatchHandler is returned to indicate that standard processing
// should be used instead.
func (s *Sharding) HandleBatchInsert(ctx *QueryContext, query string, args []interface{}) (sql.Result, error) {
	// Try to split the batch insert
	queries, queryParams, err := s.SplitBatchInsertByShards(query, args)
	if err != nil {
		if errors.Is(err, ErrSkipBatchHandler) {
			// Not a batch insert or not handled, proceed with normal execution
			return nil, err
		}

		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Error splitting batch insert: %v", err)
		}
		return nil, err
	}

	// Execute each query separately
	var lastResult sql.Result
	var rowsAffected int64

	for i, shardQuery := range queries {
		if DefaultLogLevel >= LogLevelDebug {
			debugLog("Executing shard-specific batch insert (%d/%d): %s", i+1, len(queries), shardQuery)
		}

		result, err := ctx.execContext(shardQuery, queryParams[i]...)
		if err != nil {
			return nil, fmt.Errorf("error executing shard %d: %w", i, err)
		}

		// Keep track of rows affected for final result
		rows, _ := result.RowsAffected()
		rowsAffected += rows
		lastResult = result
	}

	if DefaultLogLevel >= LogLevelInfo {
		infoLog("Successfully executed batch insert across %d shards, affecting %d rows",
			len(queries), rowsAffected)
	}

	// Create a result that combines the rows affected
	return &batchResult{
		lastResult:   lastResult,
		rowsAffected: rowsAffected,
	}, nil
}

// batchResult implements the sql.Result interface to represent combined results
// from multiple query executions. It combines the rows affected from all queries
// while preserving the last insert ID from the last executed query.
type batchResult struct {
	// lastResult is the result from the last executed query
	lastResult sql.Result

	// rowsAffected is the sum of rows affected across all executed queries
	rowsAffected int64
}

// LastInsertId returns the last insert ID from the last executed query.
// This is somewhat arbitrary since multiple queries were executed, but
// it satisfies the sql.Result interface.
func (r *batchResult) LastInsertId() (int64, error) {
	// Return the last insert ID from the last query executed
	if r.lastResult == nil {
		return 0, errors.New("no result available")
	}
	return r.lastResult.LastInsertId()
}

// RowsAffected returns the total number of rows affected across all executed queries.
func (r *batchResult) RowsAffected() (int64, error) {
	// Return the combined rows affected
	return r.rowsAffected, nil
}

// FormatSuffix is a helper function to format sharding suffixes.
// It applies the format string to the given value.
func FormatSuffix(format string, value interface{}) string {
	return fmt.Sprintf(format, value)
}
