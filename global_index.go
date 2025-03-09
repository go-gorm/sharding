package sharding

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// GlobalIndexOptions configures how the global index operates
type GlobalIndexOptions struct {
	// AutoRebuild determines whether the index should automatically rebuild when missing data
	AutoRebuild bool

	// RebuildThreshold is the percentage of misses allowed before triggering a rebuild (0-100)
	RebuildThreshold int

	// EnableMetrics tracks performance and usage metrics for the global index
	EnableMetrics bool

	// BatchSize controls how many records to process at once during operations
	BatchSize int

	// AsyncUpdates determines if index updates should happen asynchronously
	AsyncUpdates bool
}

// DefaultGlobalIndexOptions returns sensible defaults for GlobalIndexOptions
func DefaultGlobalIndexOptions() GlobalIndexOptions {
	return GlobalIndexOptions{
		AutoRebuild:      true,
		RebuildThreshold: 10,
		EnableMetrics:    true,
		BatchSize:        500,
		AsyncUpdates:     true,
	}
}

// GlobalIndexMetrics tracks usage and performance statistics
type GlobalIndexMetrics struct {
	TotalQueries      int64
	IndexHits         int64
	IndexMisses       int64
	AvgQueryTimeMs    float64
	SlowQueries       int64
	LastFullRebuild   time.Time
	IncrementalBuilds int64
}

// InitGlobalIndices initializes global indexing in the sharding system
func (s *Sharding) InitGlobalIndices() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.globalIndices == nil {
		s.globalIndices = &GlobalIndexRegistry{
			indices: make(map[string]map[string]*GlobalIndex),
		}
	}

	// Create global index table if it doesn't exist
	err := s.DB.AutoMigrate(&GlobalIndexRecord{})
	if err != nil {
		return fmt.Errorf("failed to create global index table: %w", err)
	}

	// Create composite indices for faster lookups
	err = s.DB.Exec("CREATE INDEX IF NOT EXISTS idx_global_index_lookup ON global_index (index_column, index_value)").Error
	if err != nil {
		return fmt.Errorf("failed to create index lookup index: %w", err)
	}

	err = s.DB.Exec("CREATE INDEX IF NOT EXISTS idx_global_index_record ON global_index (table_suffix, record_id)").Error
	if err != nil {
		return fmt.Errorf("failed to create record lookup index: %w", err)
	}

	return nil
}

// EnableGlobalIndex enables global indexing for an existing sharding config
func (s *Sharding) EnableGlobalIndex(tableName string, columns []string, options ...GlobalIndexOptions) error {
	if s.globalIndices == nil {
		if err := s.InitGlobalIndices(); err != nil {
			return err
		}
	}

	// Use default options if none provided
	var opts GlobalIndexOptions
	if len(options) > 0 {
		opts = options[0]
	} else {
		opts = DefaultGlobalIndexOptions()
	}

	// Check if the table is configured for sharding
	config, exists := s.configs[tableName]
	if !exists {
		return fmt.Errorf("table %s is not configured for sharding", tableName)
	}

	gi := &GlobalIndex{
		DB:           s.DB,
		TableName:    tableName,
		IndexColumns: columns,
		Config:       config,
		AutoRebuild:  opts.AutoRebuild,
		Options:      opts,
		metrics:      &GlobalIndexMetrics{},
	}

	// Register the global index
	for _, col := range columns {
		s.globalIndices.Add(tableName, col, gi)
	}

	// Schedule initial build of the index (can be done asynchronously)
	if opts.AsyncUpdates {
		go func() {
			if err := gi.RebuildIndex(context.Background()); err != nil {
				log.Printf("Error during initial index build for table %s: %v", tableName, err)
			}
		}()
	} else {
		if err := gi.RebuildIndex(context.Background()); err != nil {
			return fmt.Errorf("failed to build initial index: %w", err)
		}
	}

	return nil
}

// Enhance the GlobalIndex struct to include the new options
type GlobalIndex struct {
	sync.RWMutex
	DB           *gorm.DB
	TableName    string
	IndexColumns []string
	Config       Config
	AutoRebuild  bool
	Options      GlobalIndexOptions
	metrics      *GlobalIndexMetrics
}

// OptimizeQueryWithGlobalIndex rewrites a query to use the global index
func (s *Sharding) OptimizeQueryWithGlobalIndex(query string, args []interface{}) (string, []interface{}, bool) {
	// Skip optimization for specific query types
	// todo put this as a resolver
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return query, args, false
	}

	// Parse the query
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return query, args, false
	}

	if len(parsed.Stmts) == 0 {
		return query, args, false
	}

	// Extract the table and where conditions
	stmt := parsed.Stmts[0]
	var tableName string
	var conditions []*pg_query.Node

	switch node := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		selectStmt := node.SelectStmt

		// Get table name from FROM clause
		if len(selectStmt.FromClause) > 0 {
			if rangeVar, ok := selectStmt.FromClause[0].Node.(*pg_query.Node_RangeVar); ok {
				tableName = rangeVar.RangeVar.Relname
			}
		}

		// Get conditions from WHERE clause
		if selectStmt.WhereClause != nil {
			conditions = append(conditions, selectStmt.WhereClause)
		}

	case *pg_query.Node_UpdateStmt:
		updateStmt := node.UpdateStmt
		if updateStmt.Relation != nil {
			tableName = updateStmt.Relation.Relname
		}
		if updateStmt.WhereClause != nil {
			conditions = append(conditions, updateStmt.WhereClause)
		}

	case *pg_query.Node_DeleteStmt:
		deleteStmt := node.DeleteStmt
		if deleteStmt.Relation != nil {
			tableName = deleteStmt.Relation.Relname
		}
		if deleteStmt.WhereClause != nil {
			conditions = append(conditions, deleteStmt.WhereClause)
		}

	default:
		// Unsupported statement type
		return query, args, false
	}

	// Check if this table is configured for sharding
	_, exists := s.configs[tableName]
	if !exists || tableName == "" {
		return query, args, false
	}

	// Check if we have global indices for this table
	if s.globalIndices == nil {
		return query, args, false
	}

	// Extract condition columns and values
	for _, condNode := range conditions {
		// Check if any of the conditions match a column with a global index
		queryColumns := extractConditionColumns(condNode)

		for _, col := range queryColumns {
			// If column has a global index
			if gi := s.globalIndices.Get(tableName, col); gi != nil {
				// Try to rewrite query using this global index
				modifiedQuery, modifiedArgs, err := gi.RewriteQueryWithIndex(query, args, col)
				if err == nil {
					return modifiedQuery, modifiedArgs, true
				}
				log.Printf("Failed to rewrite query with global index: %v", err)
			}
		}
	}

	return query, args, false
}

// ConnPoolWithGlobalIndex enhances the connection pool with global index functionality
type ConnPoolWithGlobalIndex struct {
	originalPool gorm.ConnPool
	sharding     *Sharding
}

// NewConnPoolWithGlobalIndex creates a new connection pool with global index support
func NewConnPoolWithGlobalIndex(original gorm.ConnPool, s *Sharding) *ConnPoolWithGlobalIndex {
	return &ConnPoolWithGlobalIndex{
		originalPool: original,
		sharding:     s,
	}
}

// QueryContext intercepts queries and uses global index if applicable
func (cp *ConnPoolWithGlobalIndex) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	newQuery, newArgs, useGlobalIndex := cp.sharding.OptimizeQueryWithGlobalIndex(query, args)
	if useGlobalIndex {
		return cp.originalPool.QueryContext(ctx, newQuery, newArgs...)
	}
	return cp.originalPool.QueryContext(ctx, query, args...)
}

// ExecContext intercepts exec calls and uses global index if applicable
func (cp *ConnPoolWithGlobalIndex) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	newQuery, newArgs, useGlobalIndex := cp.sharding.OptimizeQueryWithGlobalIndex(query, args)
	if useGlobalIndex {
		return cp.originalPool.ExecContext(ctx, newQuery, newArgs...)
	}
	return cp.originalPool.ExecContext(ctx, query, args...)
}

// QueryRowContext intercepts query row calls and uses global index if applicable
func (cp *ConnPoolWithGlobalIndex) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	newQuery, newArgs, useGlobalIndex := cp.sharding.OptimizeQueryWithGlobalIndex(query, args)
	if useGlobalIndex {
		return cp.originalPool.QueryRowContext(ctx, newQuery, newArgs...)
	}
	return cp.originalPool.QueryRowContext(ctx, query, args...)
}

// PrepareContext implements the ConnPool interface
func (cp *ConnPoolWithGlobalIndex) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return cp.originalPool.PrepareContext(ctx, query)
}

// RewriteQueryWithIndex rewrites a query to use the global index for efficient lookup
func (gi *GlobalIndex) RewriteQueryWithIndex(query string, args []interface{}, indexColumn string) (string, []interface{}, error) {
	startTime := time.Now()

	// Check if we need to track metrics
	if gi.Options.EnableMetrics && gi.metrics != nil {
		gi.metrics.TotalQueries++
		defer func() {
			elapsed := time.Since(startTime).Milliseconds()
			// Update average query time with exponential moving average
			gi.metrics.AvgQueryTimeMs = (gi.metrics.AvgQueryTimeMs*0.9 + float64(elapsed)*0.1)
			if elapsed > 100 { // Consider slow if > 100ms
				gi.metrics.SlowQueries++
			}
		}()
	}

	// Find matching records in the global index
	var indexRecords []GlobalIndexRecord
	valueStr, argIdx, found := extractValueForIndexColumn(query, args, indexColumn)
	if !found {
		// Couldn't extract a value for this column
		return query, args, fmt.Errorf("could not extract value for index column %s", indexColumn)
	}

	// Query the global index for matching records
	var dbQuery *gorm.DB
	if argIdx >= 0 && argIdx < len(args) {
		// Use parameter value from args
		dbQuery = gi.DB.Where("index_column = ? AND index_value = ?", indexColumn, fmt.Sprintf("%v", args[argIdx]))
	} else {
		// Use literal value from query
		dbQuery = gi.DB.Where("index_column = ? AND index_value = ?", indexColumn, valueStr)
	}

	err := dbQuery.Find(&indexRecords).Error
	if err != nil {
		return query, args, fmt.Errorf("error querying global index: %w", err)
	}

	// Handle metrics
	if gi.Options.EnableMetrics && gi.metrics != nil {
		if len(indexRecords) > 0 {
			gi.metrics.IndexHits++
		} else {
			gi.metrics.IndexMisses++

			// Check if we need to trigger a rebuild
			missRate := float64(gi.metrics.IndexMisses) / float64(gi.metrics.TotalQueries) * 100
			if gi.AutoRebuild && missRate > float64(gi.Options.RebuildThreshold) {
				go gi.RebuildIndex(context.Background())
			}
		}
	}

	if len(indexRecords) == 0 {
		// No matches found in the index, return a query that will yield no results
		return fmt.Sprintf("SELECT * FROM %s WHERE 1=0 /* noglobalindex */", gi.TableName), args, nil
	}

	// Group records by shard suffix
	suffixToIDs := make(map[string][]int64)
	for _, record := range indexRecords {
		suffixToIDs[record.TableSuffix] = append(suffixToIDs[record.TableSuffix], record.RecordID)
	}

	// Rewrite the query for each shard
	var subQueries []string
	newArgs := make([]interface{}, len(args))
	copy(newArgs, args) // Make a copy of the original args

	for suffix, ids := range suffixToIDs {
		shardTableName := gi.TableName + suffix

		// Rewrite the query to use the specific shard table
		shardQuery := strings.Replace(query, gi.TableName, shardTableName, -1)

		// Add condition to filter by IDs from the global index
		idPlaceholders := make([]string, len(ids))
		for i, id := range ids {
			paramIdx := len(newArgs) + 1
			idPlaceholders[i] = fmt.Sprintf("$%d", paramIdx)
			newArgs = append(newArgs, id)
		}

		// Add ID filter to the WHERE clause
		idFilter := fmt.Sprintf(" id IN (%s)", strings.Join(idPlaceholders, ", "))
		wherePos := strings.Index(strings.ToUpper(shardQuery), "WHERE")
		if wherePos >= 0 {
			// Add to existing WHERE clause
			wherePos += 5 // Skip past "WHERE"
			shardQuery = shardQuery[:wherePos] + idFilter + " AND " + shardQuery[wherePos:]
		} else {
			// Add new WHERE clause
			groupByPos := strings.Index(strings.ToUpper(shardQuery), "GROUP BY")
			orderByPos := strings.Index(strings.ToUpper(shardQuery), "ORDER BY")
			limitPos := strings.Index(strings.ToUpper(shardQuery), "LIMIT")

			insertPos := len(shardQuery)
			if groupByPos > 0 && groupByPos < insertPos {
				insertPos = groupByPos
			}
			if orderByPos > 0 && orderByPos < insertPos {
				insertPos = orderByPos
			}
			if limitPos > 0 && limitPos < insertPos {
				insertPos = limitPos
			}

			shardQuery = shardQuery[:insertPos] + " WHERE " + idFilter + " " + shardQuery[insertPos:]
		}

		subQueries = append(subQueries, shardQuery)
	}

	if len(subQueries) == 0 {
		return query, args, fmt.Errorf("failed to build sub-queries")
	}

	if len(subQueries) == 1 {
		// Only one shard, no need for UNION
		return subQueries[0] + " /* via_global_index */", newArgs, nil
	}

	// Combine multiple shard queries with UNION ALL
	finalQuery := strings.Join(subQueries, " UNION ALL ") + " /* via_global_index */"
	return finalQuery, newArgs, nil
}

// Helper function to extract a value for an index column from a WHERE clause
func extractValueForIndexColumn(query string, args []interface{}, indexColumn string) (string, int, bool) {
	// First, try to find a parameter for this column
	re := regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*\$(\d+)`, indexColumn))
	matches := re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		paramIdx, _ := strconv.Atoi(matches[1])
		if paramIdx > 0 && paramIdx <= len(args) {
			return "", paramIdx - 1, true
		}
	}

	// Try to find a literal value for this column
	re = regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*'([^']*)'`, indexColumn))
	matches = re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return matches[1], -1, true
	}

	// Try to find a numeric literal
	re = regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*(\d+)`, indexColumn))
	matches = re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return matches[1], -1, true
	}

	return "", -1, false
}

// Helper function to extract column names from a WHERE clause
func extractConditionColumns(condNode *pg_query.Node) []string {
	var columns []string

	switch n := condNode.Node.(type) {
	case *pg_query.Node_AExpr:
		// A = B expression
		if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
			columns = append(columns, extractColumnNameFromRef(colRef.ColumnRef))
		}

	case *pg_query.Node_BoolExpr:
		// AND/OR expression, process all arguments
		for _, arg := range n.BoolExpr.Args {
			columns = append(columns, extractConditionColumns(arg)...)
		}
	}

	return columns
}

// Helper function to extract column name from a column reference
func extractColumnNameFromRef(colRef *pg_query.ColumnRef) string {
	for _, field := range colRef.Fields {
		if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
			// For simplicity, return the first string field found
			// In practice, may need to check if this is actually a column name
			return stringNode.String_.Sval
		}
	}
	return ""
}

// GetIndexMetrics returns metrics for the global index
func (gi *GlobalIndex) GetIndexMetrics() *GlobalIndexMetrics {
	if gi.metrics == nil {
		return &GlobalIndexMetrics{}
	}
	return gi.metrics
}

// Add metrics tracking to the GlobalIndexRegistry
func (r *GlobalIndexRegistry) GetAllIndices() []*GlobalIndex {
	r.RLock()
	defer r.RUnlock()

	var indices []*GlobalIndex
	seen := make(map[*GlobalIndex]bool)

	for _, tableIndices := range r.indices {
		for _, index := range tableIndices {
			if !seen[index] {
				indices = append(indices, index)
				seen[index] = true
			}
		}
	}

	return indices
}

// GetAllMetrics returns metrics for all global indices
func (r *GlobalIndexRegistry) GetAllMetrics() map[string]*GlobalIndexMetrics {
	metrics := make(map[string]*GlobalIndexMetrics)

	for _, index := range r.GetAllIndices() {
		metrics[index.TableName] = index.GetIndexMetrics()
	}

	return metrics
}
