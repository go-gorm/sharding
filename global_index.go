package sharding

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"

	pg_query "github.com/pganalyze/pg_query_go/v6"
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
		AsyncUpdates:     false, // Use synchronous updates by default for reliability
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
	// Initialize global indices if needed
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

	// Register callbacks to maintain the index
	s.registerIndexCallbacks(gi, s.DB)

	// Register the global index
	for _, col := range columns {
		s.globalIndices.Add(tableName, col, gi)
	}

	// Schedule initial build of the index
	ctx := context.Background()
	if opts.AsyncUpdates {
		go func() {
			if err := gi.RebuildIndex(ctx); err != nil {
				log.Printf("Error during initial index build for table %s: %v", tableName, err)
			}
		}()
	} else {
		if err := gi.RebuildIndex(ctx); err != nil {
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
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return query, args, false
	}

	// Parse the query
	parsed, err := pg_query.Parse(query)
	if err != nil {
		log.Printf("Error parsing query: %v", err)
		return query, args, false
	}

	if len(parsed.Stmts) == 0 {
		return query, args, false
	}

	// Extract the table and where conditions
	stmt := parsed.Stmts[0]
	var tableName string
	var whereNode *pg_query.Node
	var shardKeyCondition *IndexableCondition

	switch node := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		selectStmt := node.SelectStmt
		if len(selectStmt.FromClause) > 0 {
			if rangeVar, ok := selectStmt.FromClause[0].Node.(*pg_query.Node_RangeVar); ok {
				tableName = rangeVar.RangeVar.Relname
			}
		}
		whereNode = selectStmt.WhereClause

	case *pg_query.Node_UpdateStmt:
		updateStmt := node.UpdateStmt
		if updateStmt.Relation != nil {
			tableName = updateStmt.Relation.Relname
		}
		whereNode = updateStmt.WhereClause

	case *pg_query.Node_DeleteStmt:
		deleteStmt := node.DeleteStmt
		if deleteStmt.Relation != nil {
			tableName = deleteStmt.Relation.Relname
		}
		whereNode = deleteStmt.WhereClause

	default:
		return query, args, false
	}

	// Check if table is configured for sharding and has global indices
	config, exists := s.configs[tableName]
	if !exists || s.globalIndices == nil {
		return query, args, false
	}

	// Extract condition columns
	if whereNode == nil {
		return query, args, false
	}

	// Extract all conditions
	conditions := extractConditionsFromNode(whereNode, args)

	// Check if we have the sharding key in our conditions
	shardingKey := config.ShardingKey
	for _, cond := range conditions {
		if cond.Column == shardingKey {
			// If we have the sharding key, we don't need to use the global index
			// as we can route directly to the correct shard
			shardKeyCondition = cond
			break
		}
	}

	// If we have a sharding key condition, don't use the global index
	if shardKeyCondition != nil {
		return query, args, false
	}

	// Find conditions that can use the global index
	var indexableColumns []string
	for _, cond := range conditions {
		if gi := s.globalIndices.Get(tableName, cond.Column); gi != nil {
			indexableColumns = append(indexableColumns, cond.Column)
		}
	}

	if len(indexableColumns) == 0 {
		return query, args, false
	}

	// Find the first column with a global index
	for _, col := range indexableColumns {
		gi := s.globalIndices.Get(tableName, col)
		if gi != nil {
			// Try to find the condition for this column
			var targetCondition *IndexableCondition
			for _, cond := range conditions {
				if cond.Column == col {
					targetCondition = cond
					break
				}
			}

			if targetCondition != nil {
				// Try to rewrite query using the global index
				rewritten, newArgs, err := gi.RewriteQueryWithIndex(query, args, col)
				if err == nil {
					return rewritten, newArgs, true
				}
				log.Printf("Failed to rewrite query with global index: %v", err)
			}
		}
	}

	return query, args, false
}

// Extract conditions from a WHERE clause node
func extractConditionsFromNode(node *pg_query.Node, args []interface{}) []*IndexableCondition {
	if node == nil {
		return nil
	}

	var conditions []*IndexableCondition

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		// Handle simple comparisons (column = value)
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
			if opName == "=" {
				var colName string
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					for _, field := range colRef.ColumnRef.Fields {
						if strNode, ok := field.Node.(*pg_query.Node_String_); ok {
							colName = strNode.String_.Sval
							break
						}
					}
				}

				if colName != "" {
					var value interface{}
					argIndex := -1

					// Extract the value or parameter reference
					if paramRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ParamRef); ok {
						// Parameter like $1, $2
						paramIndex := int(paramRef.ParamRef.Number) - 1
						if paramIndex >= 0 && paramIndex < len(args) {
							argIndex = paramIndex
							value = args[paramIndex]
						}
					} else if constNode, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_AConst); ok {
						// Literal value
						if strVal, ok := constNode.AConst.Val.(*pg_query.A_Const_Sval); ok {
							value = strVal.Sval.Sval
						} else if intVal, ok := constNode.AConst.Val.(*pg_query.A_Const_Ival); ok {
							value = intVal.Ival.Ival
						}
					}

					if value != nil || argIndex >= 0 {
						conditions = append(conditions, &IndexableCondition{
							Column:   colName,
							Operator: opName,
							Value:    value,
							ArgIndex: argIndex,
						})
					}
				}
			}
		}
	case *pg_query.Node_BoolExpr:
		// For AND/OR expressions, recursively process each argument
		for _, arg := range n.BoolExpr.Args {
			conditions = append(conditions, extractConditionsFromNode(arg, args)...)
		}
	}

	return conditions
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
	// Store the original query for debugging
	cp.sharding.querys.Store("last_query", query)

	// Skip system queries and explicitly marked queries
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return cp.originalPool.QueryContext(ctx, query, args...)
	}

	// Use the query rewriter to optimize the query with global index
	if cp.sharding.queryRewriter != nil {
		newQuery, newArgs, useGlobalIndex := cp.sharding.queryRewriter.RewriteQuery(query, args)
		if useGlobalIndex {
			// Store the rewritten query for debugging/testing
			cp.sharding.querys.Store("last_query", newQuery)
			return cp.originalPool.QueryContext(ctx, newQuery, newArgs...)
		}
	}

	// If no global index optimization, fall back to normal sharding
	_, stQuery, _, err := cp.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	// Store the resolved query
	cp.sharding.querys.Store("last_query", stQuery)

	return cp.originalPool.QueryContext(ctx, stQuery, args...)
}

// ExecContext intercepts exec calls and uses global index if applicable
func (cp *ConnPoolWithGlobalIndex) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	// Store the original query for debugging
	cp.sharding.querys.Store("last_query", query)

	// Skip system queries and explicitly marked queries
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return cp.originalPool.ExecContext(ctx, query, args...)
	}

	// Use the query rewriter to optimize the query with global index
	if cp.sharding.queryRewriter != nil {
		newQuery, newArgs, useGlobalIndex := cp.sharding.queryRewriter.RewriteQuery(query, args)
		if useGlobalIndex {
			// Store the rewritten query for debugging/testing
			cp.sharding.querys.Store("last_query", newQuery)
			return cp.originalPool.ExecContext(ctx, newQuery, newArgs...)
		}
	}

	// If no global index optimization, fall back to normal sharding
	_, stQuery, _, err := cp.sharding.resolve(query, args...)
	if err != nil {
		return nil, err
	}

	// Store the resolved query
	cp.sharding.querys.Store("last_query", stQuery)

	return cp.originalPool.ExecContext(ctx, stQuery, args...)
}

// QueryRowContext intercepts query row calls and uses global index if applicable
func (cp *ConnPoolWithGlobalIndex) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	// Store the original query for debugging
	cp.sharding.querys.Store("last_query", query)

	// Skip system queries and explicitly marked queries
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return cp.originalPool.QueryRowContext(ctx, query, args...)
	}

	// Use the query rewriter to optimize the query with global index
	if cp.sharding.queryRewriter != nil {
		newQuery, newArgs, useGlobalIndex := cp.sharding.queryRewriter.RewriteQuery(query, args)
		if useGlobalIndex {
			// Store the rewritten query for debugging/testing
			cp.sharding.querys.Store("last_query", newQuery)
			return cp.originalPool.QueryRowContext(ctx, newQuery, newArgs...)
		}
	}

	// If no global index optimization, fall back to normal sharding
	_, stQuery, _, _ := cp.sharding.resolve(query, args...)

	// Store the resolved query
	cp.sharding.querys.Store("last_query", stQuery)

	return cp.originalPool.QueryRowContext(ctx, stQuery, args...)
}

// PrepareContext implements the ConnPool interface
func (cp *ConnPoolWithGlobalIndex) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return cp.originalPool.PrepareContext(ctx, query)
}

// RewriteQueryWithIndex rewrites a query to use the global index for efficient lookup
func (gi *GlobalIndex) RewriteQueryWithIndex(query string, args []interface{}, indexColumn string) (string, []interface{}, error) {
	log.Printf("Rewriting query to use global index for table %s, column %s: %s",
		gi.TableName, indexColumn, query)

	// First, get the records from the global index
	var indexRecords []GlobalIndexRecord

	// Extract the value we're searching for
	valueStr, paramIdx, found := extractValueForIndexColumn(query, args, indexColumn)
	if !found {
		return query, args, fmt.Errorf("could not extract value for column %s", indexColumn)
	}

	// If we found a parameter reference, get the actual value
	if paramIdx >= 0 {
		valueStr = fmt.Sprintf("%v", args[paramIdx])
	}

	// Query the global index for matching records
	err := gi.DB.Where("index_column = ? AND index_value = ?", indexColumn, valueStr).Find(&indexRecords).Error
	if err != nil {
		return query, args, fmt.Errorf("error querying global index: %w", err)
	}

	if len(indexRecords) == 0 {
		// No matching records found, return a query that will yield no results
		return fmt.Sprintf("SELECT * FROM %s WHERE 1=0 /* via_global_index_empty */", gi.TableName), args, nil
	}

	// Group records by shard
	suffixToIDs := make(map[string][]int64)
	for _, record := range indexRecords {
		suffixToIDs[record.TableSuffix] = append(suffixToIDs[record.TableSuffix], record.RecordID)
	}

	// Create new args for the rewritten query
	newArgs := make([]interface{}, len(args))
	copy(newArgs, args)

	// Parse the original query to help with rewriting
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return query, args, fmt.Errorf("error parsing query: %w", err)
	}

	if len(parsed.Stmts) == 0 {
		return query, args, fmt.Errorf("no statements found in query")
	}

	stmt := parsed.Stmts[0]
	var isSelectQuery bool

	// Different handling based on query type
	switch stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		isSelectQuery = true
	}

	// For each shard, create a query
	var subQueries []string

	for suffix, ids := range suffixToIDs {
		shardTableName := gi.TableName + suffix

		// Replace the table name in the query
		shardQuery := strings.Replace(query, gi.TableName, shardTableName, -1)

		// Create placeholders for the IDs
		idPlaceholders := make([]string, len(ids))
		for i, id := range ids {
			paramIndex := len(newArgs) + 1
			idPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
			newArgs = append(newArgs, id)
		}

		// Add the ID filter to the WHERE clause
		idFilter := fmt.Sprintf(" id IN (%s)", strings.Join(idPlaceholders, ", "))
		wherePos := strings.Index(strings.ToUpper(shardQuery), "WHERE")
		if wherePos >= 0 {
			// Add to existing WHERE clause
			wherePos += 5 // Skip past "WHERE"
			shardQuery = shardQuery[:wherePos] + idFilter + " AND " + shardQuery[wherePos:]
		} else {
			// Add new WHERE clause
			// Find appropriate insertion point
			fromPos := strings.Index(strings.ToUpper(shardQuery), "FROM")
			if fromPos < 0 {
				// Should never happen for valid SQL
				return query, args, fmt.Errorf("malformed query - cannot find FROM clause")
			}

			// Look for possible insertion point after FROM clause
			fromPos += 4 // Skip past "FROM"
			orderPos := strings.Index(strings.ToUpper(shardQuery), "ORDER BY")
			groupPos := strings.Index(strings.ToUpper(shardQuery), "GROUP BY")
			limitPos := strings.Index(strings.ToUpper(shardQuery), "LIMIT")

			insertPos := len(shardQuery)
			for _, pos := range []int{orderPos, groupPos, limitPos} {
				if pos > 0 && pos < insertPos {
					insertPos = pos
				}
			}

			shardQuery = shardQuery[:insertPos] + " WHERE " + idFilter + " " + shardQuery[insertPos:]
		}

		subQueries = append(subQueries, shardQuery)
	}

	// Handle special case for a single shard (no need for UNION)
	if len(subQueries) == 1 {
		return subQueries[0] + " /* via_global_index */", newArgs, nil
	}

	// Combine queries with UNION ALL
	if isSelectQuery {
		// Check if we need to wrap the UNION query for SELECT statements
		// This is needed for queries with ORDER BY, LIMIT, etc.
		orderByPos := strings.Index(strings.ToUpper(query), "ORDER BY")
		limitPos := strings.Index(strings.ToUpper(query), "LIMIT")
		groupByPos := strings.Index(strings.ToUpper(query), "GROUP BY")
		havingPos := strings.Index(strings.ToUpper(query), "HAVING")

		// If the query has clauses that should be applied after UNION, wrap it
		if orderByPos > 0 || limitPos > 0 || groupByPos > 0 || havingPos > 0 {
			// Extract clauses
			var clauses string
			earliestPos := len(query)

			for _, pos := range []int{orderByPos, limitPos, groupByPos, havingPos} {
				if pos > 0 && pos < earliestPos {
					earliestPos = pos
				}
			}

			if earliestPos < len(query) {
				clauses = query[earliestPos:]
				// Remove these clauses from the sub-queries
				for i := range subQueries {
					subQueries[i] = strings.Split(subQueries[i], clauses)[0]
				}

				// Combine with UNION ALL and wrap with the clauses
				finalQuery := fmt.Sprintf("SELECT * FROM (%s) AS combined_result %s /* via_global_index */",
					strings.Join(subQueries, " UNION ALL "), clauses)
				return finalQuery, newArgs, nil
			}
		}
	}

	// Default case: just combine with UNION ALL
	finalQuery := strings.Join(subQueries, " UNION ALL ") + " /* via_global_index */"
	return finalQuery, newArgs, nil
}

// Helper function to extract a value for an index column from a WHERE clause
func extractValueForIndexColumn(query string, args []interface{}, indexColumn string) (string, int, bool) {
	// First, try to find a parameter for this column
	re := regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*\$(\d+)`, regexp.QuoteMeta(indexColumn)))
	matches := re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		paramIdx, _ := strconv.Atoi(matches[1])
		if paramIdx > 0 && paramIdx <= len(args) {
			return "", paramIdx - 1, true
		}
	}

	// Try to find a literal value for this column
	re = regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*'([^']*)'`, regexp.QuoteMeta(indexColumn)))
	matches = re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return matches[1], -1, true
	}

	// Try to find a numeric literal
	re = regexp.MustCompile(fmt.Sprintf(`%s\s*=\s*(\d+)`, regexp.QuoteMeta(indexColumn)))
	matches = re.FindStringSubmatch(query)
	if len(matches) >= 2 {
		return matches[1], -1, true
	}

	return "", -1, false
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
