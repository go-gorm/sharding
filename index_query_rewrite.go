package sharding

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// QueryRewriteOptions configures how queries are rewritten to use global indices
type QueryRewriteOptions struct {
	// EnableCost enables cost-based decisions on whether to use the global index
	EnableCost bool

	// CostThreshold is the maximum query cost to use global index (0-1, where lower means more likely to use index)
	CostThreshold float64

	// IndexSelectivity is the expected index selectivity (0-1, where lower means more selective index)
	IndexSelectivity float64

	// EnableQueryCache enables caching of rewritten queries
	EnableQueryCache bool

	// QueryCacheSize is the maximum number of queries to cache
	QueryCacheSize int

	// EnableParallelShardQueries enables parallel execution when querying multiple shards
	EnableParallelShardQueries bool

	// InjectShardingKey causes rewrites to include the sharding key in the WHERE clause
	InjectShardingKey bool
}

// DefaultQueryRewriteOptions returns sensible defaults
func DefaultQueryRewriteOptions() QueryRewriteOptions {
	return QueryRewriteOptions{
		EnableCost:                 true,
		CostThreshold:              0.25,
		IndexSelectivity:           0.1,
		EnableQueryCache:           true,
		QueryCacheSize:             1000,
		EnableParallelShardQueries: false,
		InjectShardingKey:          true,
	}
}

// IndexQueryCacheEntry represents a cached query rewrite
type IndexQueryCacheEntry struct {
	OriginalQuery  string
	RewrittenQuery string
	Args           []interface{}
	NewArgs        []interface{}
	HitCount       int
	LastUsed       time.Time
}

// IndexQueryCache caches rewritten queries
type IndexQueryCache struct {
	entries    map[string]*IndexQueryCacheEntry
	maxEntries int
}

// NewIndexQueryCache creates a new query cache
func NewIndexQueryCache(maxEntries int) *IndexQueryCache {
	return &IndexQueryCache{
		entries:    make(map[string]*IndexQueryCacheEntry),
		maxEntries: maxEntries,
	}
}

// Get retrieves a cached query
func (c *IndexQueryCache) Get(query string, args []interface{}) (*IndexQueryCacheEntry, bool) {
	key := c.makeKey(query, args)
	entry, ok := c.entries[key]
	if ok {
		entry.HitCount++
		entry.LastUsed = time.Now()
	}
	return entry, ok
}

// Put adds a query to the cache
func (c *IndexQueryCache) Put(query string, args []interface{}, rewrittenQuery string, newArgs []interface{}) {
	// Check if cache is full
	if len(c.entries) >= c.maxEntries {
		c.evictLRU()
	}

	key := c.makeKey(query, args)
	c.entries[key] = &IndexQueryCacheEntry{
		OriginalQuery:  query,
		RewrittenQuery: rewrittenQuery,
		Args:           args,
		NewArgs:        newArgs,
		HitCount:       1,
		LastUsed:       time.Now(),
	}
}

// makeKey creates a cache key from a query and args
func (c *IndexQueryCache) makeKey(query string, args []interface{}) string {
	// Simplify the query by removing whitespace
	query = strings.Join(strings.Fields(query), " ")

	// Add a hash of the arguments
	argsStr := make([]string, len(args))
	for i, arg := range args {
		argsStr[i] = fmt.Sprintf("%v", arg)
	}

	return query + "|" + strings.Join(argsStr, ",")
}

// evictLRU evicts the least recently used entry
func (c *IndexQueryCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	// Find the oldest entry
	for key, entry := range c.entries {
		if oldestKey == "" || entry.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastUsed
		}
	}

	// Evict the oldest
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// QueryRewriter handles global index-based query rewriting
type QueryRewriter struct {
	sharding *Sharding
	options  QueryRewriteOptions
	cache    *IndexQueryCache
}

// NewQueryRewriter creates a new query rewriter
func NewQueryRewriter(s *Sharding, options ...QueryRewriteOptions) *QueryRewriter {
	var opts QueryRewriteOptions
	if len(options) > 0 {
		opts = options[0]
	} else {
		opts = DefaultQueryRewriteOptions()
	}

	var cache *IndexQueryCache
	if opts.EnableQueryCache {
		cache = NewIndexQueryCache(opts.QueryCacheSize)
	}

	return &QueryRewriter{
		sharding: s,
		options:  opts,
		cache:    cache,
	}
}

// RewriteQuery rewrites a query to use the global index
func (qr *QueryRewriter) RewriteQuery(query string, args []interface{}) (string, []interface{}, bool) {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		if elapsed > 5*time.Millisecond {
			log.Printf("Query rewrite took %v: %s", elapsed, query)
		}
	}()

	// Skip system queries and queries explicitly marked to ignore global index
	if isSystemQuery(query) || strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return query, args, false
	}

	// Check the cache first if enabled
	if qr.options.EnableQueryCache && qr.cache != nil {
		if entry, ok := qr.cache.Get(query, args); ok {
			return entry.RewrittenQuery, entry.NewArgs, true
		}
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
	var conditions []*pg_query.Node
	var isSelectQuery bool

	switch node := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		isSelectQuery = true
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
	tableConfig, exists := qr.sharding.configs[tableName]
	if !exists || tableName == "" {
		return query, args, false
	}

	// Extract indexable columns and their values from the conditions
	indexableConditions := qr.extractIndexableConditions(tableName, conditions, args)
	if len(indexableConditions) == 0 {
		return query, args, false
	}

	// Determine which condition is most selective
	mostSelectiveCondition := qr.findMostSelectiveCondition(tableName, indexableConditions)
	if mostSelectiveCondition == nil {
		return query, args, false
	}

	// Use cost-based decision if enabled
	if qr.options.EnableCost && isSelectQuery {
		// Estimate cost of using global index vs. original query
		indexCost := qr.estimateGlobalIndexCost(tableName, mostSelectiveCondition)
		originalCost := qr.estimateOriginalQueryCost(tableName, mostSelectiveCondition, tableConfig)

		// Only use index if it's cheaper by the threshold
		if indexCost > qr.options.CostThreshold*originalCost {
			log.Printf("Skipping index for query due to cost: index=%.2f, original=%.2f", indexCost, originalCost)
			return query, args, false
		}
	}

	// Get records from global index
	col := mostSelectiveCondition.Column
	val := mostSelectiveCondition.Value
	gi := qr.sharding.globalIndices.Get(tableName, col)
	if gi == nil {
		// This should never happen as we already filtered for indexable conditions
		return query, args, false
	}

	// Look up the records in the global index
	valueStr := fmt.Sprintf("%v", val)
	var indexRecords []GlobalIndexRecord

	err = gi.DB.Where("index_column = ? AND index_value = ?", col, valueStr).Find(&indexRecords).Error
	if err != nil {
		log.Printf("Error querying global index: %v", err)
		return query, args, false
	}

	if len(indexRecords) == 0 {
		// No matching records found, return a query that will yield no results
		emptyQuery := fmt.Sprintf("SELECT * FROM %s WHERE 1=0 /* via_global_index_empty */", tableName)

		// Cache the result if enabled
		if qr.options.EnableQueryCache && qr.cache != nil {
			qr.cache.Put(query, args, emptyQuery, args)
		}

		return emptyQuery, args, true
	}

	// Group records by shard suffix
	suffixToIDs := make(map[string][]int64)
	for _, record := range indexRecords {
		suffixToIDs[record.TableSuffix] = append(suffixToIDs[record.TableSuffix], record.RecordID)
	}

	// Build the rewritten query
	rewrittenQuery, newArgs := qr.buildRewrittenQuery(query, tableName, suffixToIDs, args, tableConfig, mostSelectiveCondition)

	// Cache the result if enabled
	if qr.options.EnableQueryCache && qr.cache != nil {
		qr.cache.Put(query, args, rewrittenQuery, newArgs)
	}

	return rewrittenQuery, newArgs, true
}

// IndexableCondition represents a condition that can be used with a global index
type IndexableCondition struct {
	Column    string
	Operator  string
	Value     interface{}
	ArgIndex  int     // -1 if the value is a literal
	Selective float64 // 0-1, lower is more selective
}

// extractIndexableConditions extracts conditions that can use global indices
func (qr *QueryRewriter) extractIndexableConditions(tableName string, conditions []*pg_query.Node, args []interface{}) []*IndexableCondition {
	var indexableConditions []*IndexableCondition

	// Process all conditions
	for _, condNode := range conditions {
		conditions := extractIndexableConditionsFromNode(condNode)

		for _, cond := range conditions {
			// Check if this column has a global index
			if gi := qr.sharding.globalIndices.Get(tableName, cond.Column); gi != nil {
				// For parameter references, get the actual value
				if cond.ArgIndex >= 0 && cond.ArgIndex < len(args) {
					cond.Value = args[cond.ArgIndex]
				}

				// Skip conditions with nil values
				if cond.Value == nil {
					continue
				}

				// Estimate selectivity
				cond.Selective = qr.estimateConditionSelectivity(tableName, cond)

				indexableConditions = append(indexableConditions, cond)
			}
		}
	}

	return indexableConditions
}

// extractIndexableConditionsFromNode recursively extracts equality conditions from a node
func extractIndexableConditionsFromNode(node *pg_query.Node) []*IndexableCondition {
	var conditions []*IndexableCondition

	if node == nil {
		return conditions
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		// Handle simple equality expressions like "column = value"
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval

			// We only support equality for now
			if opName == "=" {
				// Extract column name from left side
				var colName string
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					// Extract column name from reference
					if len(colRef.ColumnRef.Fields) > 0 {
						if strField, ok := colRef.ColumnRef.Fields[len(colRef.ColumnRef.Fields)-1].Node.(*pg_query.Node_String_); ok {
							colName = strField.String_.Sval
						}
					}
				}

				// Skip if column name not found
				if colName == "" {
					return conditions
				}

				// Extract value from right side
				var value interface{}
				argIndex := -1

				if paramRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ParamRef); ok {
					// This is a parameter reference ($1, $2, etc.)
					argIndex = int(paramRef.ParamRef.Number) - 1
				} else if aConst, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_AConst); ok {
					// This is a constant value
					if strVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Sval); ok {
						value = strVal.Sval.Sval
					} else if intVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Ival); ok {
						value = int64(intVal.Ival.Ival)
					} else if floatVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Fval); ok {
						val, _ := strconv.ParseFloat(floatVal.Fval.Fval, 64)
						value = val
					} else if boolVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Boolval); ok {
						value = boolVal.Boolval.Boolval
					}
				}

				// Add condition if we found a value or parameter reference
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

	case *pg_query.Node_BoolExpr:
		// For boolean expressions (AND, OR), process each argument
		for _, arg := range n.BoolExpr.Args {
			conditions = append(conditions, extractIndexableConditionsFromNode(arg)...)
		}
	}

	return conditions
}

// findMostSelectiveCondition finds the condition that will return the fewest results
func (qr *QueryRewriter) findMostSelectiveCondition(tableName string, conditions []*IndexableCondition) *IndexableCondition {
	if len(conditions) == 0 {
		return nil
	}

	// Find the most selective condition (lowest selectivity value)
	var mostSelective *IndexableCondition
	for _, cond := range conditions {
		if mostSelective == nil || cond.Selective < mostSelective.Selective {
			mostSelective = cond
		}
	}

	return mostSelective
}

// estimateConditionSelectivity estimates how selective a condition will be
func (qr *QueryRewriter) estimateConditionSelectivity(tableName string, cond *IndexableCondition) float64 {
	// If we have statistics, use them for better estimates
	gi := qr.sharding.globalIndices.Get(tableName, cond.Column)
	if gi == nil {
		return 0.5 // Default selectivity if no index
	}

	// Query the global index to get an actual count
	valueStr := fmt.Sprintf("%v", cond.Value)
	var count int64
	err := gi.DB.Model(&GlobalIndexRecord{}).
		Where("index_column = ? AND index_value = ?", cond.Column, valueStr).
		Count(&count).Error

	if err != nil {
		log.Printf("Error estimating selectivity: %v", err)
		return qr.options.IndexSelectivity // Use default
	}

	// Get total record count
	var totalCount int64
	err = gi.DB.Model(&GlobalIndexRecord{}).
		Where("index_column = ?", cond.Column).
		Count(&totalCount).Error

	if err != nil || totalCount == 0 {
		return qr.options.IndexSelectivity
	}

	// Calculate selectivity
	selectivity := float64(count) / float64(totalCount)

	// Ensure selectivity is between 0 and 1
	if selectivity < 0.001 {
		selectivity = 0.001 // Minimum selectivity
	} else if selectivity > 1.0 {
		selectivity = 1.0
	}

	return selectivity
}

// estimateGlobalIndexCost estimates the relative cost of using the global index
func (qr *QueryRewriter) estimateGlobalIndexCost(tableName string, cond *IndexableCondition) float64 {
	// Simple cost model: cost is proportional to expected number of results
	selectivity := cond.Selective

	// Higher cost for accessing multiple shards
	gi := qr.sharding.globalIndices.Get(tableName, cond.Column)
	if gi == nil {
		return 1.0 // Maximum cost if no index
	}

	// Find how many distinct shards will be accessed
	valueStr := fmt.Sprintf("%v", cond.Value)
	var distinctShards int64
	gi.DB.Model(&GlobalIndexRecord{}).
		Where("index_column = ? AND index_value = ?", cond.Column, valueStr).
		Distinct("table_suffix").
		Count(&distinctShards)

	// Cost increases with number of shards that need to be accessed
	shardFactor := float64(distinctShards) / 5.0 // Normalize to make 5 shards = 1.0
	if shardFactor > 1.0 {
		shardFactor = 1.0
	}

	// Final cost formula
	cost := selectivity * (0.5 + 0.5*shardFactor)
	return cost
}

// estimateOriginalQueryCost estimates the cost of the original cross-shard query
func (qr *QueryRewriter) estimateOriginalQueryCost(tableName string, cond *IndexableCondition, config Config) float64 {
	// Cost of original query is proportional to number of shards
	shardCount := float64(config.NumberOfShards)

	// Normalize with sharding key vs non-sharding key
	// If the condition is on the sharding key, cost is much lower
	isShardingKey := (cond.Column == config.ShardingKey)

	if isShardingKey {
		return 0.1 // Very low cost for sharding key queries
	}

	// For non-sharding key, cost is proportional to number of shards
	return math.Min(1.0, shardCount/10.0) // Cap at 1.0 for 10+ shards
}

// buildRewrittenQuery creates a new query using the global index lookup results
func (qr *QueryRewriter) buildRewrittenQuery(query, tableName string, suffixToIDs map[string][]int64, args []interface{}, config Config, condition *IndexableCondition) (string, []interface{}) {
	newArgs := make([]interface{}, len(args))
	copy(newArgs, args)

	// For a single shard, just rewrite the query
	if len(suffixToIDs) == 1 {
		for suffix, ids := range suffixToIDs {
			shardTableName := tableName + suffix

			// Replace the table name with the sharded table
			shardQuery := strings.Replace(query, tableName, shardTableName, -1)

			// Add condition to filter by IDs from the global index
			idPlaceholders := make([]string, len(ids))
			for i, id := range ids {
				paramIndex := len(newArgs) + 1
				idPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
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
				// Find appropriate insertion point
				fromPos := strings.Index(strings.ToUpper(shardQuery), "FROM")
				if fromPos < 0 {
					// Unable to find FROM - shouldn't happen for valid SQL
					return query, args
				}

				// Find first clause after FROM
				clauseKeywords := []string{"GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "FOR UPDATE"}
				insertPos := len(shardQuery)

				for _, keyword := range clauseKeywords {
					pos := strings.Index(strings.ToUpper(shardQuery), keyword)
					if pos >= 0 && pos < insertPos {
						insertPos = pos
					}
				}

				shardQuery = shardQuery[:insertPos] + " WHERE " + idFilter + " " + shardQuery[insertPos:]
			}

			// Add comment to indicate the query was rewritten
			shardQuery += " /* via_global_index */"

			return shardQuery, newArgs
		}
	}

	// For multiple shards, build a UNION ALL query
	var subQueries []string

	// For each shard, build a query with the appropriate ID filter
	for suffix, ids := range suffixToIDs {
		shardTableName := tableName + suffix

		// Replace the table name with the sharded table
		shardQuery := strings.Replace(query, tableName, shardTableName, -1)

		// Add condition to filter by IDs from the global index
		idPlaceholders := make([]string, len(ids))
		for i, id := range ids {
			paramIndex := len(newArgs) + 1
			idPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
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
			// Find appropriate insertion point
			fromPos := strings.Index(strings.ToUpper(shardQuery), "FROM")
			if fromPos < 0 {
				// Unable to find FROM - shouldn't happen for valid SQL
				continue
			}

			// Find first clause after FROM
			clauseKeywords := []string{"GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "FOR UPDATE"}
			insertPos := len(shardQuery)

			for _, keyword := range clauseKeywords {
				pos := strings.Index(strings.ToUpper(shardQuery), keyword)
				if pos >= 0 && pos < insertPos {
					insertPos = pos
				}
			}

			shardQuery = shardQuery[:insertPos] + " WHERE " + idFilter + " " + shardQuery[insertPos:]
		}

		subQueries = append(subQueries, shardQuery)
	}

	// Handle the case where we couldn't build any sub-queries
	if len(subQueries) == 0 {
		return query, args
	}

	// Combine sub-queries with UNION ALL
	combinedQuery := strings.Join(subQueries, " UNION ALL ")

	// Handle ORDER BY, LIMIT, and OFFSET clauses
	// These need to be applied to the combined result
	clauseKeywords := []string{"ORDER BY", "LIMIT", "OFFSET"}
	var clauseParts []string

	for _, keyword := range clauseKeywords {
		pos := strings.Index(strings.ToUpper(query), keyword)
		if pos >= 0 {
			// Extract the clause
			nextClausePos := len(query)
			for _, nextKeyword := range clauseKeywords {
				nextPos := strings.Index(strings.ToUpper(query[pos+len(keyword):]), nextKeyword)
				if nextPos >= 0 && pos+len(keyword)+nextPos < nextClausePos {
					nextClausePos = pos + len(keyword) + nextPos
				}
			}

			clause := query[pos:nextClausePos]
			clauseParts = append(clauseParts, clause)
		}
	}

	// If we have any clauses to add to the combined result
	if len(clauseParts) > 0 {
		// Wrap the UNION query in a subquery
		finalQuery := fmt.Sprintf("SELECT * FROM (%s) AS combined_results %s",
			combinedQuery, strings.Join(clauseParts, " "))
		return finalQuery + " /* via_global_index */", newArgs
	}

	return combinedQuery + " /* via_global_index */", newArgs
}

// GetQueryCache returns the query cache for inspection
func (qr *QueryRewriter) GetQueryCache() *IndexQueryCache {
	return qr.cache
}

// ClearQueryCache clears the query cache
func (qr *QueryRewriter) ClearQueryCache() {
	if qr.cache != nil {
		qr.cache.entries = make(map[string]*IndexQueryCacheEntry)
	}
}
