package sharding

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"log"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// GlobalIndexConnector handles the integration between global index and the main sharding module
type GlobalIndexConnector struct {
	sharding *Sharding
}

// NewGlobalIndexConnector creates a new connector
func NewGlobalIndexConnector(s *Sharding) *GlobalIndexConnector {
	return &GlobalIndexConnector{
		sharding: s,
	}
}

// ConnPoolWithGlobalIndex extends ConnPool with global index capabilities
type ConnPoolWithGlobalIndex struct {
	connPool  gorm.ConnPool
	connector *GlobalIndexConnector
}

func (cp *ConnPoolWithGlobalIndex) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	// Check if this query can use global index
	newQuery, newArgs, useGlobalIndex := cp.connector.processQuery(query, args)
	if useGlobalIndex {
		return cp.connPool.QueryContext(ctx, newQuery, newArgs...)
	}
	return cp.connPool.QueryContext(ctx, query, args...)
}

// ExecContext overrides the ConnPool ExecContext method to use global indices
func (cp *ConnPoolWithGlobalIndex) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	// Check if this query can use global index
	newQuery, newArgs, useGlobalIndex := cp.connector.processQuery(query, args)
	if useGlobalIndex {
		return cp.connPool.ExecContext(ctx, newQuery, newArgs...)
	}
	return cp.connPool.ExecContext(ctx, query, args...)
}

// QueryRowContext overrides the ConnPool QueryRowContext method to use global indices
func (cp *ConnPoolWithGlobalIndex) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	// Check if this query can use global index
	newQuery, newArgs, useGlobalIndex := cp.connector.processQuery(query, args)
	if useGlobalIndex {
		return cp.connPool.QueryRowContext(ctx, newQuery, newArgs...)
	}
	return cp.connPool.QueryRowContext(ctx, query, args...)
}

// PrepareContext passes through to the original ConnPool
func (cp *ConnPoolWithGlobalIndex) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return cp.connPool.PrepareContext(ctx, query)
}

// processQuery checks if a query can use the global index and modifies it if needed
func (gic *GlobalIndexConnector) processQuery(query string, args []interface{}) (string, []interface{}, bool) {
	// Skip if sharding ignore flag is set
	if strings.Contains(query, "/* nosharding */") || strings.Contains(query, "/* noglobalindex */") {
		return query, args, false
	}

	// Skip modification for system queries or DDL statements
	if isSystemQuery(query) || isDefinitionQuery(query) {
		return query, args, false
	}

	// Try to extract table and conditions
	tableName, conditions, err := gic.extractQueryInfo(query, args)
	if err != nil || tableName == "" {
		return query, args, false
	}

	// Check if any condition can use a global index
	for _, cond := range conditions {
		if index := gic.findGlobalIndexForColumn(tableName, cond.Column); index != nil {
			// Found a global index we can use
			newQuery, newArgs, err := gic.rewriteQueryWithGlobalIndex(index, query, cond, args)
			if err == nil {
				return newQuery, newArgs, true
			}
			log.Printf("Error rewriting query with global index: %v", err)
		}
	}

	return query, args, false
}

// QueryCondition represents a condition in a SQL query
type QueryCondition struct {
	Column string
	Op     string
	Value  interface{}
}

// isDefinitionQuery checks if this is a DDL/definition query
func isDefinitionQuery(query string) bool {
	upperQuery := strings.ToUpper(query)
	ddlStatements := []string{
		"CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "RENAME ", "COMMENT ", "GRANT ", "REVOKE ",
	}

	for _, stmt := range ddlStatements {
		if strings.HasPrefix(upperQuery, stmt) {
			return true
		}
	}

	return false
}

// extractQueryInfo extracts table name and conditions from a query
func (gic *GlobalIndexConnector) extractQueryInfo(query string, args []interface{}) (string, []QueryCondition, error) {
	var tableName string
	var conditions []QueryCondition

	// Parse the query
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return "", nil, err
	}

	if len(parsed.Stmts) == 0 {
		return "", nil, fmt.Errorf("no statements found")
	}

	stmt := parsed.Stmts[0]
	switch node := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		selectStmt := node.SelectStmt

		// Extract table name from FROM clause
		if len(selectStmt.FromClause) > 0 {
			if rangeVar, ok := selectStmt.FromClause[0].Node.(*pg_query.Node_RangeVar); ok {
				tableName = rangeVar.RangeVar.Relname
			}
		}

		// Extract conditions from WHERE clause
		if selectStmt.WhereClause != nil {
			conditions = gic.extractConditions(selectStmt.WhereClause, args)
		}

	case *pg_query.Node_UpdateStmt:
		updateStmt := node.UpdateStmt

		// Extract table name
		if updateStmt.Relation != nil {
			tableName = updateStmt.Relation.Relname
		}

		// Extract conditions from WHERE clause
		if updateStmt.WhereClause != nil {
			conditions = gic.extractConditions(updateStmt.WhereClause, args)
		}

	case *pg_query.Node_DeleteStmt:
		deleteStmt := node.DeleteStmt

		// Extract table name
		if deleteStmt.Relation != nil {
			tableName = deleteStmt.Relation.Relname
		}

		// Extract conditions from WHERE clause
		if deleteStmt.WhereClause != nil {
			conditions = gic.extractConditions(deleteStmt.WhereClause, args)
		}
	}

	// Check if this table is configured for sharding
	if _, exists := gic.sharding.configs[tableName]; !exists {
		return "", nil, fmt.Errorf("table %s is not configured for sharding", tableName)
	}

	return tableName, conditions, nil
}

// extractConditions extracts conditions from a WHERE clause
func (gic *GlobalIndexConnector) extractConditions(whereClause *pg_query.Node, args []interface{}) []QueryCondition {
	var conditions []QueryCondition

	switch n := whereClause.Node.(type) {
	case *pg_query.Node_BoolExpr:
		// Handle AND conditions
		if n.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
			for _, arg := range n.BoolExpr.Args {
				conditions = append(conditions, gic.extractConditions(arg, args)...)
			}
		}

	case *pg_query.Node_AExpr:
		// Handle simple comparisons like "column = value"
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval

			// We currently only support equality
			if opName == "=" {
				// Extract column name from left side
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					column := gic.extractColumnName(colRef.ColumnRef)

					// Extract value from right side
					var value interface{}
					paramValue := false

					if paramRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ParamRef); ok {
						// This is a parameter reference ($1, $2, etc.)
						paramIndex := int(paramRef.ParamRef.Number) - 1
						if paramIndex >= 0 && paramIndex < len(args) {
							value = args[paramIndex]
							paramValue = true
						}
					} else if aConst, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_AConst); ok {
						// This is a constant value
						if strVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Sval); ok {
							value = strVal.Sval.Sval
						} else if intVal, ok := aConst.AConst.Val.(*pg_query.A_Const_Ival); ok {
							value = intVal.Ival.Ival
						}
					}

					if column != "" && (value != nil || paramValue) {
						conditions = append(conditions, QueryCondition{
							Column: column,
							Op:     opName,
							Value:  value,
						})
					}
				}
			}
		}
	}

	return conditions
}

// extractColumnName extracts a column name from a ColumnRef
func (gic *GlobalIndexConnector) extractColumnName(colRef *pg_query.ColumnRef) string {
	if len(colRef.Fields) == 0 {
		return ""
	}

	// For a simple column, it's just the field value
	if len(colRef.Fields) == 1 {
		if strNode, ok := colRef.Fields[0].Node.(*pg_query.Node_String_); ok {
			return strNode.String_.Sval
		}
		return ""
	}

	// For a qualified column (table.column), take the last part
	if strNode, ok := colRef.Fields[len(colRef.Fields)-1].Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}

	return ""
}

// findGlobalIndexForColumn finds a global index for a column
func (gic *GlobalIndexConnector) findGlobalIndexForColumn(tableName, columnName string) *GlobalIndex {
	if gic.sharding.globalIndices == nil {
		return nil
	}

	return gic.sharding.globalIndices.Get(tableName, columnName)
}

// rewriteQueryWithGlobalIndex rewrites a query to use the global index
func (gic *GlobalIndexConnector) rewriteQueryWithGlobalIndex(gi *GlobalIndex, query string, condition QueryCondition, args []interface{}) (string, []interface{}, error) {
	// Convert the value to a string representation for the global index lookup
	var valueStr string
	if condition.Value != nil {
		valueStr = fmt.Sprintf("%v", condition.Value)
	} else {
		// Use placeholder value from args
		// This assumes the condition uses a placeholder and we can find the value in args
		found := false
		re := regexp.MustCompile(condition.Column + `\s*=\s*\$(\d+)`)
		matches := re.FindStringSubmatch(query)
		if len(matches) >= 2 {
			paramIdx, _ := fmt.Sscanf(matches[1], "%d")
			if paramIdx > 0 && paramIdx <= len(args) {
				valueStr = fmt.Sprintf("%v", args[paramIdx-1])
				found = true
			}
		}

		if !found {
			return query, args, fmt.Errorf("unable to find value for condition %s", condition.Column)
		}
	}

	// Find matching records in the global index
	var indexRecords []GlobalIndexRecord
	if err := gi.DB.Where("index_column = ? AND index_value = ?", condition.Column, valueStr).Find(&indexRecords).Error; err != nil {
		return query, args, err
	}

	if len(indexRecords) == 0 {
		// No matching records, return a query that will give empty results
		return "SELECT * FROM " + gi.TableName + " WHERE 1=0 /* noglobalindex */", args, nil
	}

	// Group records by shard
	recordsByShards := make(map[string][]int64)
	for _, record := range indexRecords {
		recordsByShards[record.TableSuffix] = append(recordsByShards[record.TableSuffix], record.RecordID)
	}

	// Build UNION ALL query for each shard
	var subQueries []string
	var newArgs []interface{}
	for i := range args {
		newArgs = append(newArgs, args[i])
	}

	for suffix, ids := range recordsByShards {
		// Replace the table name with the sharded table
		shardQuery := strings.Replace(query, gi.TableName, gi.TableName+suffix, -1)

		// Inject the ID filter into the WHERE clause
		idPlaceholders := make([]string, len(ids))
		for i, id := range ids {
			paramIndex := len(newArgs) + 1
			idPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
			newArgs = append(newArgs, id)
		}

		idCondition := fmt.Sprintf(" AND id IN (%s)", strings.Join(idPlaceholders, ", "))

		// Find WHERE clause and add the ID condition
		wherePos := strings.Index(strings.ToUpper(shardQuery), "WHERE")
		if wherePos >= 0 {
			wherePos += 5 // Move past "WHERE"
			shardQuery = shardQuery[:wherePos] + idCondition + " AND " + shardQuery[wherePos:]
		} else {
			// If there's no WHERE clause, add one
			shardQuery = shardQuery + " WHERE id IN (" + strings.Join(idPlaceholders, ", ") + ")"
		}

		// Add to list of sub-queries
		subQueries = append(subQueries, shardQuery)
	}

	if len(subQueries) == 0 {
		return query, args, fmt.Errorf("failed to build sub-queries")
	}

	if len(subQueries) == 1 {
		// Just one shard, no need for UNION
		return subQueries[0] + " /* noglobalindex */", newArgs, nil
	}

	// Combine with UNION ALL
	finalQuery := strings.Join(subQueries, " UNION ALL ") + " /* noglobalindex */"
	return finalQuery, newArgs, nil
}

// NewConnPoolWithGlobalIndex creates a new ConnPool with global index support
func NewConnPoolWithGlobalIndex(original gorm.ConnPool, s *Sharding) *ConnPoolWithGlobalIndex {
	return &ConnPoolWithGlobalIndex{
		connPool:  original,
		connector: NewGlobalIndexConnector(s),
	}
}
