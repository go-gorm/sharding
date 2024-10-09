package sharding

import (
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/sqlparser"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"gorm.io/gorm"
	"hash/crc32"
	"strconv"
	"sync"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("can not insert different suffix table in one query ")
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

type Sharding struct {
	*gorm.DB
	ConnPool       *ConnPool
	configs        map[string]Config
	querys         sync.Map
	snowflakeNodes []*snowflake.Node

	_config Config
	_tables []any

	mutex sync.RWMutex
}

// Config specifies the configuration for sharding.
type Config struct {
	// When DoubleWrite enabled, data will double write to both main table and sharding table.
	DoubleWrite bool

	// ShardingKey specifies the table column you want to used for sharding the table rows.
	// For example, for a product order table, you may want to split the rows by `user_id`.
	ShardingKey string

	// NumberOfShards specifies how many tables you want to sharding.
	NumberOfShards uint

	// tableFormat specifies the sharding table suffix format.
	tableFormat string

	// ShardingAlgorithm specifies a function to generate the sharding
	// table's suffix by the column value.
	// For example, this function implements a mod sharding algorithm.
	//
	// 	func(value any) (suffix string, err error) {
	//		if uid, ok := value.(int64);ok {
	//			return fmt.Sprintf("_%02d", user_id % 64), nil
	//		}
	//		return "", errors.New("invalid user_id")
	// 	}
	ShardingAlgorithm func(columnValue any) (suffix string, err error)

	// ShardingSuffixs specifies a function to generate all table's suffix.
	// Used to support Migrator and generate PrimaryKey.
	// For example, this function get a mod all sharding suffixs.
	//
	// func () (suffixs []string) {
	// 	numberOfShards := 5
	// 	for i := 0; i < numberOfShards; i++ {
	// 		suffixs = append(suffixs, fmt.Sprintf("_%02d", i%numberOfShards))
	// 	}
	// 	return
	// }
	ShardingSuffixs func() (suffixs []string)

	// ShardingAlgorithmByPrimaryKey specifies a function to generate the sharding
	// table's suffix by the primary key. Used when no sharding key specified.
	// For example, this function use the Snowflake library to generate the suffix.
	//
	// 	func(id int64) (suffix string) {
	//		return fmt.Sprintf("_%02d", snowflake.ParseInt64(id).Node())
	//	}
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)

	// PrimaryKeyGenerator specifies the primary key generate algorithm.
	// Used only when insert and the record does not contains an id field.
	// Options are PKSnowflake, PKPGSequence and PKCustom.
	// When use PKCustom, you should also specify PrimaryKeyGeneratorFn.
	PrimaryKeyGenerator int

	// PrimaryKeyGeneratorFn specifies a function to generate the primary key.
	// When use auto-increment like generator, the tableIdx argument could ignored.
	// For example, this function use the Snowflake library to generate the primary key.
	// If you don't want to auto-fill the `id` or use a primary key that isn't called `id`, just return 0.
	//
	// 	func(tableIdx int64) int64 {
	//		return nodes[tableIdx].Generate().Int64()
	//	}
	PrimaryKeyGeneratorFn func(tableIdx int64) int64
}

func Register(config Config, tables ...any) *Sharding {
	return &Sharding{
		_config: config,
		_tables: tables,
	}
}

func (s *Sharding) compile() error {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}
	for _, table := range s._tables {
		if t, ok := table.(string); ok {
			s.configs[t] = s._config
		} else {
			stmt := &gorm.Statement{DB: s.DB}
			if err := stmt.Parse(table); err == nil {
				s.configs[stmt.Table] = s._config
			} else {
				return err
			}
		}
	}

	for t, c := range s.configs {
		if c.NumberOfShards > 1024 && c.PrimaryKeyGenerator == PKSnowflake {
			panic("Snowflake NumberOfShards should less than 1024")
		}

		if c.PrimaryKeyGenerator == PKSnowflake {
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		} else if c.PrimaryKeyGenerator == PKPGSequence {

			// Execute SQL to CREATE SEQUENCE for this table if not exist
			err := s.createPostgreSQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genPostgreSQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.createMySQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genMySQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKCustom {
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when use PKCustom")
			}
		} else {
			return errors.New("PrimaryKeyGenerator can only be one of PKSnowflake, PKPGSequence, PKMySQLSequence and PKCustom")
		}

		if c.ShardingAlgorithm == nil {
			if c.NumberOfShards == 0 {
				return errors.New("specify NumberOfShards or ShardingAlgorithm")
			}
			if c.NumberOfShards < 10 {
				c.tableFormat = "_%01d"
			} else if c.NumberOfShards < 100 {
				c.tableFormat = "_%02d"
			} else if c.NumberOfShards < 1000 {
				c.tableFormat = "_%03d"
			} else if c.NumberOfShards < 10000 {
				c.tableFormat = "_%04d"
			}
			c.ShardingAlgorithm = func(value any) (suffix string, err error) {
				id := 0
				switch value := value.(type) {
				case int:
					id = value
				case int64:
					id = int(value)
				case string:
					id, err = strconv.Atoi(value)
					if err != nil {
						id = int(crc32.ChecksumIEEE([]byte(value)))
					}
				default:
					return "", fmt.Errorf("default algorithm only support integer and string column," +
						"if you use other type, specify you own ShardingAlgorithm")
				}

				return fmt.Sprintf(c.tableFormat, id%int(c.NumberOfShards)), nil
			}
		}

		if c.ShardingSuffixs == nil {
			c.ShardingSuffixs = func() (suffixs []string) {
				for i := 0; i < int(c.NumberOfShards); i++ {
					suffix, err := c.ShardingAlgorithm(i)
					if err != nil {
						return nil
					}
					suffixs = append(suffixs, suffix)
				}
				return
			}
		}

		if c.ShardingAlgorithmByPrimaryKey == nil {
			if c.PrimaryKeyGenerator == PKSnowflake {
				c.ShardingAlgorithmByPrimaryKey = func(id int64) (suffix string) {
					return fmt.Sprintf(c.tableFormat, snowflake.ParseInt64(id).Node())
				}
			}
		}
		s.configs[t] = c
	}

	return nil
}

// Name plugin name for Gorm plugin interface
func (s *Sharding) Name() string {
	return "gorm:sharding"
}

// LastQuery get last SQL query
func (s *Sharding) LastQuery() string {
	if query, ok := s.querys.Load("last_query"); ok {
		return query.(string)
	}

	return ""
}

// Initialize implement for Gorm plugin interface
func (s *Sharding) Initialize(db *gorm.DB) error {
	db.Dialector = NewShardingDialector(db.Dialector, s)
	s.DB = db
	s.registerCallbacks(db)

	for t, c := range s.configs {
		if c.PrimaryKeyGenerator == PKPGSequence {
			err := s.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + pgSeqName(t)).Error
			if err != nil {
				return fmt.Errorf("init postgresql sequence error, %w", err)
			}
		}
		if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.DB.Exec("CREATE TABLE IF NOT EXISTS " + mySQLSeqName(t) + " (id INT NOT NULL)").Error
			if err != nil {
				return fmt.Errorf("init mysql create sequence error, %w", err)
			}
			err = s.DB.Exec("INSERT INTO " + mySQLSeqName(t) + " VALUES (0)").Error
			if err != nil {
				return fmt.Errorf("init mysql insert sequence error, %w", err)
			}
		}
	}

	s.snowflakeNodes = make([]*snowflake.Node, 1024)
	for i := int64(0); i < 1024; i++ {
		n, err := snowflake.NewNode(i)
		if err != nil {
			return fmt.Errorf("init snowflake node error, %w", err)
		}
		s.snowflakeNodes[i] = n
	}

	return s.compile()
}

func (s *Sharding) registerCallbacks(db *gorm.DB) {
	s.Callback().Create().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Query().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Update().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Delete().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Row().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Raw().Before("*").Register("gorm:sharding", s.switchConn)
}

func (s *Sharding) switchConn(db *gorm.DB) {
	// Support ignore sharding in some case, like:
	// When DoubleWrite is enabled, we need to query database schema
	// information by table name during the migration.
	if _, ok := db.Get(ShardingIgnoreStoreKey); !ok {
		s.mutex.Lock()
		if db.Statement.ConnPool != nil {
			s.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
			db.Statement.ConnPool = s.ConnPool
		}
		s.mutex.Unlock()
	}
}

// resolve split the old query to full table query and sharding table query
func (s *Sharding) resolve(query string, args ...interface{}) (ftQuery, stQuery, tableName string, err error) {
	ftQuery = query
	stQuery = query
	if len(s.configs) == 0 {
		return
	}

	// Parse the SQL query using pg_query_go
	parsed, err := pg_query.Parse(query)
	if err != nil {
		return ftQuery, stQuery, tableName, fmt.Errorf("error parsing query: %v", err)
	}

	if len(parsed.Stmts) == 0 {
		return ftQuery, stQuery, tableName, fmt.Errorf("no statements found in query")
	}

	// We will assume single-statement queries for simplicity
	stmt := parsed.Stmts[0]

	// Initialize variables
	var tables []string
	var isInsert bool
	var insertStmt *pg_query.InsertStmt
	var conditions []*pg_query.Node

	// Process the parsed statement to extract tables and conditions
	switch stmtNode := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		selectStmt := stmtNode.SelectStmt
		tables = collectTablesFromSelect(selectStmt)
		if selectStmt.WhereClause != nil {
			conditions = append(conditions, selectStmt.WhereClause)
		}
	case *pg_query.Node_InsertStmt:
		isInsert = true
		insertStmt = stmtNode.InsertStmt
		if insertStmt.Relation != nil {
			tables = []string{insertStmt.Relation.Relname}
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in InsertStmt.Relation")
		}
	case *pg_query.Node_UpdateStmt:
		updateStmt := stmtNode.UpdateStmt
		if updateStmt.Relation != nil {
			tables = []string{updateStmt.Relation.Relname}
			if updateStmt.WhereClause != nil {
				conditions = append(conditions, updateStmt.WhereClause)
			}
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in UpdateStmt.Relation")
		}
	case *pg_query.Node_DeleteStmt:
		deleteStmt := stmtNode.DeleteStmt
		if deleteStmt.Relation != nil {
			tables = []string{deleteStmt.Relation.Relname}
			if deleteStmt.WhereClause != nil {
				conditions = append(conditions, deleteStmt.WhereClause)
			}
		} else {
			return ftQuery, stQuery, tableName, fmt.Errorf("unexpected node type in DeleteStmt.Relation")
		}
	default:
		return ftQuery, stQuery, "", fmt.Errorf("unsupported statement type")
	}

	// Process tables and conditions to update with sharded table names
	tableMap := make(map[string]string) // originalTableName -> shardedTableName
	for _, originalTableName := range tables {
		r, ok := s.configs[originalTableName]
		if !ok {
			continue // Skip tables that are not sharded
		}

		var suffix string
		if isInsert {
			// Handle insert statements
			value, id, keyFound, err := s.extractInsertShardingKey(r, insertStmt, args...)
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}

			suffix, err = getSuffix(value, id, keyFound, r)
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}

			shardedTableName := originalTableName + suffix
			tableMap[originalTableName] = shardedTableName

			// Update the table name in the insert statement
			if insertStmt.Relation != nil {
				insertStmt.Relation.Relname = shardedTableName
			}
		} else {
			// Handle non-insert statements (SELECT, UPDATE, DELETE)
			value, id, keyFound, err := s.extractShardingKeyFromConditions(r, conditions, args...)
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}

			suffix, err = getSuffix(value, id, keyFound, r)
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}

			shardedTableName := originalTableName + suffix
			tableMap[originalTableName] = shardedTableName
		}
	}

	// Now, traverse the AST and replace any references to the original table names with sharded table names
	replaceTableNames(stmt.Stmt, tableMap)

	// Deparse the modified AST back to SQL
	stmts := []*pg_query.RawStmt{stmt}
	stQuery, err = pg_query.Deparse(&pg_query.ParseResult{Stmts: stmts})
	if err != nil {
		return ftQuery, stQuery, tableName, fmt.Errorf("error deparsing modified query: %v", err)
	}

	return
}

func collectTablesFromSelect(selectStmt *pg_query.SelectStmt) []string {
	var tables []string
	for _, fromItem := range selectStmt.FromClause {
		switch node := fromItem.Node.(type) {
		case *pg_query.Node_RangeVar:
			tables = append(tables, node.RangeVar.Relname)
		case *pg_query.Node_JoinExpr:
			tables = append(tables, collectTablesFromJoin(node.JoinExpr)...)
		}
	}
	return tables
}

func collectTablesFromJoin(joinExpr *pg_query.JoinExpr) []string {
	var tables []string
	if joinExpr.Larg != nil {
		switch larg := joinExpr.Larg.Node.(type) {
		case *pg_query.Node_RangeVar:
			tables = append(tables, larg.RangeVar.Relname)
		case *pg_query.Node_JoinExpr:
			tables = append(tables, collectTablesFromJoin(larg.JoinExpr)...)
		}
	}
	if joinExpr.Rarg != nil {
		switch rarg := joinExpr.Rarg.Node.(type) {
		case *pg_query.Node_RangeVar:
			tables = append(tables, rarg.RangeVar.Relname)
		case *pg_query.Node_JoinExpr:
			tables = append(tables, collectTablesFromJoin(rarg.JoinExpr)...)
		}
	}
	return tables
}

func (s *Sharding) extractInsertShardingKey(r Config, insertStmt *pg_query.InsertStmt, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	if len(insertStmt.Cols) == 0 || insertStmt.SelectStmt == nil || len(insertStmt.SelectStmt.GetSelectStmt().ValuesLists) == 0 {
		return nil, 0, false, errors.New("invalid insert statement structure")
	}

	colNames := insertStmt.Cols
	valuesList := insertStmt.SelectStmt.GetSelectStmt().ValuesLists[0]

	for i, colItem := range colNames {
		colName := colItem.Node.(*pg_query.Node_ResTarget).ResTarget.Name
		if colName == r.ShardingKey {
			expr := valuesList.Node.(*pg_query.Node_List).List.Items[i]
			value, err = extractValueFromExpr(expr, args)
			if err != nil {
				return nil, 0, false, err
			}
			keyFound = true
			break
		}
	}
	if !keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}
	return
}

func replaceTableNames(node *pg_query.Node, tableMap map[string]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if newName, ok := tableMap[n.RangeVar.Relname]; ok {
			n.RangeVar.Relname = newName
		}
	case *pg_query.Node_SelectStmt:
		// Recurse into FROM clause
		for _, item := range n.SelectStmt.FromClause {
			replaceTableNames(item, tableMap)
		}
		// Recurse into target list (SELECT columns)
		for _, target := range n.SelectStmt.TargetList {
			replaceTableNames(target, tableMap)
		}
		// Recurse into WHERE clause
		replaceTableNames(n.SelectStmt.WhereClause, tableMap)
	case *pg_query.Node_JoinExpr:
		replaceTableNames(n.JoinExpr.Larg, tableMap)
		replaceTableNames(n.JoinExpr.Rarg, tableMap)
		replaceTableNames(n.JoinExpr.Quals, tableMap)
	case *pg_query.Node_ResTarget:
		replaceTableNames(n.ResTarget.Val, tableMap)
	case *pg_query.Node_ColumnRef:
		// Update table name in column references
		if len(n.ColumnRef.Fields) == 2 {
			if tableNameNode, ok := n.ColumnRef.Fields[0].Node.(*pg_query.Node_String_); ok {
				if newName, ok := tableMap[tableNameNode.String_.Sval]; ok {
					tableNameNode.String_.Sval = newName
				}
			}
		}
	case *pg_query.Node_AExpr:
		replaceTableNames(n.AExpr.Lexpr, tableMap)
		replaceTableNames(n.AExpr.Rexpr, tableMap)
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			replaceTableNames(arg, tableMap)
		}
	}
}

func extractValueFromExpr(expr *pg_query.Node, args []interface{}) (interface{}, error) {
	switch v := expr.Node.(type) {
	case *pg_query.Node_ParamRef:
		// Positional parameter; PostgreSQL parameters are 1-based
		if v.ParamRef.Number > 0 && int(v.ParamRef.Number) <= len(args) {
			return args[v.ParamRef.Number-1], nil
		} else {
			return nil, fmt.Errorf("parameter index out of range")
		}
	case *pg_query.Node_AConst:
		switch val := v.AConst.Val.(type) {
		case *pg_query.A_Const_Ival:
			return val.Ival, nil
		case *pg_query.A_Const_Fval:
			return val.Fval, nil
		case *pg_query.A_Const_Sval:
			return val.Sval, nil
		default:
			return nil, fmt.Errorf("unsupported constant type")
		}
	default:
		return nil, fmt.Errorf("unsupported expression type")
	}
}

func (s *Sharding) extractShardingKeyFromConditions(r Config, conditions []*pg_query.Node, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	for _, condition := range conditions {
		keyFound, value, err = traverseConditionForKey(r.ShardingKey, condition, args)
		if keyFound || err != nil {
			break
		}
	}
	if !keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}
	return
}

func traverseConditionForKey(shardingKey string, node *pg_query.Node, args []interface{}) (keyFound bool, value interface{}, err error) {
	if node == nil {
		return false, nil, nil
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		// Check if the expression is a simple equality
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
			if opName == "=" {
				// Left and right sides
				var columnName string
				var exprValue *pg_query.Node

				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					if len(colRef.ColumnRef.Fields) > 0 {
						if colNameNode, ok := colRef.ColumnRef.Fields[len(colRef.ColumnRef.Fields)-1].Node.(*pg_query.Node_String_); ok {
							columnName = colNameNode.String_.Sval
						}
					}
				}
				if columnName == shardingKey {
					exprValue = n.AExpr.Rexpr
					value, err = extractValueFromExpr(exprValue, args)
					if err != nil {
						return false, nil, err
					}
					return true, value, nil
				}
			}
		}
	case *pg_query.Node_BoolExpr:
		for _, arg := range n.BoolExpr.Args {
			keyFound, value, err = traverseConditionForKey(shardingKey, arg, args)
			if keyFound || err != nil {
				return keyFound, value, err
			}
		}
	}
	return false, nil, nil
}

func getSuffix(value any, id int64, keyFind bool, r Config) (suffix string, err error) {
	if keyFind {
		suffix, err = r.ShardingAlgorithm(value)
		if err != nil {
			return
		}
	} else {
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = fmt.Errorf("there is not sharding key and ShardingAlgorithmByPrimaryKey is not configured")
			return
		}
		suffix = r.ShardingAlgorithmByPrimaryKey(id)
	}
	return
}

func (s *Sharding) insertValue(key string, names []*sqlparser.Ident, exprs []sqlparser.Expr, args ...any) (value any, id int64, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, 0, keyFind, errors.New("column names and expressions mismatch")
	}

	for i, name := range names {
		if name.Name == key {
			switch expr := exprs[i].(type) {
			case *sqlparser.BindExpr:
				value = args[expr.Pos]
			case *sqlparser.StringLit:
				value = expr.Value
			case *sqlparser.NumberLit:
				value = expr.Value
			default:
				return nil, 0, keyFind, sqlparser.ErrNotImplemented
			}
			keyFind = true
			break
		}
	}
	if !keyFind {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

func (s *Sharding) nonInsertValue(key string, condition sqlparser.Expr, args ...any) (value any, id int64, keyFind bool, err error) {
	err = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if n, ok := node.(*sqlparser.BinaryExpr); ok {
			x, ok := n.X.(*sqlparser.Ident)
			if !ok {
				if q, ok2 := n.X.(*sqlparser.QualifiedRef); ok2 {
					x = q.Column
					ok = true
				}
			}
			if ok {
				if x.Name == key && n.Op == sqlparser.EQ {
					keyFind = true
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						value = args[expr.Pos]
					case *sqlparser.StringLit:
						value = expr.Value
					case *sqlparser.NumberLit:
						value = expr.Value
					default:
						return sqlparser.ErrNotImplemented
					}
					return nil
				} else if x.Name == "id" && n.Op == sqlparser.EQ {
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						v := args[expr.Pos]
						var ok bool
						if id, ok = v.(int64); !ok {
							return fmt.Errorf("ID should be int64 type")
						}
					case *sqlparser.NumberLit:
						id, err = strconv.ParseInt(expr.Value, 10, 64)
						if err != nil {
							return err
						}
					default:
						return ErrInvalidID
					}
					return nil
				}
			}
		}
		return nil
	}), condition)
	if err != nil {
		return
	}

	if !keyFind && id == 0 {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

func replaceOrderByTableName(orderBy []*sqlparser.OrderingTerm, oldName, newName string) []*sqlparser.OrderingTerm {
	for i, term := range orderBy {
		if x, ok := term.X.(*sqlparser.QualifiedRef); ok {
			if x.Table.Name == oldName {
				x.Table.Name = newName
				orderBy[i].X = x
			}
		}
	}

	return orderBy
}
