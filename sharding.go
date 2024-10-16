package sharding

import (
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"gorm.io/gorm"
	"hash/crc32"
	"strconv"
	"strings"
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
				switch v := value.(type) {
				case int:
					id = v
				case int64:
					id = int(v)
				case int32:
					id = int(v)
				case int16:
					id = int(v)
				case int8:
					id = int(v)
				case uint:
					id = int(v)
				case uint64:
					id = int(v)
				case uint32:
					id = int(v)
				case uint16:
					id = int(v)
				case uint8:
					id = int(v)
				case float64:
					id = int(v)
				case float32:
					id = int(v)
				case string:
					id, err = strconv.Atoi(v)
					if err != nil {
						id = int(crc32.ChecksumIEEE([]byte(v)))
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
			} else if c.PrimaryKeyGenerator == PKPGSequence || c.PrimaryKeyGenerator == PKMySQLSequence {
				c.ShardingAlgorithmByPrimaryKey = func(id int64) (suffix string) {
					return fmt.Sprintf(c.tableFormat, id%int64(c.NumberOfShards))
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
		// Check if the query is accessing system tables
		if isSystemQuery(db.Statement.SQL.String()) {
			return
		}
		s.mutex.Lock()
		if db.Statement.ConnPool != nil {
			s.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
			db.Statement.ConnPool = s.ConnPool
		}
		s.mutex.Unlock()
	}
}

// resolve splits the old query into full table query and sharding table query
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

	// Assume single-statement queries
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
		if strings.Contains(query, "/* nosharding */") {
			return ftQuery, stQuery, tableName, nil
		}
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
	// Handle DDL statements by returning the original query
	case *pg_query.Node_CreateStmt,
		*pg_query.Node_DropStmt,
		*pg_query.Node_AlterTableStmt,
		*pg_query.Node_TruncateStmt,
		*pg_query.Node_RenameStmt,
		*pg_query.Node_IndexStmt,
		*pg_query.Node_CommentStmt,
		*pg_query.Node_GrantStmt:
		// DDL statements. Bypass sharding.
		return query, query, "", nil
	default:
		return ftQuery, stQuery, "", fmt.Errorf("unsupported statement type")
	}

	// Process tables and conditions to update with sharded table names
	tableMap := make(map[string]string) // originalTableName -> shardedTableName
	for _, originalTableName := range tables {
		schemaName := ""
		tableName = originalTableName

		// Check for schema-qualified table names
		if strings.Contains(originalTableName, ".") {
			parts := strings.SplitN(originalTableName, ".", 2)
			schemaName = parts[0]
			tableName = parts[1]
		}

		fullTableName := tableName
		if schemaName != "" {
			fullTableName = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		r, ok := s.configs[fullTableName]
		if !ok {
			continue // Skip tables not configured for sharding
		}

		var suffix string
		if isInsert {
			// Handle insert statements
			var consistentSuffix string
			suffixes := make(map[string]bool)

			if insertStmt.SelectStmt != nil {
				selectStmt := insertStmt.SelectStmt.GetSelectStmt()
				if selectStmt != nil && len(selectStmt.ValuesLists) > 0 {
					// Iterate through each values list to extract suffixes
					for _, valuesList := range selectStmt.ValuesLists {
						value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
						if err != nil {
							return ftQuery, stQuery, tableName, err
						}
						//fmt.Println("value", value, "id", id, "keyFound", keyFound)
						currentSuffix, err := getSuffix(value, id, keyFound, r)
						if err != nil {
							return ftQuery, stQuery, tableName, err
						}

						suffixes[currentSuffix] = true

						// If more than one unique suffix is found, return an error
						if len(suffixes) > 1 {
							return ftQuery, stQuery, tableName, ErrInsertDiffSuffix
						}

						// Capture the consistent suffix
						consistentSuffix = currentSuffix
					}
				} else {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement select is not a ValuesLists")
				}
			} else if len(insertStmt.Cols) > 0 {
				for _, valuesList := range insertStmt.Cols {
					value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}
					currentSuffix, err := getSuffix(value, id, keyFound, r)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}

					suffixes[currentSuffix] = true

					// If more than one unique suffix is found, return an error
					if len(suffixes) > 1 {
						return ftQuery, stQuery, tableName, ErrInsertDiffSuffix
					}

					// Capture the consistent suffix
					//consistentSuffix = currentSuffix					//
				}
			} else {
				return ftQuery, stQuery, tableName, fmt.Errorf("unsupported insert statement")
			}

			// Ensure all suffixes are consistent
			if len(suffixes) == 1 {
				suffix = consistentSuffix
			} else {
				return ftQuery, stQuery, tableName, ErrInsertDiffSuffix
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

	// Traverse the AST and replace original table names with sharded table names
	replaceTableNames(stmt.Stmt, tableMap)

	// Deparse the modified AST back to SQL
	stmts := []*pg_query.RawStmt{stmt}
	stQuery, err = pg_query.Deparse(&pg_query.ParseResult{Stmts: stmts})
	if err != nil {
		return ftQuery, stQuery, tableName, fmt.Errorf("error deparsing modified query: %v", err)
	}

	return
}

func (s *Sharding) extractInsertShardingKeyFromValues(r Config, insertStmt *pg_query.InsertStmt, valuesList *pg_query.Node, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	// Ensure that columns are specified
	if len(insertStmt.Cols) == 0 {
		return nil, 0, false, errors.New("invalid insert statement structure: no columns specified")
	}

	// Type assert the valuesList to *pg_query.Node_List
	list, ok := valuesList.Node.(*pg_query.Node_List)
	if !ok {
		return nil, 0, false, errors.New("unsupported values list type")
	}

	// Ensure the number of values matches the number of columns
	if len(list.List.Items) < len(insertStmt.Cols) {
		return nil, 0, false, errors.New("values list has fewer items than columns")
	}

	// Iterate through columns to find the sharding key
	for i, colItem := range insertStmt.Cols {
		// Extract column name
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			continue // Skip if not a ResTarget
		}
		colName := resTarget.ResTarget.Name

		// Check if this column is the sharding key
		if colName == r.ShardingKey {
			// Extract the corresponding value expression
			expr := list.List.Items[i]
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

	for i, colItem := range insertStmt.Cols {
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			continue
		}
		colName := resTarget.ResTarget.Name
		if colName == "id" {
			expr := list.List.Items[i]
			idValue, err := extractValueFromExpr(expr, args)
			if err != nil {
				return nil, 0, false, err
			}
			id, ok = idValue.(int64)
			if !ok {
				return nil, 0, false, ErrInvalidID
			}
			break
		}
	}

	return value, id, keyFound, nil
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
		tables = append(tables, collectTablesFromExpr(joinExpr.Larg)...)
	}
	if joinExpr.Rarg != nil {
		tables = append(tables, collectTablesFromExpr(joinExpr.Rarg)...)
	}
	return tables
}

func collectTablesFromExpr(expr *pg_query.Node) []string {
	switch n := expr.Node.(type) {
	case *pg_query.Node_RangeVar:
		return []string{n.RangeVar.Relname}
	case *pg_query.Node_JoinExpr:
		return collectTablesFromJoin(n.JoinExpr)
	default:
		return nil
	}
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
		// Replace table names in RangeVar nodes
		schemaName := n.RangeVar.Schemaname
		tableName := n.RangeVar.Relname
		fullTableName := tableName
		if schemaName != "" {
			fullTableName = fmt.Sprintf("%s.%s", schemaName, tableName)
		}

		if newName, ok := tableMap[fullTableName]; ok {
			n.RangeVar.Relname = newName
			n.RangeVar.Location = -1 // Force quoting if necessary
		}

	case *pg_query.Node_UpdateStmt:
		if newName, ok := tableMap[n.UpdateStmt.Relation.Relname]; ok {
			n.UpdateStmt.Relation.Relname = newName
			n.UpdateStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.UpdateStmt.WhereClause, tableMap)
		for _, target := range n.UpdateStmt.TargetList {
			replaceTableNames(target, tableMap)
		}
	case *pg_query.Node_DeleteStmt:
		if newName, ok := tableMap[n.DeleteStmt.Relation.Relname]; ok {
			n.DeleteStmt.Relation.Relname = newName
			n.DeleteStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.DeleteStmt.WhereClause, tableMap)
	case *pg_query.Node_SelectStmt:
		// Recursively process FROM clause and other relevant clauses
		for _, item := range n.SelectStmt.FromClause {
			replaceTableNames(item, tableMap)
		}
		for _, target := range n.SelectStmt.TargetList {
			replaceTableNames(target, tableMap)
		}
		replaceTableNames(n.SelectStmt.WhereClause, tableMap)
	case *pg_query.Node_JoinExpr:
		// Recursively process left and right arguments
		replaceTableNames(n.JoinExpr.Larg, tableMap)
		replaceTableNames(n.JoinExpr.Rarg, tableMap)
		replaceTableNames(n.JoinExpr.Quals, tableMap)

	case *pg_query.Node_ResTarget:
		// Process expressions in the SELECT list
		replaceTableNames(n.ResTarget.Val, tableMap)
	case *pg_query.Node_ColumnRef:
		fields := n.ColumnRef.Fields
		if len(fields) >= 2 {
			// Check if the first field is a table name
			if stringNode, ok := fields[0].Node.(*pg_query.Node_String_); ok {
				originalTableName := stringNode.String_.Sval
				if newTableName, exists := tableMap[originalTableName]; exists {
					// Replace the table name with the sharded name
					stringNode.String_.Sval = newTableName
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
		return extractValueFromAConst(v.AConst)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr.Node)
	}
}

func extractValueFromAConst(aConst *pg_query.A_Const) (interface{}, error) {
	switch val := aConst.Val.(type) {
	case *pg_query.A_Const_Ival:
		// Integer value
		if val.Ival != nil {
			return val.Ival.Ival, nil
		}
	case *pg_query.A_Const_Sval:
		// String value
		if val.Sval != nil {
			return val.Sval.Sval, nil
		}
	case *pg_query.A_Const_Fval:
		// Float value
		if val.Fval != nil {
			return strconv.ParseFloat(val.Fval.Fval, 64)
		}
	case *pg_query.A_Const_Boolval:
		// Boolean value
		if val.Boolval != nil {
			return val.Boolval.Boolval, nil
		}
	case *pg_query.A_Const_Bsval:
		// Bit string value
		if val.Bsval != nil {
			return val.Bsval.Bsval, nil
		}
	default:
		return nil, fmt.Errorf("unsupported constant type: %T", val)
	}
	return nil, fmt.Errorf("value is nil in A_Const")
}

func (s *Sharding) extractShardingKeyFromConditions(r Config, conditions []*pg_query.Node, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	for _, condition := range conditions {
		keyFound, value, err = traverseConditionForKey(r.ShardingKey, condition, args)
		if keyFound || err != nil {
			break
		}
	}

	if !keyFound {
		// Try to extract 'id' if 'ShardingAlgorithmByPrimaryKey' is set
		for _, condition := range conditions {
			idFound, idValue, err := traverseConditionForKey("id", condition, args)
			if idFound {
				idInt64, ok := idValue.(int64)
				if !ok {
					err = ErrInvalidID
					return nil, 0, false, err
				}
				id = idInt64
				return nil, id, false, nil
			}
		}
		// If 'id' is not found, and 'ShardingAlgorithmByPrimaryKey' is not set
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = ErrMissingShardingKey
			return nil, 0, false, err
		}
	}

	return value, id, keyFound, err
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

func isSystemQuery(query string) bool {
	systemTables := []string{
		"information_schema",
		"pg_catalog",
		"CREATE TABLE",
		"CREATE INDEX",
		"CREATE SEQUENCE",
		"ALTER TABLE",
		"DROP TABLE",
		"DROP INDEX",
		// todo: Add other system schemas if necessary
	}
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	for _, sysTable := range systemTables {
		if strings.Contains(upperQuery, strings.ToUpper(sysTable)) {
			return true
		}
	}
	return false
}
