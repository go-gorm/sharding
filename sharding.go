package sharding

import (
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"gorm.io/gorm"
	"hash/crc32"
	"log"
	"math"
	"math/big"
	"reflect"
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

	// ValueConverter converts values before they are used in SQL queries
	// This is especially useful for handling custom types like UInt256
	ValueConverter func(value interface{}) (interface{}, error)

	engine DatabaseEngine
}

func Register(config interface{}, tables ...interface{}) *Sharding {
	s := &Sharding{
		_tables: tables,
	}
	switch c := config.(type) {
	case Config:
		s._config = c
	case map[string]Config:
		s.configs = c
	default:
		panic("Invalid config type")
	}
	return s
}

func (s *Sharding) compile() error {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}
	for _, table := range s._tables {
		var tableName string
		if t, ok := table.(string); ok {
			tableName = t
		} else {
			stmt := &gorm.Statement{DB: s.DB}
			if err := stmt.Parse(table); err != nil {
				return err
			}
			tableName = stmt.Table
		}

		// Only set the default config if a specific config is not already set
		if _, exists := s.configs[tableName]; !exists {
			s.configs[tableName] = s._config
		}
	}

	for t, c := range s.configs {
		if c.NumberOfShards > 1024 && c.PrimaryKeyGenerator == PKSnowflake {
			panic("Snowflake NumberOfShards should be less than 1024")
		}

		// Set up PrimaryKeyGeneratorFn based on PrimaryKeyGenerator
		switch c.PrimaryKeyGenerator {
		case PKSnowflake:
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		case PKPGSequence:
			// Execute SQL to CREATE SEQUENCE for this table if not exist
			if err := s.createPostgreSQLSequenceKeyIfNotExist(t); err != nil {
				return err
			}
			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genPostgreSQLSequenceKey(t, index)
			}
		case PKMySQLSequence:
			if err := s.createMySQLSequenceKeyIfNotExist(t); err != nil {
				return err
			}
			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genMySQLSequenceKey(t, index)
			}
		case PKCustom:
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when using PKCustom")
			}
		default:
			return errors.New("PrimaryKeyGenerator must be one of PKSnowflake, PKPGSequence, PKMySQLSequence, or PKCustom")
		}

		// Set up ShardingAlgorithm if not provided
		if c.ShardingAlgorithm == nil {
			if c.NumberOfShards == 0 {
				return errors.New("NumberOfShards must be specified if ShardingAlgorithm is not provided for table" + t)
			}
			switch {
			case c.NumberOfShards < 10:
				c.tableFormat = "_%01d"
			case c.NumberOfShards < 100:
				c.tableFormat = "_%02d"
			case c.NumberOfShards < 1000:
				c.tableFormat = "_%03d"
			case c.NumberOfShards < 10000:
				c.tableFormat = "_%04d"
			default:
				return errors.New("NumberOfShards exceeds maximum allowed shards")
			}
			c.ShardingAlgorithm = func(value any) (string, error) {
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
					var err error
					id, err = strconv.Atoi(v)
					if err != nil {
						id = int(crc32.ChecksumIEEE([]byte(v)))
					}
				default:
					return "", fmt.Errorf("default algorithm only supports integer and string types; specify your own ShardingAlgorithm")
				}

				return fmt.Sprintf(c.tableFormat, id%int(c.NumberOfShards)), nil
			}
		}

		// Set up ShardingSuffixs if not provided
		if c.ShardingSuffixs == nil {
			c.ShardingSuffixs = func() []string {
				var suffixes []string
				for i := 0; i < int(c.NumberOfShards); i++ {
					suffix, err := c.ShardingAlgorithm(i)
					if err != nil {
						return nil
					}
					suffixes = append(suffixes, suffix)
				}
				return suffixes
			}
		}

		// Set up ShardingAlgorithmByPrimaryKey if not provided
		if c.ShardingAlgorithmByPrimaryKey == nil {
			switch c.PrimaryKeyGenerator {
			case PKSnowflake:
				c.ShardingAlgorithmByPrimaryKey = func(id int64) string {
					return fmt.Sprintf(c.tableFormat, snowflake.ParseInt64(id).Node())
				}
			case PKPGSequence, PKMySQLSequence:
				c.ShardingAlgorithmByPrimaryKey = func(id int64) string {
					return fmt.Sprintf(c.tableFormat, id%int64(c.NumberOfShards))
				}
			}
		}

		// Assign the updated config back to the map
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
	s.setDatabaseEngine()
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
	var isInsert, isSelect bool
	var insertStmt *pg_query.InsertStmt
	var selectStmt *pg_query.SelectStmt
	var conditions []*pg_query.Node
	// Initialize a map to hold table-specific sharded names
	tableMap := make(map[string]string) // originalTableName -> shardedTableName

	// Process the parsed statement to extract tables and conditions
	switch stmtNode := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		isSelect = true
		selectStmt = stmtNode.SelectStmt
		if strings.Contains(query, "/* nosharding */") {
			return ftQuery, stQuery, tableName, nil
		}
		tables = collectTablesFromSelect(selectStmt)
		if selectStmt.WhereClause != nil {
			conditions = append(conditions, selectStmt.WhereClause)
		}
		// Collect conditions from JOINs
		joinConditions := collectJoinConditions(selectStmt)
		conditions = append(conditions, joinConditions...)

		// If this is a SELECT without sharding key conditions
		if isSelect && len(conditions) > 0 {
			hasShardingKey := false
			for _, table := range tables {
				if cfg, ok := s.configs[table]; ok {
					shardingKey := cfg.ShardingKey
					_, _, keyFound, _ := s.extractShardingKeyFromConditions(shardingKey, conditions, args, nil, table)
					if keyFound {
						hasShardingKey = true
						break
					}
				}
			}

			// If no sharding key found, we have two options:
			if !hasShardingKey {
				// Option 1: Fall back to the main table if DoubleWrite is enabled
				for _, table := range tables {
					if cfg, ok := s.configs[table]; ok && cfg.DoubleWrite {
						return ftQuery, stQuery, tableName, nil
					}
				}
				// Option 2: Query all shards
				var allQueries []string
				for _, table := range tables {
					if cfg, ok := s.configs[table]; ok {
						suffixes := cfg.ShardingSuffixs()
						for _, suffix := range suffixes {
							shardedQuery := query
							shardedQuery = strings.Replace(shardedQuery, table, table+suffix, -1)
							allQueries = append(allQueries, shardedQuery)
						}
					}
				}

				if len(allQueries) > 0 {
					// Create a UNION ALL query combining all shards
					stQuery = strings.Join(allQueries, " UNION ALL ")
					return ftQuery, stQuery, tableName, nil
				}

			}
		}

	case *pg_query.Node_InsertStmt:
		isInsert = true
		insertStmt = stmtNode.InsertStmt
		if insertStmt.Relation != nil {
			// Get base table name
			baseTable := insertStmt.Relation.Relname
			tables = []string{baseTable}

			// Update table name immediately to ensure RETURNING clause uses correct table
			if shardedName, exists := tableMap[baseTable]; exists {
				insertStmt.Relation.Relname = shardedName

				// Also update any RETURNING clauses to use sharded table name
				if insertStmt.ReturningList != nil {
					for _, returningItem := range insertStmt.ReturningList {
						if resTarget, ok := returningItem.Node.(*pg_query.Node_ResTarget); ok {
							if colRef, ok := resTarget.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
								if len(colRef.ColumnRef.Fields) > 0 {
									if stringNode, ok := colRef.ColumnRef.Fields[0].Node.(*pg_query.Node_String_); ok {
										if stringNode.String_.Sval == baseTable {
											stringNode.String_.Sval = shardedName
										}
									}
								}
							}
						}
					}
				}
			}
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

	// Iterate through each table to determine its sharded name
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
				nestedSelect, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
				if !ok {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement select is not a SelectStmt")
				}

				valuesSelect := nestedSelect.SelectStmt
				if len(valuesSelect.ValuesLists) == 0 {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement has no VALUES list")
				}
				// Iterate through each values list to extract suffixes
				for _, valuesList := range valuesSelect.ValuesLists {
					value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}
					//log.Printf("Extracted sharding key: %v, id: %d, keyFound: %v\n", value, id, keyFound)

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
				if len(insertStmt.GetReturningList()) == 0 {
					return ftQuery, stQuery, tableName, fmt.Errorf("insert statement has no VALUES list")
				}

				// Iterate through each values list to extract suffixes
				for _, valuesList := range insertStmt.ReturningList {
					value, id, keyFound, err := s.extractInsertShardingKeyFromValues(r, insertStmt, valuesList, args...)
					if err != nil {
						return ftQuery, stQuery, tableName, err
					}
					//log.Printf("Extracted sharding key: %v, id: %d, keyFound: %v\n", value, id, keyFound)

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
				//log.Printf("Updated table name to '%s'\n", shardedTableName)

				// Now handle ID generation with args
				err := s.assignIDToInsert(insertStmt, r, &args)
				if err != nil {
					return ftQuery, stQuery, tableName, err
				}
			}
		} else {
			// Handle non-insert statements (SELECT, UPDATE, DELETE)
			shardingKey := r.ShardingKey
			if strings.Contains(shardingKey, ".") {
				parts := strings.Split(shardingKey, ".")
				shardingKey = parts[len(parts)-1]
			}
			var aliasMap map[string]string
			if isSelect {
				aliasMap = collectTableAliases(selectStmt)
			}

			// Extract sharding key for the current table
			value, id, keyFound, err := s.extractShardingKeyFromConditions(shardingKey, conditions, args, aliasMap, fullTableName)
			if err != nil {
				log.Printf("Error extracting sharding key for table '%s': %v\n", originalTableName, err)
				return ftQuery, stQuery, tableName, err
			}

			// Determine the suffix based on the sharding key
			suffix, err = getSuffix(value, id, keyFound, r)
			if err != nil {
				log.Printf("Error determining suffix for table '%s': %v\n", originalTableName, err)
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

// extractShardingKeyFromConditions extracts the sharding key for a specific table based on its conditions
func (s *Sharding) extractShardingKeyFromConditions(shardingKey string, conditions []*pg_query.Node, args []interface{}, aliasMap map[string]string, currentTable string) (value interface{}, id int64, keyFound bool, err error) {
	// Initialize a separate knownKeys map for the current table
	knownKeys := make(map[string]interface{})

	// Iterate through each condition to find the sharding key
	for _, condition := range conditions {
		keyFound, value, err = traverseConditionForKey(shardingKey, condition, args, knownKeys, aliasMap)
		if keyFound || err != nil {
			break
		}
	}

	// If sharding key is not found, attempt to find 'id' for primary key sharding
	if !keyFound {
		var idFound bool
		var idValue interface{}
		for _, condition := range conditions {
			//log.Println("Traversing condition for 'id'")
			idFound, idValue, err = traverseConditionForKey("id", condition, args, knownKeys, aliasMap)
			if idFound || err != nil {
				break
			}
		}
		if idFound {
			idInt64, err := toInt64(idValue)
			if err != nil {
				return nil, 0, false, ErrInvalidID
			}
			id = idInt64
		} else {
			// Neither sharding key nor 'id' found; return error
			err = ErrMissingShardingKey
			return nil, 0, false, err
		}
	}

	return value, id, keyFound, err
}

func collectJoinConditions(selectStmt *pg_query.SelectStmt) []*pg_query.Node {
	var conditions []*pg_query.Node
	for _, fromItem := range selectStmt.FromClause {
		conditions = append(conditions, extractConditionsFromNode(fromItem)...)
	}
	return conditions
}

func extractConditionsFromNode(node *pg_query.Node) []*pg_query.Node {
	var conditions []*pg_query.Node
	switch n := node.Node.(type) {
	case *pg_query.Node_JoinExpr:
		if n.JoinExpr.Quals != nil {
			conditions = append(conditions, n.JoinExpr.Quals)
		}
		// Recursively extract from left and right arguments
		conditions = append(conditions, extractConditionsFromNode(n.JoinExpr.Larg)...)
		conditions = append(conditions, extractConditionsFromNode(n.JoinExpr.Rarg)...)
	case *pg_query.Node_RangeSubselect:
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			conditions = append(conditions, collectConditionsFromSelect(subselect.SelectStmt)...)
		}
	}
	return conditions
}

func collectConditionsFromSelect(selectStmt *pg_query.SelectStmt) []*pg_query.Node {
	var conditions []*pg_query.Node
	if selectStmt.WhereClause != nil {
		conditions = append(conditions, selectStmt.WhereClause)
	}
	conditions = append(conditions, collectJoinConditions(selectStmt)...)
	return conditions
}

// assignIDToInsert generates a new ID and assigns it to the insert statement's VALUES list
func (s *Sharding) assignIDToInsert(insertStmt *pg_query.InsertStmt, r Config, args *[]interface{}) error {
	// Check if 'id' column is present
	hasID := false
	idIndex := -1
	for i, colItem := range insertStmt.Cols {
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			return fmt.Errorf("unsupported column item type: %T", colItem.Node)
		}
		colName := resTarget.ResTarget.Name
		if strings.ToLower(colName) == "id" {
			hasID = true
			idIndex = i
			break
		}
	}

	// Extract shard index from table name
	shardedTableName := insertStmt.Relation.Relname
	shardIndex, err := extractShardIndex(shardedTableName, r)
	if err != nil {
		return err
	}

	if hasID {
		// Handle existing 'id' column
		if insertStmt.SelectStmt == nil {
			return fmt.Errorf("insert statement has no SelectStmt")
		}

		selectNode, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
		if !ok {
			return fmt.Errorf("insert statement SelectStmt is not of type SelectStmt")
		}

		valuesSelect := selectNode.SelectStmt
		if len(valuesSelect.ValuesLists) == 0 {
			return fmt.Errorf("insert statement has no VALUES list")
		}

		// Iterate through each VALUES list to assign 'id's
		for idx, valuesList := range valuesSelect.ValuesLists {
			listNode, ok := valuesList.Node.(*pg_query.Node_List)
			if !ok {
				return fmt.Errorf("unsupported values list type when assigning id")
			}

			// Ensure the VALUES list has enough items
			if len(listNode.List.Items) <= idIndex {
				return fmt.Errorf("values list does not have enough items for 'id' in VALUES list index %d", idx)
			}

			// Extract the current 'id' value
			expr := listNode.List.Items[idIndex]
			exprValue, err := extractValueFromExpr(expr, *args)
			if err != nil {
				return err
			}

			// Convert the 'id' value to int64
			idInt64, err := toInt64(exprValue)
			if err != nil {
				return err
			}

			if idInt64 == 0 {
				// Generate a new ID if 'id' is zero
				if r.PrimaryKeyGeneratorFn != nil {
					generatedID := r.PrimaryKeyGeneratorFn(int64(shardIndex))
					if generatedID != 0 {
						// Replace the 'id' value with the generated ID
						listNode.List.Items[idIndex] = &pg_query.Node{
							Node: &pg_query.Node_AConst{
								AConst: &pg_query.A_Const{
									Val: &pg_query.A_Const_Ival{
										Ival: &pg_query.Integer{Ival: int32(generatedID)},
									},
								},
							},
						}
					}
					// Else, leave 'id' as zero
				}
			}
			// Else, leave 'id' as is
		}
	} else {
		// 'id' is not present in insert columns
		if r.PrimaryKeyGeneratorFn != nil {
			generatedID := r.PrimaryKeyGeneratorFn(int64(shardIndex))
			if generatedID != 0 {
				// Proceed to add 'id' column and value
				log.Println("'id' column not present in insert columns; adding it.")
				insertStmt.Cols = append(insertStmt.Cols, &pg_query.Node{
					Node: &pg_query.Node_ResTarget{
						ResTarget: &pg_query.ResTarget{
							Name: "id",
						},
					},
				})

				if insertStmt.SelectStmt == nil {
					return fmt.Errorf("insert statement has no SelectStmt")
				}

				selectNode, ok := insertStmt.SelectStmt.Node.(*pg_query.Node_SelectStmt)
				if !ok {
					return fmt.Errorf("insert statement SelectStmt is not of type SelectStmt")
				}

				valuesSelect := selectNode.SelectStmt
				if len(valuesSelect.ValuesLists) == 0 {
					return fmt.Errorf("insert statement has no VALUES list")
				}

				for _, valuesList := range valuesSelect.ValuesLists {
					listNode, ok := valuesList.Node.(*pg_query.Node_List)
					if !ok {
						return fmt.Errorf("unsupported values list type when assigning id")
					}

					// Append the generated ID to the VALUES list
					listNode.List.Items = append(listNode.List.Items, &pg_query.Node{
						Node: &pg_query.Node_AConst{
							AConst: &pg_query.A_Const{
								Val: &pg_query.A_Const_Ival{
									Ival: &pg_query.Integer{Ival: int32(generatedID)},
								},
							},
						},
					})
				}
			}
			// Else, generatedID == 0, so we skip adding 'id' column
		}
	}

	return nil
}

// Helper function to extract shard index from the sharded table name
func extractShardIndex(tableName string, r Config) (int64, error) {
	// Assuming tableName has suffix like "_0", "_1", etc.
	parts := strings.Split(tableName, "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("table name '%s' does not have a suffix", tableName)
	}
	suffix := parts[len(parts)-1]
	shardIndex, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, fmt.Errorf("invalid shard index in table name '%s': %v", tableName, err)
	}
	return int64(shardIndex), nil
}

func (s *Sharding) extractInsertShardingKeyFromValues(r Config, insertStmt *pg_query.InsertStmt, valuesList *pg_query.Node, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	// Ensure that columns are specified
	if len(insertStmt.Cols) == 0 {
		return nil, 0, false, errors.New("invalid insert statement structure: no columns specified")
	}

	// Type assert the valuesList to *pg_query.Node_List
	listNode, ok := valuesList.Node.(*pg_query.Node_List)
	if !ok {
		return nil, 0, false, errors.New("unsupported values list type")
	}
	list := listNode.List

	// Ensure the number of values matches the number of columns
	if len(list.Items) != len(insertStmt.Cols) {
		return nil, 0, false, errors.New("values list has fewer items than columns")
	}
	// Iterate through columns to find the sharding key and id
	for i, colItem := range insertStmt.Cols {
		resTarget, ok := colItem.Node.(*pg_query.Node_ResTarget)
		if !ok {
			return nil, 0, false, fmt.Errorf("unsupported column item type: %T", colItem.Node)
		}
		colName := resTarget.ResTarget.Name

		expr := list.Items[i]
		exprValue, err := s.extractValueFromExpr(expr, args, r) // Use updated method
		if err != nil {
			return nil, 0, false, err
		}

		if strings.ToLower(colName) == "id" {
			idValue, err := toInt64(exprValue)
			if err != nil {
				return nil, 0, false, ErrInvalidID
			}
			id = idValue
			//log.Printf("ID found: %s = %v\n", colName, id)
		}

		if colName == r.ShardingKey {
			value = exprValue
			keyFound = true
			//log.Printf("Sharding key found: %s = %v\n", colName, value)
		}
	}

	if !keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}

	return value, id, keyFound, nil
}

func toInt64(value interface{}) (int64, error) {

	if value == nil {
		return 0, fmt.Errorf("cannot convert nil to int64")
	}

	// Handle pointer types first
	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Ptr {
		if reflect.ValueOf(value).IsNil() {
			return 0, fmt.Errorf("cannot convert nil pointer to int64")
		}
		// Dereference the pointer and recursively call toInt64
		return toInt64(reflect.ValueOf(value).Elem().Interface())
	}

	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			log.Printf("uint64 value %d overflows int64", v)
			return 0, fmt.Errorf("uint64 value %d overflows int64", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting string to int64: %v", err)
		}
		return i, nil
	default:
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Struct {
			// Try to find a field named "I" that is a *big.Int
			field := val.FieldByName("I")
			if field.IsValid() && field.Type() == reflect.TypeOf(&big.Int{}) {
				bigIntPtr := field.Interface().(*big.Int)
				if bigIntPtr != nil {
					return bigIntPtr.Int64(), nil
				}
			}
		}
		log.Printf("Unsupported type for conversion to int64: %T\n", v)
		return 0, fmt.Errorf("unsupported type for conversion to int64: %T", v)
	}
}

func collectTablesFromSelect(selectStmt *pg_query.SelectStmt) []string {
	var tables []string
	for _, fromItem := range selectStmt.FromClause {
		switch node := fromItem.Node.(type) {
		case *pg_query.Node_RangeVar:
			tables = append(tables, node.RangeVar.Relname)
		case *pg_query.Node_JoinExpr:
			tables = append(tables, collectTablesFromJoin(node.JoinExpr)...)
		case *pg_query.Node_RangeSubselect:
			// Recurse into subselect
			if subselect, ok := node.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
				tables = append(tables, collectTablesFromSelect(subselect.SelectStmt)...)
			}
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
	case *pg_query.Node_RangeSubselect:
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			return collectTablesFromSelect(subselect.SelectStmt)
		}
	}
	return nil
}

func replaceTableNames(node *pg_query.Node, tableMap map[string]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if n.RangeVar.Schemaname != "" {
			// Do not replace schema-qualified table names
			return
		}
		// Replace table names in RangeVar nodes
		if shardedName, exists := tableMap[n.RangeVar.Relname]; exists {
			//log.Printf("Replacing table name '%s' with sharded name '%s'\n", n.RangeVar.Relname, shardedName)
			n.RangeVar.Relname = shardedName
			n.RangeVar.Location = -1 // Force quoting
		}

	case *pg_query.Node_UpdateStmt:
		if newName, ok := tableMap[n.UpdateStmt.Relation.Relname]; ok {
			//log.Printf("Replacing table name '%s' with sharded name '%s' in UpdateStmt\n", n.UpdateStmt.Relation.Relname, newName)
			n.UpdateStmt.Relation.Relname = newName
			n.UpdateStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.UpdateStmt.WhereClause, tableMap)
		for _, target := range n.UpdateStmt.TargetList {
			replaceTableNames(target, tableMap)
		}
	case *pg_query.Node_DeleteStmt:
		if newName, ok := tableMap[n.DeleteStmt.Relation.Relname]; ok {
			//log.Printf("Replacing table name '%s' with sharded name '%s' in DeleteStmt\n", n.DeleteStmt.Relation.Relname, newName)
			n.DeleteStmt.Relation.Relname = newName
			n.DeleteStmt.Relation.Location = -1 // Force quoting
		}
		replaceTableNames(n.DeleteStmt.WhereClause, tableMap)
	case *pg_query.Node_SelectStmt:
		replaceSelectStmtTableName(n.SelectStmt, tableMap)
	case *pg_query.Node_JoinExpr:
		// Recursively process left and right arguments
		replaceTableNames(n.JoinExpr.Larg, tableMap)
		replaceTableNames(n.JoinExpr.Rarg, tableMap)
		replaceTableNames(n.JoinExpr.Quals, tableMap)
	case *pg_query.Node_SortBy:
		replaceTableNames(n.SortBy.Node, tableMap)
	case *pg_query.Node_ResTarget:
		// Process expressions in the SELECT list
		replaceTableNames(n.ResTarget.Val, tableMap)
		if n.ResTarget.Name != "" {
			n.ResTarget.Location = -1 // Force quoting of column aliases
		}
	case *pg_query.Node_FuncCall:
		// Process function arguments
		for _, arg := range n.FuncCall.Args {
			replaceTableNames(arg, tableMap)
		}
	case *pg_query.Node_ColumnRef:
		fields := n.ColumnRef.Fields
		n.ColumnRef.Location = -1 // Force quoting of column names
		if len(fields) >= 2 {
			// Check if the first field is a table name
			if stringNode, ok := fields[0].Node.(*pg_query.Node_String_); ok {
				originalTableName := stringNode.String_.Sval
				if newTableName, exists := tableMap[originalTableName]; exists {
					// Replace the table name with the sharded name
					//log.Printf("Replacing table name '%s' with sharded name '%s' in ColumnRef\n", originalTableName, newTableName)
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
	case *pg_query.Node_NamedArgExpr:
		replaceTableNames(n.NamedArgExpr.Arg, tableMap)
	case *pg_query.Node_Aggref:
		for _, arg := range n.Aggref.Args {
			replaceTableNames(arg, tableMap)
		}
	case *pg_query.Node_RangeSubselect:
		// Process the subquery in RangeSubselect
		if subselect, ok := n.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			replaceSelectStmtTableName(subselect.SelectStmt, tableMap)
		}
	case *pg_query.Node_SubLink:
		// Process subqueries in expressions
		if subselect, ok := n.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
			replaceSelectStmtTableName(subselect.SelectStmt, tableMap)
		}
	default:
		// Recursively process child nodes if any
		reflectValue := reflect.ValueOf(n)
		if reflectValue.Kind() == reflect.Ptr {
			reflectValue = reflectValue.Elem()
		}
		for i := 0; i < reflectValue.NumField(); i++ {
			field := reflectValue.Field(i)
			if field.Kind() == reflect.Ptr {
				if nodeField, ok := field.Interface().(*pg_query.Node); ok {
					replaceTableNames(nodeField, tableMap)
				}
			} else if field.Kind() == reflect.Slice {
				for j := 0; j < field.Len(); j++ {
					if nodeField, ok := field.Index(j).Interface().(*pg_query.Node); ok {
						replaceTableNames(nodeField, tableMap)
					}
				}
			}
		}
	}
}

func replaceSelectStmtTableName(selectStmt *pg_query.SelectStmt, tableMap map[string]string) {
	// Recursively process FROM clause and other relevant clauses
	for _, item := range selectStmt.FromClause {
		replaceTableNames(item, tableMap)
	}
	for _, target := range selectStmt.TargetList {
		replaceTableNames(target, tableMap)
	}
	replaceTableNames(selectStmt.WhereClause, tableMap)
	// ORDER BY clause
	for _, sortBy := range selectStmt.SortClause {
		replaceTableNames(sortBy, tableMap)
	}
	replaceTableNames(selectStmt.HavingClause, tableMap)
	for _, groupBy := range selectStmt.GroupClause {
		replaceTableNames(groupBy, tableMap)
	}

	// Handle RETURNING clause for INSERT statements
	if selectStmt.ValuesLists != nil {
		for _, returning := range selectStmt.ValuesLists {
			if resTarget, ok := returning.Node.(*pg_query.Node_ResTarget); ok {
				if colRef, ok := resTarget.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
					for _, field := range colRef.ColumnRef.Fields {
						if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
							originalTableName := stringNode.String_.Sval
							if newTableName, exists := tableMap[originalTableName]; exists {
								stringNode.String_.Sval = newTableName
							}
						}
					}
				}
			}
		}
	}
}

func (s *Sharding) extractValueFromExpr(expr *pg_query.Node, args []interface{}, config Config) (interface{}, error) {
	value, err := extractValueFromExpr(expr, args)
	if err != nil {
		return nil, err
	}

	// If there's a value converter, use it
	if config.ValueConverter != nil {
		return config.ValueConverter(value)
	}

	return value, nil
}

func extractValueFromExpr(expr *pg_query.Node, args []interface{}) (interface{}, error) {
	switch v := expr.Node.(type) {
	case *pg_query.Node_ParamRef:
		// PostgreSQL parameters are 1-based
		index := int(v.ParamRef.Number) - 1
		if index >= 0 && index < len(args) {
			return args[index], nil
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
			return int64(val.Ival.Ival), nil
		}
	case *pg_query.A_Const_Sval:
		// String value
		if val.Sval != nil {
			return val.Sval.Sval, nil
		}
	case *pg_query.A_Const_Fval:
		// Float value
		if val.Fval != nil {
			f, err := strconv.ParseFloat(val.Fval.Fval, 64)
			if err != nil {
				return nil, err
			}
			return f, nil
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

func traverseConditionForKey(shardingKey string, node *pg_query.Node, args []interface{}, knownKeys map[string]interface{}, aliasMap map[string]string) (keyFound bool, value interface{}, err error) {
	if node == nil {
		return false, nil, nil
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP && len(n.AExpr.Name) > 0 {
			opName := n.AExpr.Name[0].Node.(*pg_query.Node_String_).String_.Sval
			if opName == "=" {
				var leftColName, rightColName string
				var leftValue, rightValue interface{}
				var leftIsCol, rightIsCol bool

				// Left expression
				if colRef, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					leftColName = extractColumnName(colRef.ColumnRef, aliasMap)
					leftIsCol = true
				} else {
					leftValue, _ = extractValueFromExpr(n.AExpr.Lexpr, args)
				}

				// Right expression
				if colRef, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_ColumnRef); ok {
					rightColName = extractColumnName(colRef.ColumnRef, aliasMap)
					rightIsCol = true
				} else {
					rightValue, _ = extractValueFromExpr(n.AExpr.Rexpr, args)
				}

				// Store known values with both qualified and unqualified names
				if leftIsCol && !rightIsCol {
					storeKnownKey(knownKeys, leftColName, rightValue)
				}
				if rightIsCol && !leftIsCol {
					storeKnownKey(knownKeys, rightColName, leftValue)
				}

				// Record known keys for transitive inference
				if leftIsCol && rightIsCol {
					propagateKnownKeys(knownKeys, leftColName, rightColName)
				}

				// Check if we have found the sharding key
				if val, exists := knownKeys[shardingKey]; exists {
					return true, val, nil
				}
				//log.Printf("Processing AExpr: Operator '%s'", opName)
				//log.Printf("Left Column: '%s', Right Column: '%s'", leftColName, rightColName)
				//log.Printf("Known Keys: %v", knownKeys)
			}

		}

	case *pg_query.Node_BoolExpr:
		//log.Printf("Processing BoolExpr of type '%s'", n.BoolExpr.Boolop)
		for _, arg := range n.BoolExpr.Args {
			keyFound, value, err = traverseConditionForKey(shardingKey, arg, args, knownKeys, aliasMap)
			if keyFound || err != nil {
				return keyFound, value, err
			}
		}
	case *pg_query.Node_SubLink:
		// Handle subqueries in conditions
		if subselect, ok := n.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
			conditions := collectConditionsFromSelect(subselect.SelectStmt)
			for _, condition := range conditions {
				keyFound, value, err = traverseConditionForKey(shardingKey, condition, args, knownKeys, aliasMap)
				if keyFound || err != nil {
					return keyFound, value, err
				}
			}
		}

	}
	return false, nil, nil
}

func storeKnownKey(knownKeys map[string]interface{}, colName string, value interface{}) {
	knownKeys[colName] = value
	knownKeys[getColumnNameWithoutTable(colName)] = value
}

func propagateKnownKeys(knownKeys map[string]interface{}, colName1, colName2 string) {
	// If both columns have known values, do nothing
	if _, exists := knownKeys[colName1]; exists {
		if _, exists := knownKeys[colName2]; exists {
			return
		}
	}

	// If one column has a known value, assign it to the other
	if val, exists := knownKeys[colName1]; exists {
		storeKnownKey(knownKeys, colName2, val)
	} else if val, exists := knownKeys[colName2]; exists {
		storeKnownKey(knownKeys, colName1, val)
	} else {
		// Neither column has a known value; store their equivalence
		addColumnEquivalence(colName1, colName2)
	}
}

type EquivalenceClass struct {
	columns map[string]struct{}
}

var equivalenceClasses []EquivalenceClass

func addColumnEquivalence(colName1, colName2 string) {
	// Check if either column is already in an equivalence class
	var class *EquivalenceClass
	for i, eqClass := range equivalenceClasses {
		if _, exists := eqClass.columns[colName1]; exists {
			class = &equivalenceClasses[i]
			class.columns[colName2] = struct{}{}
			return
		}
		if _, exists := eqClass.columns[colName2]; exists {
			class = &equivalenceClasses[i]
			class.columns[colName1] = struct{}{}
			return
		}
	}
	// Neither column is in an equivalence class; create a new one
	newClass := EquivalenceClass{columns: map[string]struct{}{
		colName1: {},
		colName2: {},
	}}
	equivalenceClasses = append(equivalenceClasses, newClass)
}

func getColumnNameWithoutTable(columnName string) string {
	if idx := strings.LastIndex(columnName, "."); idx != -1 {
		return columnName[idx+1:]
	}
	return columnName
}

func extractColumnName(colRef *pg_query.ColumnRef, aliasMap map[string]string) string {
	var parts []string
	for i, field := range colRef.Fields {
		if stringNode, ok := field.Node.(*pg_query.Node_String_); ok {
			val := stringNode.String_.Sval
			// Resolve alias to table name for the first field (table alias)
			if i == 0 && len(colRef.Fields) > 1 {
				if realTable, exists := aliasMap[val]; exists {
					val = realTable
				}
			}
			parts = append(parts, val)
		}
	}
	return strings.Join(parts, ".")
}

func getSuffix(value any, id int64, keyFind bool, r Config) (suffix string, err error) {
	if keyFind {
		suffix, err = r.ShardingAlgorithm(value)
		if err != nil {
			log.Printf("Error in ShardingAlgorithm: %v\n", err)
			return
		}
		//log.Printf("Sharding key value: %v, Suffix: %s\n", value, suffix)
	} else {
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = fmt.Errorf("there is not sharding key and ShardingAlgorithmByPrimaryKey is not configured")
			log.Printf("Error: %v\n", err)
			return
		}
		suffix = r.ShardingAlgorithmByPrimaryKey(id)
		//log.Printf("Sharding by primary key: %d, Suffix: %s\n", id, suffix)
	}
	return
}

func collectTableAliases(selectStmt *pg_query.SelectStmt) map[string]string {
	aliasMap := make(map[string]string)
	for _, fromItem := range selectStmt.FromClause {
		collectAliasesFromNode(fromItem, aliasMap)
	}
	return aliasMap
}

func collectAliasesFromNode(node *pg_query.Node, aliasMap map[string]string) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		tableName := n.RangeVar.Relname
		alias := ""
		if n.RangeVar.Alias != nil {
			alias = n.RangeVar.Alias.Aliasname
			aliasMap[alias] = tableName
		} else {
			aliasMap[tableName] = tableName
		}
	case *pg_query.Node_JoinExpr:
		collectAliasesFromNode(n.JoinExpr.Larg, aliasMap)
		collectAliasesFromNode(n.JoinExpr.Rarg, aliasMap)
	}
}

func collectAliasesFromJoin(joinExpr *pg_query.JoinExpr) map[string]string {
	aliasMap := make(map[string]string)
	if joinExpr.Larg != nil {
		mergeMaps(aliasMap, collectAliasesFromExpr(joinExpr.Larg))
	}
	if joinExpr.Rarg != nil {
		mergeMaps(aliasMap, collectAliasesFromExpr(joinExpr.Rarg))
	}
	return aliasMap
}

func collectAliasesFromExpr(expr *pg_query.Node) map[string]string {
	switch n := expr.Node.(type) {
	case *pg_query.Node_RangeVar:
		tableName := n.RangeVar.Relname
		var alias string
		if n.RangeVar.Alias != nil {
			alias = n.RangeVar.Alias.Aliasname
			return map[string]string{alias: tableName}
		}
		return map[string]string{tableName: tableName}
	case *pg_query.Node_JoinExpr:
		return collectAliasesFromJoin(n.JoinExpr)
	}
	return nil
}

func mergeMaps(dest, src map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
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
