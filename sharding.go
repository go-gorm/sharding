package sharding

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/test_driver"
	"gorm.io/gorm"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
)

type Sharding struct {
	*gorm.DB
	ConnPool       *ConnPool
	configs        map[string]Config
	querys         sync.Map
	snowflakeNodes []*snowflake.Node

	_config Config
	_tables []interface{}
}

//  Config specifies the configuration for sharding.
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
	// 	func(value interface{}) (suffix string, err error) {
	//		if uid, ok := value.(int64);ok {
	//			return fmt.Sprintf("_%02d", user_id % 64), nil
	//		}
	//		return "", errors.New("invalid user_id")
	// 	}
	ShardingAlgorithm func(columnValue interface{}) (suffix string, err error)

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
	//
	// 	func(tableIdx int64) int64 {
	//		return nodes[tableIdx].Generate().Int64()
	//	}
	PrimaryKeyGeneratorFn func(tableIdx int64) int64
}

func Register(config Config, tables ...interface{}) *Sharding {
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
			// stmt := &gorm.Statement{DB: s.DB}
			stmt := s.DB.Statement
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
		} else if c.PrimaryKeyGenerator == PKCustom {
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when use PKCustom")
			}
		} else {
			return errors.New("PrimaryKeyGenerator can only be one of PKSnowflake, PKPGSequence and PKCustom")
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
			c.ShardingAlgorithm = func(value interface{}) (suffix string, err error) {
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
	s.DB = db
	s.registerConnPool(db)

	for t, c := range s.configs {
		if c.PrimaryKeyGenerator == PKPGSequence {
			err := s.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + pgSeqName(t)).Error
			if err != nil {
				return fmt.Errorf("init postgresql sequence error, %w", err)
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

// resolve split the old query to full table query and sharding table query
func (s *Sharding) resolve(query string, args ...interface{}) (ftQuery, stQuery, tableName string, err error) {
	ftQuery = query
	stQuery = query
	if len(s.configs) == 0 {
		return
	}

	var leftTable *ast.TableName
	var tableNames []*ast.TableName
	var condition ast.ExprNode
	var isInsert bool
	var insertNames []*ast.ColumnName
	var insertValues []ast.ExprNode

	p := parser.New()
	stmtNodes, _, err := p.Parse(query, "", "")
	if err != nil {
		return ftQuery, stQuery, tableName, nil
	}
	switch stmt := stmtNodes[0].(type) {
	case *ast.SelectStmt:
		var ok bool
		tableNames, ok = collectSelectTableName(stmt.From.TableRefs)
		if !ok {
			return
		}
		if len(tableNames) == 1 {
			leftTable = tableNames[0]
		}
		condition = stmt.Where
	case *ast.InsertStmt:
		isInsert = true
		tblSource, ok := stmt.Table.TableRefs.Left.(*ast.TableSource)
		if !ok {
			return
		}
		leftTable, ok = tblSource.Source.(*ast.TableName)
		if !ok {
			return
		}

		if len(stmt.Lists) != 1 {
			return
		}
		insertValues = stmt.Lists[0]
		insertNames = stmt.Columns

	case *ast.UpdateStmt:
		tblSource, ok := stmt.TableRefs.TableRefs.Left.(*ast.TableSource)
		if !ok {
			return
		}
		leftTable, ok = tblSource.Source.(*ast.TableName)
		if !ok {
			return
		}

		condition = stmt.Where
		if len(stmt.List) > 0 {
			placeHolderCnt := calculateArgsInUpdate(stmt.List)
			args = args[placeHolderCnt:]
		}
	case *ast.DeleteStmt:
		tblSource, ok := stmt.TableRefs.TableRefs.Left.(*ast.TableSource)
		if !ok {
			return
		}
		leftTable, ok = tblSource.Source.(*ast.TableName)
		if !ok {
			return
		}
		condition = stmt.Where
	}

	if len(tableNames) > 1 {
		// select with join
		var buf bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		var needReplace bool
		for _, tab := range tableNames {
			tableName = tab.Name.String()
			r, ok := s.configs[tableName]
			if !ok {
				continue
			}

			needReplace = true

			var value interface{}
			var keyFind bool
			value, _, keyFind, err = s.nonInsertValue(r.ShardingKey, tableName, condition, args...)
			if err != nil {
				return
			}

			var suffix string

			if keyFind {
				suffix, err = r.ShardingAlgorithm(value)
				if err != nil {
					return
				}
			}

			newTableName := tableName + suffix

			switch stmt := stmtNodes[0].(type) {
			case *ast.SelectStmt:

				stmt.From.TableRefs = replaceJoinByTableName(stmt.From.TableRefs, tableName, newTableName)
				if stmt.OrderBy != nil {
					stmt.OrderBy.Items = replaceOrderByTableName(stmt.OrderBy.Items, tableName, newTableName)
				}
				if stmt.Fields != nil {
					stmt.Fields.Fields = replaceSelectFieldByTableName(stmt.Fields.Fields, tableName, newTableName)
				}
				stmt.Where = replaceWhereByTableName(stmt.Where, tableName, newTableName)
			default:
				return
			}
		}
		if !needReplace {
			return
		}
		ftQuery = stmtNodes[0].Text()
		err = stmtNodes[0].Restore(restoreCtx)
		if err == nil {
			stQuery = buf.String()
		}
	} else {
		if leftTable == nil {
			return
		}
		tableName = leftTable.Name.String()
		r, ok := s.configs[tableName]
		if !ok {
			return
		}

		var value interface{}
		var id int64
		var keyFind bool
		if isInsert {
			value, id, keyFind, err = s.insertValue(r.ShardingKey, insertNames, insertValues, args...)
			if err != nil {
				return
			}
		} else {
			value, id, keyFind, err = s.nonInsertValue(r.ShardingKey, tableName, condition, args...)
			if err != nil {
				return
			}
		}

		var suffix string

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

		newTableName := tableName + suffix

		var buf bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)

		switch stmt := stmtNodes[0].(type) {
		case *ast.InsertStmt:
			ftQuery = stmt.Text()
			stmt.Table.TableRefs.Left = replaceTableSourceByTableName(stmt.Table.TableRefs.Left, tableName, newTableName)
			err = stmt.Restore(restoreCtx)
		case *ast.SelectStmt:
			ftQuery = stmt.Text()
			stmt.From.TableRefs.Left = replaceTableSourceByTableName(stmt.From.TableRefs.Left, tableName, newTableName)
			if stmt.OrderBy != nil {
				stmt.OrderBy.Items = replaceOrderByTableName(stmt.OrderBy.Items, tableName, newTableName)
			}
			if stmt.Fields != nil {
				stmt.Fields.Fields = replaceSelectFieldByTableName(stmt.Fields.Fields, tableName, newTableName)
			}
			stmt.Where = replaceWhereByTableName(stmt.Where, tableName, newTableName)
			err = stmt.Restore(restoreCtx)
		case *ast.UpdateStmt:
			ftQuery = stmt.Text()
			stmt.TableRefs.TableRefs.Left = replaceTableSourceByTableName(stmt.TableRefs.TableRefs.Left, tableName, newTableName)
			stmt.Where = replaceWhereByTableName(stmt.Where, tableName, newTableName)
			err = stmt.Restore(restoreCtx)
		case *ast.DeleteStmt:
			ftQuery = stmt.Text()
			stmt.TableRefs.TableRefs.Left = replaceTableSourceByTableName(stmt.TableRefs.TableRefs.Left, tableName, newTableName)
			stmt.Where = replaceWhereByTableName(stmt.Where, tableName, newTableName)
			err = stmt.Restore(restoreCtx)
		}
		if err == nil {
			stQuery = buf.String()
		}
		return
	}
	return
}

func (s *Sharding) insertValue(key string, names []*ast.ColumnName, exprs []ast.ExprNode, args ...interface{}) (value interface{}, id int64, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, 0, keyFind, errors.New("column names and expressions mismatch")
	}
	inserts := make([]insertField, 0, len(names))
	paramIdx := 0
	for i := range names {
		insert := insertField{
			columnName: names[i].Name.L,
			paramIndex: -1,
		}

		switch exprs[i].(type) {
		case *test_driver.ParamMarkerExpr:
			insert.paramIndex = paramIdx
			paramIdx++
		case *test_driver.ValueExpr:
			insert.value = exprs[i].(*test_driver.ValueExpr).GetValue()
		}

		inserts = append(inserts, insert)

	}

	for _, insert := range inserts {
		if insert.columnName == key {
			if insert.paramIndex == -1 {
				keyFind = true
				value = insert.value
			} else if insert.paramIndex < len(args) {
				keyFind = true
				value = args[insert.paramIndex]
			} else {
				return
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

func (s *Sharding) nonInsertValue(key string, tableName string, condition ast.ExprNode, args ...interface{}) (value interface{}, id int64, keyFind bool, err error) {
	v := &conditionCol{}
	condition.Accept(v)

	for _, whereCon := range v.conditions {
		if whereCon.columnName == key {
			if whereCon.tableName != "" && whereCon.tableName != tableName {
				continue
			}
			if whereCon.paramIndex == -1 {
				keyFind = true
				value = whereCon.value
			} else if whereCon.paramIndex < len(args) {
				keyFind = true
				value = args[whereCon.paramIndex]
			} else {
				return
			}
		}
	}

	if !keyFind {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

type conditionCol struct {
	conditions []whereField
	paramIndex int
}

type insertField struct {
	columnName string
	paramIndex int
	value      interface{}
}

type whereField struct {
	tableName  string
	columnName string
	paramIndex int
	value      interface{}
}

// 收集所有 EQ 操作的 Where 条件
func (v *conditionCol) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.BinaryOperationExpr); ok {
		if col, ok := node.L.(*ast.ColumnNameExpr); ok {
			w := whereField{
				tableName:  col.Name.Table.L,
				columnName: col.Name.Name.L,
				paramIndex: -1,
			}

			if _, ok := node.R.(*test_driver.ParamMarkerExpr); ok {
				w.paramIndex = v.paramIndex
				v.paramIndex++
			} else if vNode, ok := node.R.(*test_driver.ValueExpr); ok {
				w.value = vNode.Datum.GetValue()
			}

			if node.Op == opcode.EQ {
				v.conditions = append(v.conditions, w)
			}
		}
	}
	return in, false
}

func (v *conditionCol) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func replaceOrderByTableName(orderBy []*ast.ByItem, oldName, newName string) []*ast.ByItem {
	for i := range orderBy {
		if _, ok := orderBy[i].Expr.(*ast.ColumnNameExpr); ok {
			replaceColumnNameExpr(orderBy[i].Expr.(*ast.ColumnNameExpr), oldName, newName)
		}
	}

	return orderBy
}

func replaceWhereByTableName(expr ast.ExprNode, oldName, newName string) ast.ExprNode {
	// DFS L & R until L is ast.ColumnNameExpr
	n, ok := expr.(*ast.BinaryOperationExpr)
	if ok {
		replaceBinaryOperationExpr(n, oldName, newName)
		return n
	}
	return expr
}

func replaceBinaryOperationExpr(expr *ast.BinaryOperationExpr, oldName, newName string) {
	switch expr.L.(type) {
	case *ast.BinaryOperationExpr:
		// DFS
		replaceBinaryOperationExpr(expr.L.(*ast.BinaryOperationExpr), oldName, newName)
	case *ast.ColumnNameExpr:
		replaceColumnNameExpr(expr.L.(*ast.ColumnNameExpr), oldName, newName)
	}

	switch expr.R.(type) {
	case *ast.BinaryOperationExpr:
		// DFS
		replaceBinaryOperationExpr(expr.R.(*ast.BinaryOperationExpr), oldName, newName)
	}
}

func replaceColumnNameExpr(expr *ast.ColumnNameExpr, oldName, newName string) {
	if expr.Name.Table.L == oldName {
		expr.Name.Table.L = newName
	}
	if strings.ToLower(expr.Name.Table.O) == oldName {
		expr.Name.Table.O = newName
	}
}

func replaceTableSourceByTableName(expr ast.ResultSetNode, oldName, newName string) ast.ResultSetNode {
	n, ok := expr.(*ast.TableSource)
	if ok {
		tblName, ok := n.Source.(*ast.TableName)
		if ok {
			if tblName.Name.L == oldName {
				tblName.Name.L = newName
			}
			if strings.ToLower(tblName.Name.O) == oldName {
				tblName.Name.O = newName
			}
			return n
		}
	}

	return expr
}

func replaceJoinByTableName(stmt *ast.Join, oldName, newName string) *ast.Join {
	switch stmt.Left.(type) {
	case *ast.Join:
		stmt.Left = replaceJoinByTableName(stmt.Left.(*ast.Join), oldName, newName)
	case *ast.TableSource:
		stmt.Left = replaceTableSourceByTableName(stmt.Left, oldName, newName)
	}

	switch stmt.Right.(type) {
	case *ast.TableSource:
		stmt.Right = replaceTableSourceByTableName(stmt.Right, oldName, newName)
	}

	if on, ok := stmt.On.Expr.(*ast.BinaryOperationExpr); ok {
		if _, ok = on.L.(*ast.ColumnNameExpr); ok {
			replaceColumnNameExpr(on.L.(*ast.ColumnNameExpr), oldName, newName)
		}
		if _, ok = on.R.(*ast.ColumnNameExpr); ok {
			replaceColumnNameExpr(on.R.(*ast.ColumnNameExpr), oldName, newName)
		}
	}

	return stmt
}

func replaceSelectFieldByTableName(stmt []*ast.SelectField, oldName, newName string) []*ast.SelectField {
	if len(stmt) < 1 {
		return stmt
	}

	for _, n := range stmt {
		_, ok := n.Expr.(*ast.ColumnNameExpr)
		if ok {
			replaceColumnNameExpr(n.Expr.(*ast.ColumnNameExpr), oldName, newName)
		}
	}
	return stmt
}

func calculateArgsInUpdate(assignments []*ast.Assignment) int {
	cnt := 0
	for _, ass := range assignments {
		if _, ok := ass.Expr.(*test_driver.ParamMarkerExpr); ok {
			cnt++
		}
	}
	return cnt
}

func collectSelectTableName(stmt *ast.Join) ([]*ast.TableName, bool) {
	var result []*ast.TableName
	switch stmt.Left.(type) {
	case *ast.Join:
		t, ok := collectSelectTableName(stmt.Left.(*ast.Join))
		if ok == true {
			result = append(result, t...)
		}
	case *ast.TableSource:
		n, ok := stmt.Left.(*ast.TableSource).Source.(*ast.TableName)
		if ok {
			result = append(result, n)
		}
	default:
		return nil, false
	}

	switch stmt.Right.(type) {
	case *ast.TableSource:
		n, ok := stmt.Right.(*ast.TableSource).Source.(*ast.TableName)
		if ok {
			result = append(result, n)
		}
	default:
		return nil, false
	}
	return result, true
}
