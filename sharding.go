package sharding

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/longbridgeapp/sqlparser"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
)

type Sharding struct {
	*gorm.DB
	ConnPool *ConnPool
	configs  map[string]Config
	querys   sync.Map
}

//  Config specifies the configuration for sharding.
type Config struct {
	// When DoubleWrite enabled, data will double write to both main table and sharding table.
	DoubleWrite bool

	// ShardingKey specifies the table column you want to used for sharding the table rows.
	// For example, for a product order table, you may want to split the rows by `user_id`.
	ShardingKey string

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
	// For example, this function use the LongKey library to generate the suffix.
	//
	// 	func(id int64) (suffix string) {
	//		return fmt.Sprintf("_%02d", longkey.TableIdx(id))
	//	}
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)

	// PrimaryKeyGenerate specifies a function to generate the primary key.
	// Used only when insert and the record does not contains an id field.
	// We recommend you use the
	// [LongKey](https://github.com/longbridgeapp/longkey) component,
	// it is a distributed primary key generator.
	// When use auto-increment like generator, the tableIdx argument could ignored.
	//
	// For example, this function use the LongKey library to generate the primary key.
	//
	// 	func(tableIdx int64) int64 {
	//		return longkey.Next(tableIdx)
	//	}
	PrimaryKeyGenerate func(tableIdx int64) int64
}

func Register(config Config, tables ...interface{}) *Sharding {
	return (&Sharding{}).Register(config, tables...)
}

func (s *Sharding) Register(config Config, tables ...interface{}) *Sharding {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}
	for _, table := range tables {
		if t, ok := table.(string); ok {
			s.configs[t] = config
		} else if t, ok := table.(schema.Tabler); ok {
			s.configs[t.TableName()] = config
		} else {
			panic("invalid config, use string table name or schema.Tabler")
		}
	}

	return s
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
	return nil
}

// resolve split the old query to full table query and sharding table query
func (s *Sharding) resolve(query string, args ...interface{}) (ftQuery, stQuery, tableName string, err error) {
	ftQuery = query
	stQuery = query
	if len(s.configs) == 0 {
		return
	}

	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return ftQuery, stQuery, tableName, nil
	}

	var table *sqlparser.TableName
	var condition sqlparser.Expr
	var isInsert bool
	var insertNames []*sqlparser.Ident
	var insertValues []sqlparser.Expr

	switch stmt := expr.(type) {
	case *sqlparser.SelectStatement:
		tbl, ok := stmt.FromItems.(*sqlparser.TableName)
		if !ok {
			return
		}
		if stmt.Hint != nil && stmt.Hint.Value == "nosharding" {
			return
		}
		table = tbl
		condition = stmt.Condition

	case *sqlparser.InsertStatement:
		table = stmt.TableName
		isInsert = true
		insertNames = stmt.ColumnNames
		insertValues = stmt.Expressions[0].Exprs
	case *sqlparser.UpdateStatement:
		condition = stmt.Condition
		table = stmt.TableName
	case *sqlparser.DeleteStatement:
		condition = stmt.Condition
		table = stmt.TableName
	default:
		return ftQuery, stQuery, "", sqlparser.ErrNotImplemented
	}

	tableName = table.Name.Name
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
		value, id, keyFind, err = s.nonInsertValue(r.ShardingKey, condition, args...)
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

	newTable := &sqlparser.TableName{Name: &sqlparser.Ident{Name: tableName + suffix}}

	fillID := true
	if isInsert {
		for _, name := range insertNames {
			if name.Name == "id" {
				fillID = false
				break
			}
		}
		if fillID {
			tblIdx, err := strconv.Atoi(strings.Replace(suffix, "_", "", 1))
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}
			id := r.PrimaryKeyGenerate(int64(tblIdx))
			insertNames = append(insertNames, &sqlparser.Ident{Name: "id"})
			insertValues = append(insertValues, &sqlparser.NumberLit{Value: strconv.FormatInt(id, 10)})
		}
	}

	switch stmt := expr.(type) {
	case *sqlparser.InsertStatement:
		if fillID {
			stmt.ColumnNames = insertNames
			stmt.Expressions[0].Exprs = insertValues
		}
		ftQuery = stmt.String()
		stmt.TableName = newTable
		stQuery = stmt.String()
	case *sqlparser.SelectStatement:
		ftQuery = stmt.String()
		stmt.FromItems = newTable
		stmt.OrderBy = replaceOrderByTableName(stmt.OrderBy, tableName, newTable.Name.Name)
		stQuery = stmt.String()
	case *sqlparser.UpdateStatement:
		ftQuery = stmt.String()
		stmt.TableName = newTable
		stQuery = stmt.String()
	case *sqlparser.DeleteStatement:
		ftQuery = stmt.String()
		stmt.TableName = newTable
		stQuery = stmt.String()
	}

	return
}

func (s *Sharding) insertValue(key string, names []*sqlparser.Ident, exprs []sqlparser.Expr, args ...interface{}) (value interface{}, id int64, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, 0, keyFind, errors.New("column names and expressions mismatch")
	}

	for i, name := range names {
		if name.Name == key {
			switch expr := exprs[i].(type) {
			case *sqlparser.BindExpr:
				value, err = getBindValue(expr.Name, args)
				if err != nil {
					return nil, 0, keyFind, err
				}
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

func (s *Sharding) nonInsertValue(key string, condition sqlparser.Expr, args ...interface{}) (value interface{}, id int64, keyFind bool, err error) {
	err = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if n, ok := node.(*sqlparser.BinaryExpr); ok {
			if x, ok := n.X.(*sqlparser.Ident); ok {
				if x.Name == key && n.Op == sqlparser.EQ {
					keyFind = true
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						value, err = getBindValue(expr.Name, args)
						if err != nil {
							return err
						}
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
						v, err := getBindValue(expr.Name, args)
						if err != nil {
							return err
						}
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

func getBindValue(value interface{}, args []interface{}) (interface{}, error) {
	bindPos := strings.Replace(value.(string), "$", "", 1)
	pos, err := strconv.Atoi(bindPos)
	if err != nil {
		return nil, err
	}
	return args[pos-1], nil
}
