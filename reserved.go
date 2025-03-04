package sharding

import (
	"regexp"
	"strings"
)

var validPostgresIdent = regexp.MustCompile(`^[a-z_][a-z0-9_$]*$`)

func (s *Sharding) quoteIdent(ident string) string {
	if s.IsReservedKeyword(ident) {
		return s.quote(ident)
	}
	// SQL identifiers and key words must begin with a letter (a-z, but also
	// letters with diacritical marks and non-Latin letters) or an underscore
	// (_). Subsequent characters in an identifier or key word can be letters,
	// underscores, digits (0-9), or dollar signs ($).
	//
	// https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	if s._config.engine == EnginePostgreSQL {
		// camelCase means the column is also camelCase
		if strings.ToLower(ident) != ident {
			return s.quote(ident)
		}
		if !validPostgresIdent.MatchString(strings.ToLower(ident)) {
			return s.quote(ident)
		}
	}
	return ident
}

func (s *Sharding) quote(x string) string {
	switch s._config.engine {
	case EngineMySQL:
		return "`" + x + "`"
	default:
		return "\"" + x + "\""
	}
}

// https://www.postgresql.org/docs/current/sql-keywords-appendix.html
func (s *Sharding) IsReservedKeyword(str string) bool {
	switch strings.ToLower(str) {
	case "all":
	case "analyse":
	case "analyze":
	case "and":
	case "any":
	case "array":
	case "as":
	case "asc":
	case "asymmetric":
	case "authorization":
	case "binary":
	case "both":
	case "case":
	case "cast":
	case "check":
	case "collate":
	case "collation":
	case "column":
	case "concurrently":
	case "constraint":
	case "create":
	case "cross":
	case "current_catalog":
	case "current_date":
	case "current_role":
	case "current_schema":
	case "current_time":
	case "current_timestamp":
	case "current_user":
	case "default":
	case "deferrable":
	case "desc":
	case "distinct":
	case "do":
	case "else":
	case "end":
	case "except":
	case "false":
	case "fetch":
	case "for":
	case "foreign":
	case "freeze":
	case "from":
	case "full":
	case "grant":
	case "group":
	case "having":
	case "ilike":
	case "in":
	case "initially":
	case "inner":
	case "intersect":
	case "into":
	case "is":
	case "isnull":
	case "join":
	case "lateral":
	case "leading":
	case "left":
	case "like":
	case "limit":
	case "localtime":
	case "localtimestamp":
	case "natural":
	case "not":
	case "notnull":
	case "null":
	case "offset":
	case "on":
	case "only":
	case "or":
	case "order":
	case "outer":
	case "overlaps":
	case "placing":
	case "primary":
	case "references":
	case "returning":
	case "right":
	case "select":
	case "session_user":
	case "similar":
	case "some":
	case "symmetric":
	case "table":
	case "tablesample":
	case "then":
	case "to":
	case "trailing":
	case "true":
	case "union":
	case "unique":
	case "user":
	case "using":
	case "variadic":
	case "verbose":
	case "when":
	case "where":
	case "window":
	case "with":
	default:
		return false
	}
	return true
}
