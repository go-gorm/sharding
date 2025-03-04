package sharding

import "strings"

type DatabaseEngine int

const (
	EngineUnknown DatabaseEngine = iota
	EnginePostgreSQL
	EngineMySQL
	EngineSQLite
)

func (s *Sharding) setDatabaseEngine() {
	switch strings.ToLower(s.DB.Dialector.Name()) {
	case "postgres":
		s._config.engine = EnginePostgreSQL
	case "mysql":
		s._config.engine = EngineMySQL
	case "sqlite":
		s._config.engine = EngineSQLite
	default:
		s._config.engine = EngineUnknown
	}
}
