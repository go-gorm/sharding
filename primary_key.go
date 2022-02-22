package sharding

import "fmt"

const (
	// Use Snowflake primary key generator
	PKSnowflake = iota
	// Use PostgreSQL sequence primary key generator
	PKPGSequence
	// Use custom primary key generator
	PKCustom
)

func (s *Sharding) genSnowflakeKey(index int64) int64 {
	return s.snowflakeNodes[index].Generate().Int64()
}

func (s *Sharding) genPostgreSQLSequenceKey(tableName string, index int64) int64 {
	var id int64
	err := s.DB.Raw("SELECT nextval('" + pgSeqName(tableName) + "')").Scan(&id).Error
	if err != nil {
		panic(err)
	}
	return id
}

func (s *Sharding) createPostgreSQLSequenceKeyIfNotExist(tableName string) error {
	return s.DB.Exec(`CREATE SEQUENCE IF NOT EXISTS "` + pgSeqName(tableName) + `" START 1`).Error
}

func pgSeqName(table string) string {
	return fmt.Sprintf("gorm_sharding_%s_id_seq", table)
}
