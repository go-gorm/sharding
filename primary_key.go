package sharding

import "fmt"

const (
	// Use Snowflake primary key generator
	PKSnowflake = iota
	// Use PostgreSQL sequence primary key generator
	PKPGSequence
	// Use MySQL sequence primary key generator
	PKMySQLSequence
	// Use custom primary key generator
	PKCustom
)

func (s *Sharding) genSnowflakeKey(index int64) int64 {
	return s.snowflakeNodes[index].Generate().Int64()
}

// PostgreSQL sequence

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

// MySQL Sequence

func (s *Sharding) genMySQLSequenceKey(tableName string, index int64) int64 {
	var id int64
	err := s.DB.Exec("UPDATE `" + mySQLSeqName(tableName) + "` SET id = LAST_INSERT_ID(id + 1)").Error
	if err != nil {
		panic(err)
	}
	err = s.DB.Raw("SELECT LAST_INSERT_ID()").Scan(&id).Error
	if err != nil {
		panic(err)
	}
	return id
}

func (s *Sharding) createMySQLSequenceKeyIfNotExist(tableName string) error {
	stmt := s.DB.Exec("CREATE TABLE IF NOT EXISTS `" + mySQLSeqName(tableName) + "` (id INT NOT NULL)")
	if stmt.Error != nil {
		return fmt.Errorf("failed to create sequence table: %w", stmt.Error)
	}
	var count int
	err := s.DB.Raw("SELECT COUNT(*) FROM `" + mySQLSeqName(tableName) + "`").Scan(&count).Error
	if err != nil {
		return fmt.Errorf("failed to check sequence table data: %w", err)
	}

	// Insert initial values only when there is no data in the table
	if count == 0 {
		stmt = s.DB.Exec("INSERT INTO `" + mySQLSeqName(tableName) + "` VALUES (0)")
		if stmt.Error != nil {
			return fmt.Errorf("failed to insert into sequence table: %w", stmt.Error)
		}
	}
	return nil
}

func mySQLSeqName(table string) string {
	return fmt.Sprintf("gorm_sharding_%s_id_seq", table)
}
