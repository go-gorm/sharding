package sharding

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"hash/fnv"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func truncateTables(db *gorm.DB, tables ...string) {
	for _, table := range tables {
		db.Exec(fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", table))
	}
}

func toDialect(sql string) string {
	if os.Getenv("DIALECTOR") == "mysql" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
		sql = strings.ReplaceAll(sql, " RETURNING `id`", "")
	} else if os.Getenv("DIALECTOR") == "mariadb" {
		sql = strings.ReplaceAll(sql, `"`, "`")
		r := regexp.MustCompile(`\$([0-9]+)`)
		sql = r.ReplaceAllString(sql, "?")
	}
	return sql
}

// skip $sfid compare
func assertSfidQueryResult(t *testing.T, expected, lastQuery string) {
	t.Helper()

	node, _ := snowflake.NewNode(0)
	sfid := node.Generate().Int64()
	sfidLen := len(strconv.Itoa(int(sfid)))
	re := regexp.MustCompile(`\$sfid`)

	for {
		match := re.FindStringIndex(expected)
		if len(match) == 0 {
			break
		}

		start := match[0]
		end := match[1]

		if len(lastQuery) < start+sfidLen {
			break
		}

		sfid := lastQuery[start : start+sfidLen]
		expected = expected[:start] + sfid + expected[end:]
	}

	assert.Equal(t, toDialect(expected), lastQuery)
}

func mysqlDialector() bool {
	return os.Getenv("DIALECTOR") == "mysql" || os.Getenv("DIALECTOR") == "mariadb"
}

func mariadbDialector() bool {
	return os.Getenv("DIALECTOR") == "mariadb"
}

func shardingHasher32Algorithm(columnValue any) (suffix string, err error) {
	str, ok := columnValue.(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", columnValue)
	}

	// Use a default value if name is empty
	if str == "" {
		str = "default"
	}

	// Create a new FNV-1a 32-bit hash.
	hasher := fnv.New32a()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", fmt.Errorf("failed to write to hasher: %v", err)
	}
	hashValue := hasher.Sum32()

	// Assume we have 32 shards; adjust as needed.
	suffix = fmt.Sprintf("_%d", hashValue%32)
	return suffix, nil
}

// Function to safely get value from pointer types
func dereferenceValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		return v.Elem().Interface()
	}
	return value
}

func assertQueryResult(t *testing.T, expected string, middleware *Sharding) {
	t.Helper()
	normalize := func(query string) string {
		// Remove quotes around identifiers
		re := regexp.MustCompile(`"(\w+)"`)
		query = re.ReplaceAllString(query, `$1`)
		// Replace parameter numbers with a placeholder
		query = regexp.MustCompile(`\$\d+`).ReplaceAllString(query, `$?`)
		// Normalize whitespace
		query = strings.TrimSpace(query)
		query = regexp.MustCompile(`\s+`).ReplaceAllString(query, ` `)
		return query
	}
	normalizedExpected := normalize(toDialect(expected))
	normalizedActual := normalize(middleware.LastQuery())
	if normalizedExpected != normalizedActual {
		t.Errorf("\nExpected:\n%s\nActual:\n%s", normalizedExpected, normalizedActual)
	}
}

// NameShardingConfig creates a Config with name-based sharding algorithm
func NameShardingConfig(numberOfShards uint) Config {
	config := Config{
		DoubleWrite:    true,
		ShardingKey:    "name",
		NumberOfShards: numberOfShards,
		ShardingAlgorithm: func(columnValue any) (suffix string, err error) {
			fmt.Println("columnValue: ", columnValue)
			str, ok := columnValue.(string)
			if !ok {
				return "", fmt.Errorf("expected string, got %T", columnValue)
			}

			// Use a default value if name is empty
			if str == "" {
				str = "default"
			}

			// Create a new FNV-1a 32-bit hash.
			hasher := fnv.New32a()
			_, err = hasher.Write([]byte(str))
			if err != nil {
				return "", fmt.Errorf("failed to write to hasher: %v", err)
			}
			hashValue := hasher.Sum32()

			// Use modulo to determine the shard
			suffix = fmt.Sprintf("_%d", hashValue%uint32(numberOfShards))
			fmt.Println("suffix: ", suffix)
			return suffix, nil
		},
	}
	return config
}
