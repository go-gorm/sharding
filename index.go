package sharding

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

// GlobalIndexRegistry holds all the global indices
type GlobalIndexRegistry struct {
	sync.RWMutex
	indices map[string]map[string]*GlobalIndex // table -> column -> index
}

// Add adds a global index to the registry
func (r *GlobalIndexRegistry) Add(tableName, columnName string, index *GlobalIndex) {
	r.Lock()
	defer r.Unlock()

	if r.indices == nil {
		r.indices = make(map[string]map[string]*GlobalIndex)
	}

	if _, ok := r.indices[tableName]; !ok {
		r.indices[tableName] = make(map[string]*GlobalIndex)
	}

	r.indices[tableName][columnName] = index
}

// Get retrieves a global index from the registry
func (r *GlobalIndexRegistry) Get(tableName, columnName string) *GlobalIndex {
	r.RLock()
	defer r.RUnlock()

	if r.indices == nil {
		return nil
	}

	tableIndices, ok := r.indices[tableName]
	if !ok {
		return nil
	}

	return tableIndices[columnName]
}

// GlobalIndexRecord represents a record in the global index table
type GlobalIndexRecord struct {
	ID          uint64 `gorm:"primaryKey"`
	TableSuffix string `gorm:"index:idx_table_suffix"`
	RecordID    int64  `gorm:"index:idx_record_id"`
	IndexValue  string `gorm:"index:idx_index_value"`
	IndexColumn string `gorm:"index:idx_index_column"`
	CreatedAt   int64
	UpdatedAt   int64
}

// TableName returns the name of the global index table
func (GlobalIndexRecord) TableName() string {
	return "global_index"
}

// GlobalIndexStats represents statistics about the global index
type GlobalIndexStats struct {
	TableName    string
	IndexColumns []string
	RecordCount  int64
	LastUpdated  time.Time
	Cardinality  map[string]int64 // column name -> count of unique values
}

// register callbacks to maintain the index
// register callbacks to maintain the index
func (s *Sharding) registerIndexCallbacks(gi *GlobalIndex, db *gorm.DB) {
	// Ensure we only register each callback once by checking if it already exists
	callbacks := db.Callback()

	// Create callback
	if callbacks.Create().Get("global_index:create") == nil {
		callbacks.Create().After("gorm:create").Register("global_index:create", func(db *gorm.DB) {
			// Skip if this isn't being called from a sharded table operation
			if db.Statement.Schema == nil || !strings.HasPrefix(db.Statement.Table, gi.TableName) {
				return
			}

			// Call our index update function
			gi.afterCreate(db)
		})
	}

	// Delete callback
	if callbacks.Delete().Get("global_index:delete") == nil {
		callbacks.Delete().After("gorm:delete").Register("global_index:delete", func(db *gorm.DB) {
			// Skip if this isn't being called from a sharded table operation
			if db.Statement.Schema == nil || !strings.HasPrefix(db.Statement.Table, gi.TableName) {
				return
			}

			// Call our index update function
			gi.afterDelete(db)
		})
	}

	// Update callback
	if callbacks.Update().Get("global_index:update") == nil {
		callbacks.Update().After("gorm:update").Register("global_index:update", func(db *gorm.DB) {
			// Skip if this isn't being called from a sharded table operation
			if db.Statement.Schema == nil || !strings.HasPrefix(db.Statement.Table, gi.TableName) {
				return
			}

			// Call our index update function
			gi.afterUpdate(db)
		})
	}
}

// We need to add globalIndices field to the Sharding struct
// This is required to register and track global indices

// NewGlobalIndex creates a new global index for a sharded table
//func (s *Sharding) NewGlobalIndex(tableName string, indexColumns []string) (*GlobalIndex, error) {
//	// Check if the table is configured for sharding
//	config, exists := s.configs[tableName]
//	if !exists {
//		return nil, fmt.Errorf("table %s is not configured for sharding", tableName)
//	}
//
//	gi := &GlobalIndex{
//		DB:           s.DB,
//		TableName:    tableName,
//		IndexColumns: indexColumns,
//		Config:       config,
//		AutoRebuild:  true, // Enable auto-rebuild by default
//	}
//
//	// Ensure the global index table exists
//	err := gi.ensureGlobalIndexTable()
//	if err != nil {
//		return nil, err
//	}
//
//	// Register callbacks to maintain the index
//	//s.Callback().Create().After("gorm:create").Register("global_index:create", gi.afterCreate)
//	//s.Callback().Delete().After("gorm:delete").Register("global_index:delete", gi.afterDelete)
//	//s.Callback().Update().After("gorm:update").Register("global_index:update", gi.afterUpdate)
//
//	// Register the index with the global registry
//	// We'll handle this in the public RegisterGlobalIndex method
//
//	return gi, nil
//}

// RegisterGlobalIndex registers a global index for a table with the specified columns
//func (s *Sharding) RegisterGlobalIndex(tableName string, indexColumns []string) (*GlobalIndex, error) {
//	if s.globalIndices == nil {
//		s.globalIndices = &GlobalIndexRegistry{
//			indices: make(map[string]map[string]*GlobalIndex),
//		}
//	}
//
//	gi, err := s.NewGlobalIndex(tableName, indexColumns)
//	if err != nil {
//		return nil, err
//	}
//
//	// Register with the global registry
//	for _, col := range indexColumns {
//		s.globalIndices.Add(tableName, col, gi)
//	}
//
//	return gi, nil
//}

// ensureGlobalIndexTable creates the global index table if it doesn't exist
//func (gi *GlobalIndex) ensureGlobalIndexTable() error {
//	if !gi.DB.Migrator().HasTable(&GlobalIndexRecord{}) {
//		err := gi.DB.AutoMigrate(&GlobalIndexRecord{})
//		if err != nil {
//			return fmt.Errorf("failed to create global index table: %w", err)
//		}
//
//		// Create composite indices for faster lookups
//		for _, col := range gi.IndexColumns {
//			indexName := fmt.Sprintf("idx_%s_%s", gi.TableName, col)
//			err = gi.DB.Exec(fmt.Sprintf(
//				"CREATE INDEX IF NOT EXISTS %s ON global_index (index_column, index_value) WHERE index_column = '%s'",
//				indexName, col,
//			)).Error
//			if err != nil {
//				return fmt.Errorf("failed to create index for column %s: %w", col, err)
//			}
//		}
//	}
//	return nil
//}

func (gi *GlobalIndex) afterCreate(db *gorm.DB) {
	// Extract the suffix from the table name
	tableName := db.Statement.Table
	suffix := ""

	// Correctly extract the suffix - this is a key fix
	if strings.HasPrefix(tableName, gi.TableName) {
		suffix = tableName[len(gi.TableName):]
	}

	// If we couldn't determine a suffix, just return
	if suffix == "" {
		return
	}

	// Get the primary key value
	var recordID int64
	if pkField := db.Statement.Schema.PrioritizedPrimaryField; pkField != nil {
		value, isZero := pkField.ValueOf(context.Background(), db.Statement.ReflectValue)
		if isZero {
			return
		}

		var err error
		recordID, err = toInt64(value)
		if err != nil {
			log.Printf("Error converting primary key to int64: %v", err)
			return
		}
	} else {
		log.Printf("No primary key found for table %s", gi.TableName)
		return
	}

	// Create index entries
	currentTime := time.Now().Unix()
	var indexRecords []*GlobalIndexRecord

	for _, colName := range gi.IndexColumns {
		field, ok := db.Statement.Schema.FieldsByDBName[colName]
		if !ok {
			continue
		}

		value, isZero := field.ValueOf(context.Background(), db.Statement.ReflectValue)
		if isZero || value == nil {
			continue
		}

		// Convert value to string for storage
		valueStr := fmt.Sprintf("%v", value)

		indexRecords = append(indexRecords, &GlobalIndexRecord{
			TableSuffix: suffix,
			RecordID:    recordID,
			IndexColumn: colName,
			IndexValue:  valueStr,
			CreatedAt:   currentTime,
			UpdatedAt:   currentTime,
		})
	}

	// Insert the index records
	if len(indexRecords) > 0 {
		// Use a transaction to ensure all records are inserted
		tx := gi.DB.Begin()
		if err := tx.CreateInBatches(indexRecords, gi.Options.BatchSize).Error; err != nil {
			tx.Rollback()
			log.Printf("Error creating global index entries: %v", err)
		} else {
			tx.Commit()
		}
	}
}

// afterDelete removes entries from the global index after a record is deleted
func (gi *GlobalIndex) afterDelete(db *gorm.DB) {
	// Skip if not applicable
	if db.Statement.Schema == nil || db.Statement.Schema.Table != gi.TableName {
		return
	}

	// Extract the suffix from the table name
	tableName := db.Statement.Table
	suffix := ""
	if strings.HasPrefix(tableName, gi.TableName) && len(tableName) > len(gi.TableName) {
		suffix = tableName[len(gi.TableName):]
	}

	if suffix == "" {
		return
	}

	// Get the primary key value
	var recordID int64
	if pkField := db.Statement.Schema.PrioritizedPrimaryField; pkField != nil {
		value, isZero := pkField.ValueOf(context.Background(), db.Statement.ReflectValue)
		if isZero {
			return
		}

		var err error
		recordID, err = toInt64(value)
		if err != nil {
			log.Printf("Error converting primary key to int64: %v", err)
			return
		}
	} else {
		log.Printf("No primary key found for table %s", gi.TableName)
		return
	}

	// Delete the index entries
	deleteQuery := gi.DB.Where("table_suffix = ? AND record_id = ?", suffix, recordID).Delete(&GlobalIndexRecord{})

	// Either sync or async delete based on options
	if gi.Options.AsyncUpdates {
		go func() {
			if err := deleteQuery.Error; err != nil {
				log.Printf("Error deleting global index entries: %v", err)
			}
		}()
	} else {
		if err := deleteQuery.Error; err != nil {
			log.Printf("Error deleting global index entries: %v", err)
		}
	}
}

// afterUpdate updates entries in the global index after a record is updated
func (gi *GlobalIndex) afterUpdate(db *gorm.DB) {
	// Skip if not applicable
	if db.Statement.Schema == nil || db.Statement.Schema.Table != gi.TableName {
		return
	}

	// Check if any indexed columns were changed
	var changedIndexColumns []string
	for _, colName := range gi.IndexColumns {
		if db.Statement.Changed(colName) {
			changedIndexColumns = append(changedIndexColumns, colName)
		}
	}

	// If no indexed columns were changed, we're done
	if len(changedIndexColumns) == 0 {
		return
	}

	// For simplicity, delete the old entries and create new ones
	// This is easier than trying to update individual values
	gi.afterDelete(db)
	gi.afterCreate(db)
}

// FindByIndexColumn finds records by a specific index column value
func (gi *GlobalIndex) FindByIndexColumn(column string, value interface{}, dest interface{}) error {
	// Check if the column is indexed
	isIndexed := false
	for _, col := range gi.IndexColumns {
		if col == column {
			isIndexed = true
			break
		}
	}

	if !isIndexed {
		return fmt.Errorf("column %s is not indexed", column)
	}

	// Convert value to string for lookup
	valueStr := fmt.Sprintf("%v", value)

	// Find all matching records in the global index
	var indexRecords []GlobalIndexRecord
	err := gi.DB.Where("index_column = ? AND index_value = ?", column, valueStr).Find(&indexRecords).Error
	if err != nil {
		return err
	}

	if len(indexRecords) == 0 {
		return gorm.ErrRecordNotFound
	}

	// Group records by shard to minimize the number of queries
	recordsByShards := make(map[string][]int64)
	for _, record := range indexRecords {
		recordsByShards[record.TableSuffix] = append(recordsByShards[record.TableSuffix], record.RecordID)
	}

	// Create a slice to hold the results
	results := reflect.New(reflect.TypeOf(dest).Elem()).Elem()

	// For each shard, fetch the records
	for suffix, ids := range recordsByShards {
		tableName := gi.TableName + suffix

		// Use a temporary slice of the same type as dest
		tmpDest := reflect.New(reflect.TypeOf(dest).Elem()).Interface()

		// Fetch the records from this shard
		err := gi.DB.Table(tableName).Where("id IN ?", ids).Find(tmpDest).Error
		if err != nil {
			return err
		}

		// Append to results
		tmpSlice := reflect.ValueOf(tmpDest).Elem()
		for i := 0; i < tmpSlice.Len(); i++ {
			results = reflect.Append(results, tmpSlice.Index(i))
		}
	}

	// Set the destination to the results
	reflect.ValueOf(dest).Elem().Set(results)

	return nil
}

// IndexQuery represents a query that uses the global index
type IndexQuery struct {
	Conditions       []IndexCondition
	NonIndexedFilter func(interface{}) bool // Optional function for filtering on non-indexed fields
	SortBy           string                 // Field name to sort results by
	SortDesc         bool                   // Sort in descending order
	Limit            int                    // Maximum number of results to return
	Offset           int                    // Number of results to skip
}

// IndexCondition represents a condition in an indexed query
type IndexCondition struct {
	Column string
	Op     string
	Value  interface{}
}

// FindWithIndexedQuery executes a complex query using the global index
func (gi *GlobalIndex) FindWithIndexedQuery(dest interface{}, query *IndexQuery) error {
	// Build a query to find all matching records in the global index
	indexQuery := gi.DB.Model(&GlobalIndexRecord{})

	for _, condition := range query.Conditions {
		if !gi.isColumnIndexed(condition.Column) {
			return fmt.Errorf("column %s is not indexed", condition.Column)
		}

		valueStr := fmt.Sprintf("%v", condition.Value)

		switch condition.Op {
		case "=":
			indexQuery = indexQuery.Or("index_column = ? AND index_value = ?", condition.Column, valueStr)
		case "IN":
			// For IN conditions, we need to handle each value separately
			values, ok := condition.Value.([]interface{})
			if !ok {
				return fmt.Errorf("IN operator requires a slice of values")
			}

			valueStrs := make([]string, len(values))
			for i, v := range values {
				valueStrs[i] = fmt.Sprintf("%v", v)
			}

			indexQuery = indexQuery.Or("index_column = ? AND index_value IN ?", condition.Column, valueStrs)
		default:
			return fmt.Errorf("operator %s not supported for indexed queries", condition.Op)
		}
	}

	// Execute the index query
	var indexRecords []GlobalIndexRecord
	err := indexQuery.Find(&indexRecords).Error
	if err != nil {
		return err
	}

	if len(indexRecords) == 0 {
		return gorm.ErrRecordNotFound
	}

	// Group records by shard to minimize the number of queries
	recordsByShards := make(map[string][]int64)
	for _, record := range indexRecords {
		recordsByShards[record.TableSuffix] = append(recordsByShards[record.TableSuffix], record.RecordID)
	}

	// Create a slice to hold the results
	resultValue := reflect.ValueOf(dest).Elem()
	resultType := resultValue.Type()

	// Ensure dest is a pointer to a slice
	if resultType.Kind() != reflect.Slice {
		return fmt.Errorf("destination must be a pointer to a slice, got %T", dest)
	}

	// Get the type of slice elements
	elemType := resultType.Elem()

	// Create an empty slice of the appropriate type
	results := reflect.MakeSlice(resultType, 0, len(indexRecords))

	// For each shard, fetch the records
	for suffix, ids := range recordsByShards {
		tableName := gi.TableName + suffix

		// Create a temporary slice to hold records from this shard
		tmpSlice := reflect.New(reflect.SliceOf(elemType))
		tmpSlicePtr := tmpSlice.Interface()

		// Fetch the records from this shard
		err := gi.DB.Table(tableName).Where("id IN ?", ids).Find(tmpSlicePtr).Error
		if err != nil {
			return err
		}

		// Append records from this shard to the results
		tmpSliceVal := tmpSlice.Elem()
		for i := 0; i < tmpSliceVal.Len(); i++ {
			results = reflect.Append(results, tmpSliceVal.Index(i))
		}
	}

	// Apply any additional filtering based on non-indexed conditions
	if query.NonIndexedFilter != nil {
		filteredResults := reflect.MakeSlice(resultType, 0, results.Len())

		for i := 0; i < results.Len(); i++ {
			record := results.Index(i).Interface()
			if query.NonIndexedFilter(record) {
				filteredResults = reflect.Append(filteredResults, results.Index(i))
			}
		}

		results = filteredResults
	}

	// Apply sorting if specified
	if query.SortBy != "" {
		sort.Slice(results.Interface(), func(i, j int) bool {
			iVal := reflect.Indirect(results.Index(i)).FieldByName(query.SortBy)
			jVal := reflect.Indirect(results.Index(j)).FieldByName(query.SortBy)

			switch iVal.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if query.SortDesc {
					return iVal.Int() > jVal.Int()
				}
				return iVal.Int() < jVal.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if query.SortDesc {
					return iVal.Uint() > jVal.Uint()
				}
				return iVal.Uint() < jVal.Uint()
			case reflect.Float32, reflect.Float64:
				if query.SortDesc {
					return iVal.Float() > jVal.Float()
				}
				return iVal.Float() < jVal.Float()
			case reflect.String:
				if query.SortDesc {
					return iVal.String() > jVal.String()
				}
				return iVal.String() < jVal.String()
			default:
				// Skip sorting for unsupported types
				return i < j
			}
		})
	}

	// Apply pagination if specified
	if query.Limit > 0 {
		if query.Offset >= results.Len() {
			// If offset is beyond the results, return empty set
			resultValue.Set(reflect.MakeSlice(resultType, 0, 0))
			return nil
		}

		end := query.Offset + query.Limit
		if end > results.Len() {
			end = results.Len()
		}

		results = results.Slice(query.Offset, end)
	}

	// Set the result
	resultValue.Set(results)

	return nil
}

// isColumnIndexed checks if a column is indexed
func (gi *GlobalIndex) isColumnIndexed(column string) bool {
	for _, col := range gi.IndexColumns {
		if col == column {
			return true
		}
	}
	return false
}

// RebuildIndex rebuilds the global index for all shards
func (gi *GlobalIndex) RebuildIndex(ctx context.Context) error {
	log.Printf("Starting to rebuild global index for table %s", gi.TableName)

	// Clear existing index entries for this table
	if err := gi.DB.Where("1=1").Delete(&GlobalIndexRecord{}).Error; err != nil {
		return fmt.Errorf("failed to clear global index: %w", err)
	}

	// Get all shards for this table
	suffixes := gi.Config.ShardingSuffixs()
	if len(suffixes) == 0 {
		return fmt.Errorf("no shards found for table %s", gi.TableName)
	}

	totalRecords := int64(0)
	totalEntries := int64(0)
	currentTime := time.Now().Unix()

	// Process each shard
	for _, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// Skip if table doesn't exist
		exists := gi.DB.Migrator().HasTable(tableName)
		if !exists {
			log.Printf("Table %s doesn't exist, skipping", tableName)
			continue
		}

		// Get model structure
		var count int64
		err := gi.DB.Table(tableName).Count(&count).Error
		if err != nil {
			log.Printf("Error counting records in table %s: %v", tableName, err)
			continue
		}

		if count == 0 {
			continue // Skip empty tables
		}

		// Process in batches
		batchSize := gi.Options.BatchSize
		if batchSize <= 0 {
			batchSize = 500 // Default batch size
		}

		for offset := int64(0); offset < count; offset += int64(batchSize) {
			var records []map[string]interface{}
			err := gi.DB.Table(tableName).Offset(int(offset)).Limit(batchSize).Find(&records).Error
			if err != nil {
				log.Printf("Error fetching records from table %s: %v", tableName, err)
				continue
			}

			// Create index entries for each record
			var indexRecords []*GlobalIndexRecord

			for _, record := range records {
				recordID, ok := record["id"]
				if !ok {
					continue
				}

				id, err := toInt64(recordID)
				if err != nil {
					continue
				}

				// Index each column
				for _, colName := range gi.IndexColumns {
					value, ok := record[colName]
					if !ok || value == nil {
						continue
					}

					valueStr := fmt.Sprintf("%v", value)

					indexRecords = append(indexRecords, &GlobalIndexRecord{
						TableSuffix: suffix,
						RecordID:    id,
						IndexColumn: colName,
						IndexValue:  valueStr,
						CreatedAt:   currentTime,
						UpdatedAt:   currentTime,
					})
				}
			}

			// Insert the index records
			if len(indexRecords) > 0 {
				err := gi.DB.CreateInBatches(indexRecords, 500).Error
				if err != nil {
					log.Printf("Error inserting index records for table %s: %v", tableName, err)
					continue
				}

				totalRecords += int64(len(records))
				totalEntries += int64(len(indexRecords))
			}
		}
	}

	log.Printf("Rebuilt global index for table %s, processed %d records, created %d index entries",
		gi.TableName, totalRecords, totalEntries)

	return nil
}

// GetStats returns statistics about the global index
func (gi *GlobalIndex) GetStats() (*GlobalIndexStats, error) {
	var count int64
	if err := gi.DB.Model(&GlobalIndexRecord{}).Count(&count).Error; err != nil {
		return nil, fmt.Errorf("failed to get index record count: %w", err)
	}

	// Get cardinality for each indexed column
	cardinality := make(map[string]int64)
	for _, colName := range gi.IndexColumns {
		var distinctCount int64
		subQuery := gi.DB.Model(&GlobalIndexRecord{}).
			Where("index_column = ?", colName).
			Group("index_value").
			Select("1")

		if err := gi.DB.Model(&GlobalIndexRecord{}).
			Where("EXISTS (?)", subQuery).
			Count(&distinctCount).Error; err != nil {
			log.Printf("Error getting cardinality for column %s: %v", colName, err)
			cardinality[colName] = -1
		} else {
			cardinality[colName] = distinctCount
		}
	}

	// Get last update time
	var lastRecord GlobalIndexRecord
	if err := gi.DB.Model(&GlobalIndexRecord{}).
		Order("updated_at DESC").
		First(&lastRecord).Error; err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("failed to get last update time: %w", err)
	}

	lastUpdated := time.Unix(lastRecord.UpdatedAt, 0)
	if lastRecord.UpdatedAt == 0 {
		lastUpdated = time.Time{} // Zero time if no records
	}

	return &GlobalIndexStats{
		TableName:    gi.TableName,
		IndexColumns: gi.IndexColumns,
		RecordCount:  count,
		LastUpdated:  lastUpdated,
		Cardinality:  cardinality,
	}, nil
}
