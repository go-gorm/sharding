package sharding

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"log"
	"runtime"
	"sync"
	"time"
)

// IndexMaintenanceOptions configure how index maintenance works
type IndexMaintenanceOptions struct {
	// MaxConcurrentRebuildTasks is the maximum number of concurrent index rebuilding tasks
	MaxConcurrentRebuildTasks int

	// BackgroundRebuildInterval is the interval for periodic background rebuilds
	BackgroundRebuildInterval time.Duration

	// BatchSize is the number of records to process in a single batch
	BatchSize int

	// IncrementalRebuild enables more efficient rebuilding by only processing new records
	IncrementalRebuild bool

	// FailFast causes rebuilding to stop on first error
	FailFast bool
}

// DefaultIndexMaintenanceOptions returns sensible defaults
func DefaultIndexMaintenanceOptions() IndexMaintenanceOptions {
	return IndexMaintenanceOptions{
		MaxConcurrentRebuildTasks: runtime.NumCPU(),
		BackgroundRebuildInterval: 24 * time.Hour,
		BatchSize:                 1000,
		IncrementalRebuild:        true,
		FailFast:                  false,
	}
}

// IndexMaintenanceTask represents a background maintenance job
type IndexMaintenanceTask struct {
	ID            string
	Type          string // "rebuild", "optimize", "validate", etc.
	TableName     string
	StartTime     time.Time
	EndTime       time.Time
	Status        string // "pending", "running", "completed", "failed"
	Progress      float64
	Error         string
	RowsProcessed int64
}

// IndexMaintenanceManager manages all index maintenance tasks
type IndexMaintenanceManager struct {
	sync.Mutex
	tasks     map[string]*IndexMaintenanceTask
	options   IndexMaintenanceOptions
	sharding  *Sharding
	ctx       context.Context
	cancel    context.CancelFunc
	taskQueue chan *IndexMaintenanceTask
	wg        sync.WaitGroup
}

// NewIndexMaintenanceManager creates a new maintenance manager
func NewIndexMaintenanceManager(s *Sharding, options ...IndexMaintenanceOptions) *IndexMaintenanceManager {
	var opts IndexMaintenanceOptions
	if len(options) > 0 {
		opts = options[0]
	} else {
		opts = DefaultIndexMaintenanceOptions()
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &IndexMaintenanceManager{
		tasks:     make(map[string]*IndexMaintenanceTask),
		options:   opts,
		sharding:  s,
		ctx:       ctx,
		cancel:    cancel,
		taskQueue: make(chan *IndexMaintenanceTask, 100),
	}

	// Start worker goroutines
	for i := 0; i < opts.MaxConcurrentRebuildTasks; i++ {
		m.wg.Add(1)
		go m.worker(i)
	}

	// Start background maintenance if interval is set
	if opts.BackgroundRebuildInterval > 0 {
		go m.backgroundMaintenance()
	}

	return m
}

// worker processes tasks from the queue
func (m *IndexMaintenanceManager) worker(id int) {
	defer m.wg.Done()

	log.Printf("Index maintenance worker %d started", id)

	for {
		select {
		case <-m.ctx.Done():
			log.Printf("Index maintenance worker %d shutting down", id)
			return

		case task := <-m.taskQueue:
			log.Printf("Worker %d processing task %s for table %s", id, task.ID, task.TableName)

			task.Status = "running"
			task.StartTime = time.Now()

			// Process task based on type
			var err error
			switch task.Type {
			case "rebuild":
				err = m.processRebuildTask(task)
			case "optimize":
				err = m.processOptimizeTask(task)
			case "validate":
				err = m.processValidateTask(task)
			default:
				err = fmt.Errorf("unknown task type: %s", task.Type)
			}

			task.EndTime = time.Now()
			if err != nil {
				task.Status = "failed"
				task.Error = err.Error()
				log.Printf("Task %s failed: %v", task.ID, err)
			} else {
				task.Status = "completed"
				task.Progress = 1.0
				log.Printf("Task %s completed successfully", task.ID)
			}
		}
	}
}

// backgroundMaintenance periodically schedules maintenance tasks
func (m *IndexMaintenanceManager) backgroundMaintenance() {
	ticker := time.NewTicker(m.options.BackgroundRebuildInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return

		case <-ticker.C:
			log.Println("Starting periodic background index maintenance")

			// Check all global indices and schedule maintenance as needed
			if m.sharding.globalIndices != nil {
				indices := m.sharding.globalIndices.GetAllIndices()
				for _, index := range indices {
					stats, err := index.GetStats()
					if err != nil {
						log.Printf("Error getting stats for index on %s: %v", index.TableName, err)
						continue
					}

					// If the index is older than 1 day, schedule a rebuild
					if time.Since(stats.LastUpdated) > 24*time.Hour {
						m.ScheduleRebuild(index.TableName)
					}
				}
			}
		}
	}
}

// processRebuildTask processes a rebuild task
func (m *IndexMaintenanceManager) processRebuildTask(task *IndexMaintenanceTask) error {
	// Get the global index for this table
	var targetIndex *GlobalIndex

	if m.sharding.globalIndices != nil {
		indices := m.sharding.globalIndices.GetAllIndices()
		for _, index := range indices {
			if index.TableName == task.TableName {
				targetIndex = index
				break
			}
		}
	}

	if targetIndex == nil {
		return fmt.Errorf("no global index found for table %s", task.TableName)
	}

	// Determine if we should do incremental or full rebuild
	if m.options.IncrementalRebuild {
		return m.incrementalRebuild(targetIndex, task)
	}

	return m.fullRebuild(targetIndex, task)
}

// fullRebuild performs a complete rebuild of the global index
func (m *IndexMaintenanceManager) fullRebuild(gi *GlobalIndex, task *IndexMaintenanceTask) error {
	log.Printf("Starting full rebuild of global index for table %s", gi.TableName)

	// Delete all existing entries for this table
	if err := gi.DB.Where("1=1").Delete(&GlobalIndexRecord{}).Error; err != nil {
		return fmt.Errorf("failed to clear global index: %w", err)
	}

	// Get all shards for this table
	suffixes := gi.Config.ShardingSuffixs()
	if len(suffixes) == 0 {
		return fmt.Errorf("no shards found for table %s", gi.TableName)
	}

	totalRecords := int64(0)
	totalShards := len(suffixes)
	currentTime := time.Now().Unix()

	// Process each shard
	for i, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// Check if table exists
		if !gi.DB.Migrator().HasTable(tableName) {
			log.Printf("Table %s doesn't exist, skipping", tableName)
			continue
		}

		// Count total records in this shard
		var count int64
		if err := gi.DB.Table(tableName).Count(&count).Error; err != nil {
			log.Printf("Error counting records in table %s: %v", tableName, err)
			if m.options.FailFast {
				return err
			}
			continue
		}

		// Process in batches
		batchSize := m.options.BatchSize
		batches := (count + int64(batchSize) - 1) / int64(batchSize)

		for batch := int64(0); batch < batches; batch++ {
			offset := batch * int64(batchSize)

			// Get a batch of records
			var records []map[string]interface{}
			if err := gi.DB.Table(tableName).Offset(int(offset)).Limit(batchSize).Find(&records).Error; err != nil {
				log.Printf("Error fetching records from table %s: %v", tableName, err)
				if m.options.FailFast {
					return err
				}
				continue
			}

			// Extract indexed columns and add to index
			indexRecords := make([]*GlobalIndexRecord, 0, len(records)*len(gi.IndexColumns))

			for _, record := range records {
				recordID, ok := record["id"]
				if !ok {
					continue // Skip records without ID
				}

				// Convert ID to int64
				var id int64
				switch v := recordID.(type) {
				case int64:
					id = v
				case int:
					id = int64(v)
				case float64:
					id = int64(v)
				default:
					continue // Skip records with invalid ID
				}

				// For each indexed column, create an index entry
				for _, colName := range gi.IndexColumns {
					value, ok := record[colName]
					if !ok || value == nil {
						continue
					}

					// Convert value to string for storage
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

			// Batch insert index records
			if len(indexRecords) > 0 {
				if err := gi.DB.CreateInBatches(indexRecords, 500).Error; err != nil {
					log.Printf("Error inserting index records for table %s: %v", tableName, err)
					if m.options.FailFast {
						return err
					}
				}
			}

			// Update progress
			task.RowsProcessed += int64(len(records))
			progress := float64(i)/float64(totalShards) +
				float64(batch+1)/float64(batches)/float64(totalShards)
			task.Progress = progress

			totalRecords += int64(len(records))
		}
	}

	// Update metrics
	if gi.metrics != nil {
		gi.metrics.LastFullRebuild = time.Now()
		// Reset index misses after a full rebuild
		gi.metrics.IndexMisses = 0
	}

	log.Printf("Completed full rebuild of global index for table %s, processed %d records",
		gi.TableName, totalRecords)

	return nil
}

// incrementalRebuild performs an incremental rebuild, only indexing new or changed records
func (m *IndexMaintenanceManager) incrementalRebuild(gi *GlobalIndex, task *IndexMaintenanceTask) error {
	log.Printf("Starting incremental rebuild of global index for table %s", gi.TableName)

	// Get the last indexed record time
	var lastRecord GlobalIndexRecord
	err := gi.DB.Model(&GlobalIndexRecord{}).Order("updated_at DESC").First(&lastRecord).Error
	lastUpdateTime := int64(0)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("failed to get last update time: %w", err)
	} else if err == nil {
		lastUpdateTime = lastRecord.UpdatedAt
	}

	// For each shard, only index records updated since the last rebuild
	suffixes := gi.Config.ShardingSuffixs()
	if len(suffixes) == 0 {
		return fmt.Errorf("no shards found for table %s", gi.TableName)
	}

	totalRecords := int64(0)
	currentTime := time.Now().Unix()

	// Process each shard
	for i, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// Check if table exists
		if !gi.DB.Migrator().HasTable(tableName) {
			continue
		}

		// Find records updated since last index update
		// Note: This requires that your tables have updated_at column
		var query *gorm.DB
		if lastUpdateTime > 0 {
			query = gi.DB.Table(tableName).Where("updated_at > ?", lastUpdateTime)
		} else {
			query = gi.DB.Table(tableName)
		}

		// Count new records
		var count int64
		if err := query.Count(&count).Error; err != nil {
			log.Printf("Error counting records in table %s: %v", tableName, err)
			if m.options.FailFast {
				return err
			}
			continue
		}

		if count == 0 {
			continue // No new records in this shard
		}

		// Process in batches
		batchSize := m.options.BatchSize
		batches := (count + int64(batchSize) - 1) / int64(batchSize)

		for batch := int64(0); batch < batches; batch++ {
			offset := batch * int64(batchSize)

			// Get a batch of records
			var records []map[string]interface{}
			if err := query.Offset(int(offset)).Limit(batchSize).Find(&records).Error; err != nil {
				log.Printf("Error fetching records from table %s: %v", tableName, err)
				if m.options.FailFast {
					return err
				}
				continue
			}

			// Extract record IDs
			recordIDs := make([]int64, 0, len(records))
			for _, record := range records {
				recordID, ok := record["id"]
				if !ok {
					continue
				}

				var id int64
				switch v := recordID.(type) {
				case int64:
					id = v
				case int:
					id = int64(v)
				case float64:
					id = int64(v)
				default:
					continue
				}

				recordIDs = append(recordIDs, id)
			}

			// Delete existing index entries for these records
			if len(recordIDs) > 0 {
				if err := gi.DB.Where("table_suffix = ? AND record_id IN ?", suffix, recordIDs).
					Delete(&GlobalIndexRecord{}).Error; err != nil {
					log.Printf("Error deleting old index entries for table %s: %v", tableName, err)
					if m.options.FailFast {
						return err
					}
				}
			}

			// Create new index entries
			indexRecords := make([]*GlobalIndexRecord, 0, len(records)*len(gi.IndexColumns))

			for _, record := range records {
				recordID, ok := record["id"]
				if !ok {
					continue
				}

				var id int64
				switch v := recordID.(type) {
				case int64:
					id = v
				case int:
					id = int64(v)
				case float64:
					id = int64(v)
				default:
					continue
				}

				// For each indexed column, create an index entry
				for _, colName := range gi.IndexColumns {
					value, ok := record[colName]
					if !ok || value == nil {
						continue
					}

					// Convert value to string for storage
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

			// Batch insert index records
			if len(indexRecords) > 0 {
				if err := gi.DB.CreateInBatches(indexRecords, 500).Error; err != nil {
					log.Printf("Error inserting index records for table %s: %v", tableName, err)
					if m.options.FailFast {
						return err
					}
				}
			}

			// Update progress
			task.RowsProcessed += int64(len(records))
			totalRecords += int64(len(records))

			progress := float64(i)/float64(len(suffixes)) +
				float64(batch+1)/float64(batches)/float64(len(suffixes))
			task.Progress = progress
		}
	}

	// Update metrics
	if gi.metrics != nil {
		gi.metrics.IncrementalBuilds++
	}

	log.Printf("Completed incremental rebuild of global index for table %s, processed %d records",
		gi.TableName, totalRecords)

	return nil
}

// processOptimizeTask optimizes the global index for better performance
func (m *IndexMaintenanceManager) processOptimizeTask(task *IndexMaintenanceTask) error {
	// Perform various optimization tasks
	var totalOptimized int64

	// 1. Remove any index entries where the record no longer exists
	if task.TableName != "" {
		// Find all entries for this specific table
		var targetIndex *GlobalIndex
		if m.sharding.globalIndices != nil {
			indices := m.sharding.globalIndices.GetAllIndices()
			for _, index := range indices {
				if index.TableName == task.TableName {
					targetIndex = index
					break
				}
			}
		}

		if targetIndex == nil {
			return fmt.Errorf("no global index found for table %s", task.TableName)
		}

		result, err := m.optimizeIndexForTable(targetIndex, task)
		if err != nil {
			return err
		}
		totalOptimized += result
	} else {
		// Optimize all indices
		if m.sharding.globalIndices != nil {
			indices := m.sharding.globalIndices.GetAllIndices()
			for _, index := range indices {
				result, err := m.optimizeIndexForTable(index, task)
				if err != nil {
					if m.options.FailFast {
						return err
					}
					log.Printf("Error optimizing index for table %s: %v", index.TableName, err)
					continue
				}
				totalOptimized += result
			}
		}
	}

	log.Printf("Optimized %d index entries", totalOptimized)
	task.RowsProcessed = totalOptimized

	return nil
}

// optimizeIndexForTable optimizes the index for a single table
func (m *IndexMaintenanceManager) optimizeIndexForTable(gi *GlobalIndex, task *IndexMaintenanceTask) (int64, error) {
	var totalOptimized int64

	// Get all suffix and record ID combinations
	var indexEntries []struct {
		TableSuffix string
		RecordID    int64
	}
	if err := gi.DB.Model(&GlobalIndexRecord{}).
		Select("DISTINCT table_suffix, record_id").
		Find(&indexEntries).Error; err != nil {
		return 0, fmt.Errorf("failed to get index entries: %w", err)
	}

	// Check each record to see if it still exists
	batchSize := 100
	batches := (len(indexEntries) + batchSize - 1) / batchSize

	for i := 0; i < batches; i++ {
		start := i * batchSize
		end := (i + 1) * batchSize
		if end > len(indexEntries) {
			end = len(indexEntries)
		}

		batch := indexEntries[start:end]

		// Group by suffix for efficient querying
		bySuffix := make(map[string][]int64)
		for _, entry := range batch {
			bySuffix[entry.TableSuffix] = append(bySuffix[entry.TableSuffix], entry.RecordID)
		}

		// For each suffix, check which records still exist
		for suffix, ids := range bySuffix {
			tableName := gi.TableName + suffix

			// Skip if table doesn't exist
			if !gi.DB.Migrator().HasTable(tableName) {
				// All records in this suffix are invalid, delete them
				result := gi.DB.Where("table_suffix = ?", suffix).Delete(&GlobalIndexRecord{})
				if result.Error != nil {
					log.Printf("Error deleting index entries for non-existent table %s: %v", tableName, result.Error)
					if m.options.FailFast {
						return totalOptimized, result.Error
					}
				} else {
					totalOptimized += result.RowsAffected
				}
				continue
			}

			// Get IDs of records that exist
			var existingIDs []int64
			if err := gi.DB.Table(tableName).Where("id IN ?", ids).Pluck("id", &existingIDs).Error; err != nil {
				log.Printf("Error checking existing records in table %s: %v", tableName, err)
				if m.options.FailFast {
					return totalOptimized, err
				}
				continue
			}

			// Find IDs that don't exist anymore
			existingIDMap := make(map[int64]bool)
			for _, id := range existingIDs {
				existingIDMap[id] = true
			}

			var missingIDs []int64
			for _, id := range ids {
				if !existingIDMap[id] {
					missingIDs = append(missingIDs, id)
				}
			}

			// Delete index entries for missing IDs
			if len(missingIDs) > 0 {
				result := gi.DB.Where("table_suffix = ? AND record_id IN ?", suffix, missingIDs).
					Delete(&GlobalIndexRecord{})
				if result.Error != nil {
					log.Printf("Error deleting index entries for missing records: %v", result.Error)
					if m.options.FailFast {
						return totalOptimized, result.Error
					}
				} else {
					totalOptimized += result.RowsAffected
				}
			}
		}

		// Update progress
		task.Progress = float64(end) / float64(len(indexEntries))
	}

	return totalOptimized, nil
}

// processValidateTask validates the global index for consistency
func (m *IndexMaintenanceManager) processValidateTask(task *IndexMaintenanceTask) error {
	// Validation logic - verify that the index is consistent with the actual data
	var targetIndex *GlobalIndex

	if task.TableName != "" {
		// Find index for the specified table
		if m.sharding.globalIndices != nil {
			indices := m.sharding.globalIndices.GetAllIndices()
			for _, index := range indices {
				if index.TableName == task.TableName {
					targetIndex = index
					break
				}
			}
		}

		if targetIndex == nil {
			return fmt.Errorf("no global index found for table %s", task.TableName)
		}

		return m.validateIndexForTable(targetIndex, task)
	}

	// Validate all indices
	if m.sharding.globalIndices != nil {
		indices := m.sharding.globalIndices.GetAllIndices()
		for i, index := range indices {
			err := m.validateIndexForTable(index, task)
			if err != nil {
				if m.options.FailFast {
					return err
				}
				log.Printf("Error validating index for table %s: %v", index.TableName, err)
			}

			// Update progress
			task.Progress = float64(i+1) / float64(len(indices))
		}
	}

	return nil
}

// validateIndexForTable performs validation for a single table
func (m *IndexMaintenanceManager) validateIndexForTable(gi *GlobalIndex, task *IndexMaintenanceTask) error {
	log.Printf("Validating global index for table %s", gi.TableName)

	// 1. Check for entries with non-existent records (same as optimize but just reporting)
	orphaned, err := m.countOrphanedIndexEntries(gi)
	if err != nil {
		return fmt.Errorf("failed to check orphaned entries: %w", err)
	}
	if orphaned > 0 {
		log.Printf("Found %d orphaned index entries for table %s", orphaned, gi.TableName)
	}

	// 2. Sample check - verify that some random indexed values match actual record values
	sampleSize := 100
	var indexSamples []GlobalIndexRecord
	err = gi.DB.Model(&GlobalIndexRecord{}).
		Order("RANDOM()").Limit(sampleSize).Find(&indexSamples).Error
	if err != nil {
		return fmt.Errorf("failed to get index samples: %w", err)
	}

	inconsistencies := 0
	for _, sample := range indexSamples {
		tableName := gi.TableName + sample.TableSuffix

		// Skip if table doesn't exist
		if !gi.DB.Migrator().HasTable(tableName) {
			continue
		}

		// Get the actual record
		var record map[string]interface{}
		err := gi.DB.Table(tableName).Where("id = ?", sample.RecordID).First(&record).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				inconsistencies++
				continue
			}
			log.Printf("Error fetching record: %v", err)
			continue
		}

		// Compare indexed value with actual value
		actualValue, ok := record[sample.IndexColumn]
		if !ok {
			inconsistencies++
			continue
		}

		actualValueStr := fmt.Sprintf("%v", actualValue)
		if actualValueStr != sample.IndexValue {
			inconsistencies++
		}
	}

	if inconsistencies > 0 {
		log.Printf("Found %d inconsistencies in %d sampled index entries for table %s",
			inconsistencies, len(indexSamples), gi.TableName)
	}

	// Update task stats
	task.RowsProcessed = int64(len(indexSamples))

	if orphaned > 0 || inconsistencies > 0 {
		return fmt.Errorf("index inconsistencies found for table %s: %d orphaned, %d value mismatches",
			gi.TableName, orphaned, inconsistencies)
	}

	log.Printf("Index validation completed successfully for table %s", gi.TableName)
	return nil
}

// countOrphanedIndexEntries counts index entries that reference non-existent records
func (m *IndexMaintenanceManager) countOrphanedIndexEntries(gi *GlobalIndex) (int64, error) {
	var totalOrphaned int64

	// Group by table suffix for efficient checking
	var suffixes []string
	if err := gi.DB.Model(&GlobalIndexRecord{}).
		Distinct("table_suffix").Pluck("table_suffix", &suffixes).Error; err != nil {
		return 0, fmt.Errorf("failed to get distinct suffixes: %w", err)
	}

	for _, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// If table doesn't exist, all entries for this suffix are orphaned
		if !gi.DB.Migrator().HasTable(tableName) {
			var count int64
			if err := gi.DB.Model(&GlobalIndexRecord{}).
				Where("table_suffix = ?", suffix).Count(&count).Error; err != nil {
				return totalOrphaned, fmt.Errorf("failed to count orphaned entries: %w", err)
			}
			totalOrphaned += count
			continue
		}

		// For tables that exist, check in batches
		batchSize := 1000
		offset := 0
		for {
			var records []GlobalIndexRecord
			if err := gi.DB.Model(&GlobalIndexRecord{}).
				Where("table_suffix = ?", suffix).
				Offset(offset).Limit(batchSize).
				Find(&records).Error; err != nil {
				return totalOrphaned, fmt.Errorf("failed to get index records: %w", err)
			}

			if len(records) == 0 {
				break
			}

			// Extract record IDs
			var recordIDs []int64
			for _, record := range records {
				recordIDs = append(recordIDs, record.RecordID)
			}

			// Check which records actually exist
			var existingIDs []int64
			if err := gi.DB.Table(tableName).
				Where("id IN ?", recordIDs).
				Pluck("id", &existingIDs).Error; err != nil {
				return totalOrphaned, fmt.Errorf("failed to check existing records: %w", err)
			}

			// Count orphaned entries
			orphaned := len(recordIDs) - len(existingIDs)
			totalOrphaned += int64(orphaned)

			// Move to next batch
			offset += batchSize
			if len(records) < batchSize {
				break
			}
		}
	}

	return totalOrphaned, nil
}

// Public task scheduling methods

// ScheduleRebuild schedules a rebuild task for a table's global index
func (m *IndexMaintenanceManager) ScheduleRebuild(tableName string) string {
	task := &IndexMaintenanceTask{
		ID:        fmt.Sprintf("rebuild-%s-%d", tableName, time.Now().UnixNano()),
		Type:      "rebuild",
		TableName: tableName,
		Status:    "pending",
		StartTime: time.Now(),
	}

	m.Lock()
	m.tasks[task.ID] = task
	m.Unlock()

	m.taskQueue <- task

	return task.ID
}

// ScheduleOptimize schedules an optimization task
func (m *IndexMaintenanceManager) ScheduleOptimize(tableName string) string {
	task := &IndexMaintenanceTask{
		ID:        fmt.Sprintf("optimize-%s-%d", tableName, time.Now().UnixNano()),
		Type:      "optimize",
		TableName: tableName,
		Status:    "pending",
		StartTime: time.Now(),
	}

	m.Lock()
	m.tasks[task.ID] = task
	m.Unlock()

	m.taskQueue <- task

	return task.ID
}

// ScheduleValidate schedules a validation task
func (m *IndexMaintenanceManager) ScheduleValidate(tableName string) string {
	task := &IndexMaintenanceTask{
		ID:        fmt.Sprintf("validate-%s-%d", tableName, time.Now().UnixNano()),
		Type:      "validate",
		TableName: tableName,
		Status:    "pending",
		StartTime: time.Now(),
	}

	m.Lock()
	m.tasks[task.ID] = task
	m.Unlock()

	m.taskQueue <- task

	return task.ID
}

// GetTaskStatus returns the status of a task
func (m *IndexMaintenanceManager) GetTaskStatus(taskID string) (*IndexMaintenanceTask, error) {
	m.Lock()
	defer m.Unlock()

	task, ok := m.tasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, nil
}

// GetAllTasks returns all tasks
func (m *IndexMaintenanceManager) GetAllTasks() []*IndexMaintenanceTask {
	m.Lock()
	defer m.Unlock()

	tasks := make([]*IndexMaintenanceTask, 0, len(m.tasks))
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// PruneCompletedTasks removes completed tasks older than the specified duration
func (m *IndexMaintenanceManager) PruneCompletedTasks(olderThan time.Duration) int {
	m.Lock()
	defer m.Unlock()

	now := time.Now()
	cutoff := now.Add(-olderThan)

	count := 0
	for id, task := range m.tasks {
		if (task.Status == "completed" || task.Status == "failed") &&
			task.EndTime.Before(cutoff) {
			delete(m.tasks, id)
			count++
		}
	}

	return count
}

// SynchronizeIndex ensures the global index is synchronized with the actual data
// This is more comprehensive than a normal rebuild as it validates the synchronization
func (m *IndexMaintenanceManager) SynchronizeIndex(tableName string) (string, error) {
	// First validate the index to check for inconsistencies
	validateTaskID := m.ScheduleValidate(tableName)

	// Wait for validation to complete
	var validateTask *IndexMaintenanceTask
	for {
		var err error
		validateTask, err = m.GetTaskStatus(validateTaskID)
		if err != nil {
			return "", fmt.Errorf("failed to get validation task status: %w", err)
		}

		if validateTask.Status == "completed" || validateTask.Status == "failed" {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// If validation failed or found inconsistencies, optimize the index
	if validateTask.Status == "failed" || validateTask.Error != "" {
		log.Printf("Index validation found inconsistencies, optimizing index for table %s", tableName)
		optimizeTaskID := m.ScheduleOptimize(tableName)

		// Wait for optimization to complete
		for {
			optimizeTask, err := m.GetTaskStatus(optimizeTaskID)
			if err != nil {
				return "", fmt.Errorf("failed to get optimization task status: %w", err)
			}

			if optimizeTask.Status == "completed" || optimizeTask.Status == "failed" {
				if optimizeTask.Status == "failed" {
					return "", fmt.Errorf("index optimization failed: %s", optimizeTask.Error)
				}
				break
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

	// Finally, perform a full rebuild to ensure complete synchronization
	rebuildTaskID := m.ScheduleRebuild(tableName)
	return rebuildTaskID, nil
}

// DetectAndRepairOrphanedRecords finds and fixes inconsistencies between the index and actual data
func (m *IndexMaintenanceManager) DetectAndRepairOrphanedRecords(gi *GlobalIndex) (int64, error) {
	log.Printf("Detecting and repairing orphaned records for table %s", gi.TableName)

	var totalRepairs int64

	// Check for orphaned index entries (index entries with no corresponding record)
	orphanedEntries, err := m.countOrphanedIndexEntries(gi)
	if err != nil {
		return 0, fmt.Errorf("failed to detect orphaned entries: %w", err)
	}

	if orphanedEntries > 0 {
		log.Printf("Found %d orphaned index entries for table %s", orphanedEntries, gi.TableName)

		// Get distinct table suffixes
		var suffixes []string
		if err := gi.DB.Model(&GlobalIndexRecord{}).
			Distinct("table_suffix").Pluck("table_suffix", &suffixes).Error; err != nil {
			return 0, fmt.Errorf("failed to get distinct suffixes: %w", err)
		}

		// Process each suffix
		for _, suffix := range suffixes {
			tableName := gi.TableName + suffix

			// If table doesn't exist, delete all entries for this suffix
			if !gi.DB.Migrator().HasTable(tableName) {
				result := gi.DB.Where("table_suffix = ?", suffix).Delete(&GlobalIndexRecord{})
				if result.Error != nil {
					log.Printf("Error deleting orphaned entries for non-existent table %s: %v",
						tableName, result.Error)
					continue
				}

				totalRepairs += result.RowsAffected
				continue
			}

			// For existing tables, check each record ID
			var recordIDs []int64
			if err := gi.DB.Model(&GlobalIndexRecord{}).
				Where("table_suffix = ?", suffix).
				Distinct("record_id").Pluck("record_id", &recordIDs).Error; err != nil {
				log.Printf("Error getting record IDs for table %s: %v", tableName, err)
				continue
			}

			// Process in batches to avoid overloading the database
			batchSize := 1000
			for i := 0; i < len(recordIDs); i += batchSize {
				end := i + batchSize
				if end > len(recordIDs) {
					end = len(recordIDs)
				}

				batch := recordIDs[i:end]

				// Find which records actually exist
				var existingIDs []int64
				if err := gi.DB.Table(tableName).
					Where("id IN ?", batch).
					Pluck("id", &existingIDs).Error; err != nil {
					log.Printf("Error checking existing records in table %s: %v", tableName, err)
					continue
				}

				// Find IDs that don't exist
				existingIDMap := make(map[int64]bool)
				for _, id := range existingIDs {
					existingIDMap[id] = true
				}

				var missingIDs []int64
				for _, id := range batch {
					if !existingIDMap[id] {
						missingIDs = append(missingIDs, id)
					}
				}

				// Delete orphaned entries
				if len(missingIDs) > 0 {
					result := gi.DB.Where("table_suffix = ? AND record_id IN ?", suffix, missingIDs).
						Delete(&GlobalIndexRecord{})
					if result.Error != nil {
						log.Printf("Error deleting orphaned entries: %v", result.Error)
						continue
					}

					totalRepairs += result.RowsAffected
				}
			}
		}
	}

	// Check for missing index entries (records that exist but are not indexed)
	missingEntries, err := m.detectMissingIndexEntries(gi)
	if err != nil {
		return totalRepairs, fmt.Errorf("failed to detect missing entries: %w", err)
	}

	if missingEntries > 0 {
		log.Printf("Found %d records missing from index for table %s", missingEntries, gi.TableName)

		// Create index entries for missing records
		repairedCount, err := m.repairMissingIndexEntries(gi)
		if err != nil {
			return totalRepairs, fmt.Errorf("failed to repair missing entries: %w", err)
		}

		totalRepairs += repairedCount
	}

	log.Printf("Completed repairs for table %s: fixed %d inconsistencies", gi.TableName, totalRepairs)
	return totalRepairs, nil
}

// detectMissingIndexEntries finds records that should be indexed but aren't
func (m *IndexMaintenanceManager) detectMissingIndexEntries(gi *GlobalIndex) (int64, error) {
	var totalMissing int64

	// Get all shards for this table
	suffixes := gi.Config.ShardingSuffixs()
	for _, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// Skip if table doesn't exist
		if !gi.DB.Migrator().HasTable(tableName) {
			continue
		}

		// Count total records in this table
		var totalRecords int64
		if err := gi.DB.Table(tableName).Count(&totalRecords).Error; err != nil {
			log.Printf("Error counting records in table %s: %v", tableName, err)
			continue
		}

		// Count records that are in the index
		var indexedRecords int64
		err := gi.DB.Model(&GlobalIndexRecord{}).
			Where("table_suffix = ?", suffix).
			Distinct("record_id").
			Count(&indexedRecords).Error
		if err != nil {
			log.Printf("Error counting indexed records for table %s: %v", tableName, err)
			continue
		}

		// Calculate missing records
		missing := totalRecords - indexedRecords
		if missing > 0 {
			totalMissing += missing
		}
	}

	return totalMissing, nil
}

// repairMissingIndexEntries adds missing records to the index
func (m *IndexMaintenanceManager) repairMissingIndexEntries(gi *GlobalIndex) (int64, error) {
	var totalRepaired int64
	currentTime := time.Now().Unix()

	// Process each shard
	suffixes := gi.Config.ShardingSuffixs()
	for _, suffix := range suffixes {
		tableName := gi.TableName + suffix

		// Skip if table doesn't exist
		if !gi.DB.Migrator().HasTable(tableName) {
			continue
		}

		// Get IDs of all records in this shard
		var allIDs []int64
		if err := gi.DB.Table(tableName).Pluck("id", &allIDs).Error; err != nil {
			log.Printf("Error getting all IDs from table %s: %v", tableName, err)
			continue
		}

		// Get IDs of records that are already indexed
		var indexedIDs []int64
		err := gi.DB.Model(&GlobalIndexRecord{}).
			Where("table_suffix = ?", suffix).
			Distinct("record_id").
			Pluck("record_id", &indexedIDs).Error
		if err != nil {
			log.Printf("Error getting indexed IDs for table %s: %v", tableName, err)
			continue
		}

		// Find IDs that are not indexed
		indexedIDMap := make(map[int64]bool)
		for _, id := range indexedIDs {
			indexedIDMap[id] = true
		}

		var missingIDs []int64
		for _, id := range allIDs {
			if !indexedIDMap[id] {
				missingIDs = append(missingIDs, id)
			}
		}

		// Process missing IDs in batches
		batchSize := 100
		for i := 0; i < len(missingIDs); i += batchSize {
			end := i + batchSize
			if end > len(missingIDs) {
				end = len(missingIDs)
			}

			batch := missingIDs[i:end]

			// Get the record data for these IDs
			var records []map[string]interface{}
			if err := gi.DB.Table(tableName).Where("id IN ?", batch).Find(&records).Error; err != nil {
				log.Printf("Error fetching records from table %s: %v", tableName, err)
				continue
			}

			// Create index entries for these records
			indexRecords := make([]*GlobalIndexRecord, 0, len(records)*len(gi.IndexColumns))

			for _, record := range records {
				recordID, ok := record["id"]
				if !ok {
					continue
				}

				var id int64
				switch v := recordID.(type) {
				case int64:
					id = v
				case int:
					id = int64(v)
				case float64:
					id = int64(v)
				default:
					continue
				}

				// For each indexed column, create an index entry
				for _, colName := range gi.IndexColumns {
					value, ok := record[colName]
					if !ok || value == nil {
						continue
					}

					// Convert value to string for storage
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

			// Batch insert index records
			if len(indexRecords) > 0 {
				if err := gi.DB.CreateInBatches(indexRecords, 500).Error; err != nil {
					log.Printf("Error inserting index records for table %s: %v", tableName, err)
					continue
				}

				totalRepaired += int64(len(indexRecords))
			}
		}
	}

	return totalRepaired, nil
}

// AnalyzeIndexPerformance evaluates the performance of the global index
func (m *IndexMaintenanceManager) AnalyzeIndexPerformance(gi *GlobalIndex) (map[string]interface{}, error) {
	results := make(map[string]interface{})

	// 1. Check index cardinality (unique values vs. total records)
	var totalRecords int64
	err := gi.DB.Model(&GlobalIndexRecord{}).Count(&totalRecords).Error
	if err != nil {
		return nil, fmt.Errorf("failed to count index records: %w", err)
	}

	columnCardinality := make(map[string]float64)

	for _, col := range gi.IndexColumns {
		var uniqueValues int64
		err := gi.DB.Model(&GlobalIndexRecord{}).
			Where("index_column = ?", col).
			Distinct("index_value").
			Count(&uniqueValues).Error
		if err != nil {
			log.Printf("Error counting unique values for column %s: %v", col, err)
			continue
		}

		var totalColumnRecords int64
		err = gi.DB.Model(&GlobalIndexRecord{}).
			Where("index_column = ?", col).
			Count(&totalColumnRecords).Error
		if err != nil {
			log.Printf("Error counting records for column %s: %v", col, err)
			continue
		}

		// Calculate cardinality ratio (unique values / total values)
		// Higher ratio means more unique values, which may make the index less effective
		var cardinalityRatio float64
		if totalColumnRecords > 0 {
			cardinalityRatio = float64(uniqueValues) / float64(totalColumnRecords)
		}

		columnCardinality[col] = cardinalityRatio
	}

	results["column_cardinality"] = columnCardinality

	// 2. Analyze distribution across shards
	shardDistribution := make(map[string]int64)

	var suffixes []string
	err = gi.DB.Model(&GlobalIndexRecord{}).
		Distinct("table_suffix").Pluck("table_suffix", &suffixes).Error
	if err != nil {
		log.Printf("Error getting distinct suffixes: %v", err)
	} else {
		for _, suffix := range suffixes {
			var count int64
			gi.DB.Model(&GlobalIndexRecord{}).
				Where("table_suffix = ?", suffix).
				Count(&count)

			shardDistribution[suffix] = count
		}
	}

	results["shard_distribution"] = shardDistribution

	// 3. Calculate index size
	var indexSize int64
	// This is database-specific - for PostgreSQL:
	err = gi.DB.Raw(`SELECT pg_total_relation_size('global_index') AS size`).
		Row().Scan(&indexSize)
	if err != nil {
		log.Printf("Error calculating index size: %v", err)
	} else {
		results["index_size_bytes"] = indexSize
	}

	// 4. Record efficiency metrics
	for _, col := range gi.IndexColumns {
		// Sample some values to check efficiency
		var sampleValues []string
		err := gi.DB.Model(&GlobalIndexRecord{}).
			Where("index_column = ?", col).
			Select("DISTINCT index_value").
			Limit(10).
			Pluck("index_value", &sampleValues).Error
		if err != nil {
			log.Printf("Error sampling values for column %s: %v", col, err)
			continue
		}

		valueStats := make(map[string]map[string]interface{})

		for _, value := range sampleValues {
			// Count records with this value
			var recordCount int64
			err := gi.DB.Model(&GlobalIndexRecord{}).
				Where("index_column = ? AND index_value = ?", col, value).
				Count(&recordCount).Error
			if err != nil {
				continue
			}

			// Count distinct shards with this value
			var shardCount int64
			err = gi.DB.Model(&GlobalIndexRecord{}).
				Where("index_column = ? AND index_value = ?", col, value).
				Distinct("table_suffix").
				Count(&shardCount).Error
			if err != nil {
				continue
			}

			valueStats[value] = map[string]interface{}{
				"record_count": recordCount,
				"shard_count":  shardCount,
				"efficiency":   1.0 - (float64(shardCount) / float64(len(suffixes))),
			}
		}

		results["sample_values_"+col] = valueStats
	}

	return results, nil
}

// Shutdown stops the maintenance manager and waits for all tasks to complete
func (m *IndexMaintenanceManager) Shutdown(timeout time.Duration) error {
	log.Printf("Shutting down index maintenance manager")

	// Signal all workers to stop after their current task
	m.cancel()

	// Wait with timeout
	c := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for maintenance tasks to complete")
	}
}

// CheckIndexHealth performs a quick health check on all global indices
func (m *IndexMaintenanceManager) CheckIndexHealth() (map[string]map[string]interface{}, error) {
	result := make(map[string]map[string]interface{})

	indices := m.sharding.globalIndices.GetAllIndices()
	for _, gi := range indices {
		health := make(map[string]interface{})

		// Check if index table exists
		var count int64
		err := gi.DB.Model(&GlobalIndexRecord{}).Count(&count).Error
		if err != nil {
			health["status"] = "error"
			health["error"] = fmt.Sprintf("index table error: %v", err)
			result[gi.TableName] = health
			continue
		}

		health["record_count"] = count

		// Quick sampling for consistency
		var inconsistencies int
		var samples []GlobalIndexRecord
		err = gi.DB.Model(&GlobalIndexRecord{}).
			Order("RANDOM()").Limit(10).Find(&samples).Error
		if err == nil {
			for _, sample := range samples {
				tableName := gi.TableName + sample.TableSuffix

				if !gi.DB.Migrator().HasTable(tableName) {
					inconsistencies++
					continue
				}

				var recordExists int64
				gi.DB.Table(tableName).
					Where("id = ?", sample.RecordID).
					Count(&recordExists)

				if recordExists == 0 {
					inconsistencies++
				}
			}
		}

		// Calculate health score (0-100)
		var healthScore int
		if len(samples) > 0 {
			healthScore = 100 - (inconsistencies * 100 / len(samples))
		} else {
			healthScore = 0
		}

		health["health_score"] = healthScore
		health["sample_inconsistencies"] = inconsistencies
		health["sample_size"] = len(samples)

		if healthScore >= 90 {
			health["status"] = "healthy"
		} else if healthScore >= 70 {
			health["status"] = "warning"
		} else {
			health["status"] = "critical"
		}

		// Get last update time
		var lastRecord GlobalIndexRecord
		err = gi.DB.Model(&GlobalIndexRecord{}).
			Order("updated_at DESC").
			First(&lastRecord).Error
		if err == nil {
			health["last_updated"] = time.Unix(lastRecord.UpdatedAt, 0)
		}

		result[gi.TableName] = health
	}

	return result, nil
}

// GetMaintenanceStats returns statistics about maintenance operations
func (m *IndexMaintenanceManager) GetMaintenanceStats() map[string]interface{} {
	m.Lock()
	defer m.Unlock()

	stats := make(map[string]interface{})

	// Count tasks by status
	taskCounts := make(map[string]int)
	for _, task := range m.tasks {
		taskCounts[task.Status]++
	}
	stats["task_counts"] = taskCounts

	// Calculate average task duration
	var totalDuration int64
	completedTasks := 0
	for _, task := range m.tasks {
		if task.Status == "completed" {
			duration := task.EndTime.Sub(task.StartTime).Milliseconds()
			totalDuration += duration
			completedTasks++
		}
	}

	var avgDuration int64
	if completedTasks > 0 {
		avgDuration = totalDuration / int64(completedTasks)
	}
	stats["avg_task_duration_ms"] = avgDuration
	stats["completed_tasks"] = completedTasks

	// Get counts by task type
	typeCounts := make(map[string]int)
	for _, task := range m.tasks {
		typeCounts[task.Type]++
	}
	stats["task_types"] = typeCounts

	// Get queue length
	stats["queue_length"] = len(m.taskQueue)

	return stats
}
