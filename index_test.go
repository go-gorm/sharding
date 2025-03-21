package sharding

//
//import (
//	"fmt"
//	"math/rand"
//	"os"
//	"sort"
//	"testing"
//	"time"
//
//	tassert "github.com/stretchr/testify/assert"
//	"gorm.io/driver/postgres"
//	"gorm.io/gorm"
//	"gorm.io/gorm/logger"
//)
//
//// Global Index test constants
//var (
//	dbGiConfig = postgres.Config{
//		DSN:                  dbGlobalIndexURL(),
//		PreferSimpleProtocol: true,
//	}
//	dbGi               *gorm.DB
//	globalIndexConfigs map[string]Config
//	giMiddleware       *Sharding
//)
//
//// IndexedOrder for global index testing
//type IndexedOrder struct {
//	ID           int64 `gorm:"primarykey"`
//	UserID       int64
//	OrderStatus  string
//	PaymentType  string
//	Amount       float64
//	ItemCount    int
//	ShippingCode string
//	CreatedAt    time.Time
//	UpdatedAt    time.Time
//}
//
//func dbGlobalIndexURL() string {
//	dbURL := os.Getenv("DB_GI_URL")
//	if len(dbURL) == 0 {
//		dbURL = "postgres://postgres:@localhost:6432/sharding-globalindex-test?sslmode=disable"
//		if mysqlDialector() {
//			dbURL = "root@tcp(127.0.0.1:3306)/sharding-globalindex-test?charset=utf8mb4"
//		}
//	}
//	return dbURL
//}
//
//func initGlobalIndexTests() {
//	if dbGi != nil {
//		return
//	}
//
//	// Initialize database
//	dbGi, _ = gorm.Open(postgres.New(dbGiConfig), &gorm.Config{
//		DisableForeignKeyConstraintWhenMigrating: true,
//		Logger:                                   logger.Default.LogMode(logger.Info),
//	})
//
//	// Configure sharding with global index
//	globalIndexConfigs = map[string]Config{
//		"indexed_orders": {
//			ShardingKey:         "user_id",
//			PartitionType:       PartitionTypeHash,
//			NumberOfShards:      4,
//			PrimaryKeyGenerator: PKSnowflake,
//		},
//	}
//
//	// Register sharding middleware
//	giMiddleware = Register(globalIndexConfigs, &IndexedOrder{})
//
//	// Clean tables
//	truncateTables(dbGi,
//		"indexed_orders", "indexed_orders_0", "indexed_orders_1",
//		"indexed_orders_2", "indexed_orders_3", "global_index")
//
//	// Create tables
//	dbGi.AutoMigrate(&IndexedOrder{})
//	for i := 0; i < 4; i++ {
//		tableName := fmt.Sprintf("indexed_orders_%d", i)
//		createIndexedOrdersTable(tableName)
//	}
//
//	// Initialize and apply middleware
//	dbGi.Use(giMiddleware)
//
//	// Initialize global indices
//	err := giMiddleware.InitGlobalIndices()
//	if err != nil {
//		panic(fmt.Sprintf("Failed to initialize global indices: %v", err))
//	}
//}
//
//// Helper to create indexed order tables
//func createIndexedOrdersTable(table string) {
//	dbGi.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (
//        id bigint PRIMARY KEY,
//        user_id bigint,
//        order_status text,
//        payment_type text,
//        amount float8,
//        item_count int,
//        shipping_code text,
//        created_at timestamp with time zone,
//        updated_at timestamp with time zone
//    )`)
//}
//
//func TestGlobalIndexInit(t *testing.T) {
//	initGlobalIndexTests()
//
//	// Verify global index table was created
//	var count int64
//	err := dbGi.Table("global_index").Count(&count).Error
//	tassert.NoError(t, err)
//
//	// Check if the global index table has the expected structure
//	var columns []string
//	err = dbGi.Raw(`SELECT column_name FROM information_schema.columns WHERE table_name = 'global_index'`).Pluck("column_name", &columns).Error
//	tassert.NoError(t, err)
//
//	expectedColumns := []string{"id", "table_suffix", "record_id", "index_column", "index_value", "created_at", "updated_at"}
//	for _, col := range expectedColumns {
//		tassert.Contains(t, columns, col)
//	}
//}
//
//func TestGlobalIndexEnable(t *testing.T) {
//	initGlobalIndexTests()
//
//	// Enable global index for specific columns
//	err := giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//	tassert.NoError(t, err)
//
//	// Verify global indices were registered
//	gi := giMiddleware.globalIndices.Get("indexed_orders", "order_status")
//	tassert.NotNil(t, gi)
//	tassert.Equal(t, "indexed_orders", gi.TableName)
//	tassert.Contains(t, gi.IndexColumns, "order_status")
//	tassert.Contains(t, gi.IndexColumns, "payment_type")
//}
//
//func TestGlobalIndexInsert(t *testing.T) {
//	initGlobalIndexTests()
//
//	// Ensure global index is enabled
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert a test record
//	order := IndexedOrder{
//		UserID:       100,
//		OrderStatus:  "pending",
//		PaymentType:  "credit_card",
//		Amount:       99.99,
//		ItemCount:    3,
//		ShippingCode: "SHIP123",
//	}
//	dbGi.Create(&order)
//
//	// Verify the record was created
//	tassert.Greater(t, order.ID, int64(0))
//
//	// Give a moment for the async index updates to complete
//	time.Sleep(50 * time.Millisecond)
//
//	// Verify global index entries were created
//	var indexCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&indexCount)
//	tassert.GreaterOrEqual(t, indexCount, int64(2)) // Should have entries for order_status and payment_type
//
//	// Check specific index entries
//	var orderStatusEntry GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ?", "order_status", "pending").First(&orderStatusEntry)
//	tassert.Equal(t, order.ID, orderStatusEntry.RecordID)
//	tassert.Equal(t, "_0", orderStatusEntry.TableSuffix) // order.UserID % 4 = 0
//
//	var paymentTypeEntry GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ?", "payment_type", "credit_card").First(&paymentTypeEntry)
//	tassert.Equal(t, order.ID, paymentTypeEntry.RecordID)
//}
//
//func TestGlobalIndexQueryByIndexedColumn(t *testing.T) {
//	initGlobalIndexTests()
//
//	// Clean up from previous tests
//	cleanGlobalIndexTestData(t)
//
//	// Enable global index
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data
//	insertTestOrders(t, 20)
//
//	// Setup query rewriter
//	//rewriter := NewQueryRewriter(giMiddleware)
//
//	// Test a query that should use the global index
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ?", "shipped").Find(&orders)
//
//	// Verify the query was rewritten to use global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Verify results
//	tassert.NotEmpty(t, orders)
//	for _, order := range orders {
//		tassert.Equal(t, "shipped", order.OrderStatus)
//	}
//}
//
//func TestGlobalIndexQueryByMultipleConditions(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//	insertTestOrders(t, 20)
//
//	// Query with multiple conditions where one is indexed
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ? AND amount > ?", "pending", 50.0).Find(&orders)
//
//	// Verify the query used the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Verify results
//	tassert.NotEmpty(t, orders)
//	for _, order := range orders {
//		tassert.Equal(t, "pending", order.OrderStatus)
//		tassert.Greater(t, order.Amount, 50.0)
//	}
//}
//
//func TestGlobalIndexQueryWithShardingKey(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//	insertTestOrders(t, 20)
//
//	// When the sharding key is present, it should use direct sharding
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ? AND user_id = ?", "shipped", 101).Find(&orders)
//
//	// Verify the query did NOT use the global index (used sharding key)
//	tassert.NotContains(t, giMiddleware.LastQuery(), "via_global_index")
//	tassert.Contains(t, giMiddleware.LastQuery(), "indexed_orders_1") // user_id 101 % 4 = 1
//
//	// Verify results
//	for _, order := range orders {
//		tassert.Equal(t, "shipped", order.OrderStatus)
//		tassert.Equal(t, int64(101), order.UserID)
//	}
//}
//
//func TestGlobalIndexUpdateAndReflection(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert a test record
//	order := IndexedOrder{
//		UserID:      100,
//		OrderStatus: "pending",
//		PaymentType: "credit_card",
//		Amount:      99.99,
//	}
//	dbGi.Create(&order)
//
//	// Verify initial index entry
//	var initialEntry GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ?", "order_status", "pending").First(&initialEntry)
//	tassert.Equal(t, order.ID, initialEntry.RecordID)
//
//	// Update the record
//	dbGi.Model(&order).Update("order_status", "shipped")
//
//	// Give a moment for the async index updates to complete
//	time.Sleep(50 * time.Millisecond)
//
//	// Verify old index entry was removed
//	var oldEntryCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Where("index_column = ? AND index_value = ? AND record_id = ?",
//		"order_status", "pending", order.ID).Count(&oldEntryCount)
//	tassert.Equal(t, int64(0), oldEntryCount)
//
//	// Verify new index entry was created
//	var newEntry GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ? AND record_id = ?",
//		"order_status", "shipped", order.ID).First(&newEntry)
//	tassert.Equal(t, order.ID, newEntry.RecordID)
//
//	// Verify queries now use the updated index value
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ?", "shipped").Find(&orders)
//	tassert.NotEmpty(t, orders)
//	tassert.Contains(t, extractIDs(orders), order.ID)
//}
//
//func TestGlobalIndexDelete(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert a test record
//	order := IndexedOrder{
//		UserID:      100,
//		OrderStatus: "pending",
//		PaymentType: "credit_card",
//		Amount:      99.99,
//	}
//	dbGi.Create(&order)
//
//	// Verify initial index entries
//	var initialCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Where("record_id = ?", order.ID).Count(&initialCount)
//	tassert.GreaterOrEqual(t, initialCount, int64(2))
//
//	// Delete the record
//	dbGi.Delete(&order)
//
//	// Give a moment for the async index updates to complete
//	time.Sleep(50 * time.Millisecond)
//
//	// Verify index entries were removed
//	var remainingCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Where("record_id = ?", order.ID).Count(&remainingCount)
//	tassert.Equal(t, int64(0), remainingCount)
//}
//
//func TestGlobalIndexRebuild(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data directly into tables (bypassing index creation)
//	orders := createOrdersDirectly(t, 10)
//
//	// Initially, there should be no index entries
//	var initialCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&initialCount)
//	tassert.Equal(t, int64(0), initialCount)
//
//	// Create maintenance manager
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//
//	// Schedule index rebuild
//	taskID := maintenanceManager.ScheduleRebuild("indexed_orders")
//
//	// Wait for task to complete
//	waitForTask(t, maintenanceManager, taskID)
//
//	// Verify index entries were created
//	var afterRebuildCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&afterRebuildCount)
//	tassert.GreaterOrEqual(t, afterRebuildCount, int64(20)) // Should have at least 2 entries for each of the 10 orders
//
//	// Verify queries work after rebuild
//	var pendingOrders []IndexedOrder
//	dbGi.Where("order_status = ?", "pending").Find(&pendingOrders)
//
//	// Ensure the query uses the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Verify results
//	var pendingOrderIDs []int64
//	for _, order := range orders {
//		if order.OrderStatus == "pending" {
//			pendingOrderIDs = append(pendingOrderIDs, order.ID)
//		}
//	}
//
//	tassert.Equal(t, len(pendingOrderIDs), len(pendingOrders))
//	tassert.ElementsMatch(t, pendingOrderIDs, extractIDs(pendingOrders))
//}
//
//func TestGlobalIndexOptimize(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert some orders
//	//orders := createOrdersDirectly(t, 5)
//
//	// Rebuild index for these orders
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//	taskID := maintenanceManager.ScheduleRebuild("indexed_orders")
//	waitForTask(t, maintenanceManager, taskID)
//
//	// Create orphaned index entries (pointing to non-existent records)
//	createOrphanedIndexEntries(t, 5)
//
//	// Count initial records including orphaned ones
//	var initialCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&initialCount)
//
//	// Run optimization
//	optimizeTaskID := maintenanceManager.ScheduleOptimize("indexed_orders")
//	waitForTask(t, maintenanceManager, optimizeTaskID)
//
//	// Verify orphaned entries were removed
//	var afterOptimizeCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&afterOptimizeCount)
//	tassert.Less(t, afterOptimizeCount, initialCount)
//
//	// Verify all remaining entries are valid
//	validateTaskID := maintenanceManager.ScheduleValidate("indexed_orders")
//	task, _ := waitForTask(t, maintenanceManager, validateTaskID)
//	tassert.Equal(t, "completed", task.Status)
//	tassert.Empty(t, task.Error)
//}
//
//func TestGlobalIndexPerformance(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert larger dataset
//	insertTestOrders(t, 50)
//
//	// Test query without global index (full scan across all shards)
//	startDirect := time.Now()
//	var directResults []IndexedOrder
//	tx := dbGi.Where("/* noglobalindex */ order_status = ?", "shipped").Find(&directResults)
//	directDuration := time.Since(startDirect)
//
//	// Test query with global index
//	startIndexed := time.Now()
//	var indexedResults []IndexedOrder
//	tx2 := dbGi.Where("order_status = ?", "shipped").Find(&indexedResults)
//	indexedDuration := time.Since(startIndexed)
//
//	// Verify the indexed query was faster
//	t.Logf("Direct query: %v, Indexed query: %v", directDuration, indexedDuration)
//	tassert.True(t, indexedDuration <= directDuration,
//		"Global index query should be faster or equal to direct query")
//
//	// Verify both queries returned the same results
//	tassert.Equal(t, len(directResults), len(indexedResults))
//	tassert.ElementsMatch(t, extractIDs(directResults), extractIDs(indexedResults))
//
//	// Verify the queries executed as expected
//	tassert.Contains(t, tx.Statement.SQL.String(), "UNION ALL")         // Direct should use UNION
//	tassert.Contains(t, tx2.Statement.SQL.String(), "via_global_index") // Indexed should use index
//}
//
//func TestGlobalIndexWithJoin(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//
//	// Create related table
//	dbGi.Exec(`DROP TABLE IF EXISTS order_items`)
//	dbGi.Exec(`CREATE TABLE order_items (
//		id bigint PRIMARY KEY,
//		order_id bigint,
//		product_name text,
//		quantity int
//	)`)
//
//	// Enable global index
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test orders
//	orders := insertTestOrders(t, 5)
//
//	// Insert related order items
//	for _, order := range orders {
//		dbGi.Exec(`INSERT INTO order_items (id, order_id, product_name, quantity) VALUES
//			(?, ?, ?, ?)`, order.ID*10, order.ID, "Product "+fmt.Sprint(order.ID), rand.Intn(5)+1)
//	}
//
//	// Query with a join
//	var results []struct {
//		IndexedOrder
//		ProductName string
//		Quantity    int
//	}
//
//	dbGi.Table("indexed_orders").
//		Select("indexed_orders.*, order_items.product_name, order_items.quantity").
//		Joins("JOIN order_items ON order_items.order_id = indexed_orders.id").
//		Where("order_status = ?", "pending").
//		Scan(&results)
//
//	// Verify the query used the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Verify the join worked
//	tassert.NotEmpty(t, results)
//	for _, result := range results {
//		tassert.Equal(t, "pending", result.OrderStatus)
//		tassert.NotEmpty(t, result.ProductName)
//		tassert.Contains(t, result.ProductName, "Product")
//		tassert.GreaterOrEqual(t, result.Quantity, 1)
//	}
//}
//
//func TestGlobalIndexQueryCaching(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data
//	insertTestOrders(t, 10)
//
//	// Set up query rewriter with caching
//	options := DefaultQueryRewriteOptions()
//	options.EnableQueryCache = true
//	rewriter := NewQueryRewriter(giMiddleware, options)
//
//	// Execute the same query twice
//	query := "SELECT * FROM indexed_orders WHERE order_status = ?"
//	args := []interface{}{"shipped"}
//
//	// First execution
//	rewriter.RewriteQuery(query, args)
//
//	// Get the cache entry
//	cache := rewriter.GetQueryCache()
//	entry, exists := cache.Get(query, args)
//
//	// Verify cache hit
//	tassert.True(t, exists)
//	tassert.NotNil(t, entry)
//	tassert.Equal(t, 1, entry.HitCount)
//
//	// Execute again
//	rewriter.RewriteQuery(query, args)
//
//	// Verify hit count increased
//	entry, _ = cache.Get(query, args)
//	tassert.Equal(t, 2, entry.HitCount)
//}
//
////func TestGlobalIndexMonitor(t *testing.T) {
////	initGlobalIndexTests()
////	cleanGlobalIndexTestData(t)
////	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
////
////	// Set up monitor
////	monitor := NewGlobalIndexMonitor(giMiddleware, 100*time.Millisecond)
////	monitor.Start()
////	defer monitor.Stop()
////
////	// Insert some data
////	insertTestOrders(t, 10)
////
////	// Execute some queries
////	dbGi.Where("order_status = ?", "shipped").Find(&[]IndexedOrder{})
////	dbGi.Where("order_status = ?", "pending").Find(&[]IndexedOrder{})
////	dbGi.Where("user_id = ?", 100).Find(&[]IndexedOrder{}) // Direct shard query
////
////	// Wait for monitoring to update
////	time.Sleep(200 * time.Millisecond)
////
////	// Get monitoring snapshot
////	snapshot := monitor.GetSnapshot()
////
////	// Verify monitoring data
////	tassert.GreaterOrEqual(t, snapshot.QueriesUsingIndex, int64(2))
////	tassert.GreaterOrEqual(t, snapshot.QueriesNotUsingIndex, int64(1))
////	tassert.Greater(t, snapshot.TotalRecords, int64(0))
////	tassert.NotEmpty(t, snapshot.LatestQueries)
////
////	// Verify we have coverage data
////	tassert.NotEmpty(t, snapshot.TableIndexCoverage)
////	tassert.Contains(t, snapshot.TableIndexCoverage, "indexed_orders")
////	tassert.Greater(t, snapshot.TableIndexCoverage["indexed_orders"], 0.0)
////}
//
//func TestGlobalIndexAnalyze(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data with controlled distribution
//	for i := 0; i < 20; i++ {
//		dbGi.Create(&IndexedOrder{
//			UserID:      int64(100 + i%4),
//			OrderStatus: []string{"pending", "shipped", "delivered", "cancelled"}[i%4],
//			PaymentType: []string{"credit_card", "paypal", "bank_transfer"}[i%3],
//			Amount:      float64(i*10) + 10.0,
//		})
//	}
//
//	// Create maintenance manager
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//
//	// Get index for analysis
//	gi := giMiddleware.globalIndices.Get("indexed_orders", "order_status")
//	tassert.NotNil(t, gi)
//
//	// Analyze index performance
//	results, err := maintenanceManager.AnalyzeIndexPerformance(gi)
//	tassert.NoError(t, err)
//
//	// Verify analysis results
//	tassert.Contains(t, results, "column_cardinality")
//	tassert.Contains(t, results, "shard_distribution")
//
//	// Check cardinality
//	cardinality, ok := results["column_cardinality"].(map[string]float64)
//	tassert.True(t, ok)
//	tassert.Contains(t, cardinality, "order_status")
//	tassert.Contains(t, cardinality, "payment_type")
//
//	// The cardinality should be reasonable (lower is more selective)
//	tassert.LessOrEqual(t, cardinality["order_status"], 0.5)
//	tassert.LessOrEqual(t, cardinality["payment_type"], 0.5)
//}
//
//func TestGlobalIndexHealth(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data
//	insertTestOrders(t, 10)
//
//	// Create maintenance manager
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//
//	// Check index health
//	healthReport, err := maintenanceManager.CheckIndexHealth()
//	tassert.NoError(t, err)
//
//	// Verify health report format
//	tassert.Contains(t, healthReport, "indexed_orders")
//
//	orderHealth := healthReport["indexed_orders"]
//	tassert.Contains(t, orderHealth, "status")
//	tassert.Contains(t, orderHealth, "health_score")
//	tassert.Contains(t, orderHealth, "record_count")
//
//	// A fresh, clean index should have good health
//	tassert.Equal(t, "healthy", orderHealth["status"])
//	tassert.GreaterOrEqual(t, orderHealth["health_score"], 90)
//
//	// Introduce some inconsistencies
//	createOrphanedIndexEntries(t, 3)
//
//	// Check health again
//	healthReport, _ = maintenanceManager.CheckIndexHealth()
//	orderHealth = healthReport["indexed_orders"]
//
//	// Health score should be reduced
//	tassert.NotEqual(t, "healthy", orderHealth["status"])
//	tassert.Less(t, orderHealth["health_score"], 90)
//}
//
//func TestGlobalIndexCrossShardQuery(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert orders with different user_ids (different shards) but same order_status
//	userIDs := []int64{100, 101, 102, 103} // These will go to different shards
//	for _, userID := range userIDs {
//		dbGi.Create(&IndexedOrder{
//			UserID:      userID,
//			OrderStatus: "pending",
//			PaymentType: "credit_card",
//			Amount:      99.99,
//		})
//	}
//
//	// Query by order_status which crosses shards
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ?", "pending").Find(&orders)
//
//	// Verify we got results from all shards
//	tassert.Equal(t, len(userIDs), len(orders))
//
//	// Verify the results include all user IDs
//	var resultUserIDs []int64
//	for _, order := range orders {
//		resultUserIDs = append(resultUserIDs, order.UserID)
//	}
//	tassert.ElementsMatch(t, userIDs, resultUserIDs)
//
//	// Verify the query used the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//}
//
//func TestGlobalIndexSelectivity(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert orders with different distributions
//	// Many orders with status "pending"
//	for i := 0; i < 20; i++ {
//		dbGi.Create(&IndexedOrder{
//			UserID:      int64(100 + i),
//			OrderStatus: "pending",
//			PaymentType: []string{"credit_card", "paypal", "bank_transfer"}[i%3],
//		})
//	}
//
//	// Few orders with status "cancelled"
//	for i := 0; i < 3; i++ {
//		dbGi.Create(&IndexedOrder{
//			UserID:      int64(200 + i),
//			OrderStatus: "cancelled",
//			PaymentType: "paypal",
//		})
//	}
//
//	// Query by the more selective value
//	startCancelled := time.Now()
//	var cancelledOrders []IndexedOrder
//	dbGi.Where("order_status = ?", "cancelled").Find(&cancelledOrders)
//	cancelledDuration := time.Since(startCancelled)
//
//	// Query by the less selective value
//	startPending := time.Now()
//	var pendingOrders []IndexedOrder
//	dbGi.Where("order_status = ?", "pending").Find(&pendingOrders)
//	pendingDuration := time.Since(startPending)
//
//	// Both should use the global index
//	// But the more selective query should be potentially faster
//	t.Logf("Cancelled query (3 results): %v, Pending query (20 results): %v",
//		cancelledDuration, pendingDuration)
//
//	// Verify results
//	tassert.Equal(t, 3, len(cancelledOrders))
//	tassert.Equal(t, 20, len(pendingOrders))
//}
//
//// Helper functions
//
//func cleanGlobalIndexTestData(t *testing.T) {
//	truncateTables(dbGi, "indexed_orders", "indexed_orders_0", "indexed_orders_1",
//		"indexed_orders_2", "indexed_orders_3", "global_index")
//}
//
//func insertTestOrders(t *testing.T, count int) []IndexedOrder {
//	var orders []IndexedOrder
//
//	statuses := []string{"pending", "shipped", "delivered", "cancelled"}
//	paymentTypes := []string{"credit_card", "paypal", "bank_transfer", "crypto"}
//
//	for i := 0; i < count; i++ {
//		order := IndexedOrder{
//			UserID:       int64(100 + i%4), // This distributes across 4 shards
//			OrderStatus:  statuses[i%len(statuses)],
//			PaymentType:  paymentTypes[i%len(paymentTypes)],
//			Amount:       float64(50 + (i * 10)),
//			ItemCount:    i%5 + 1,
//			ShippingCode: fmt.Sprintf("SHIP%d", i+1000),
//		}
//
//		dbGi.Create(&order)
//		orders = append(orders, order)
//	}
//
//	return orders
//}
//
//func createOrdersDirectly(t *testing.T, count int) []IndexedOrder {
//	var orders []IndexedOrder
//	rand.Seed(time.Now().UnixNano())
//
//	statuses := []string{"pending", "shipped", "delivered", "cancelled"}
//	paymentTypes := []string{"credit_card", "paypal", "bank_transfer", "crypto"}
//
//	for i := 0; i < count; i++ {
//		// Generate a random ID
//		id := rand.Int63n(1000000) + 1
//
//		// Determine shard index from user_id
//		userID := int64(100 + i%4)
//		shardIndex := userID % 4
//
//		// Create the order directly in the shard table
//		order := IndexedOrder{
//			ID:           id,
//			UserID:       userID,
//			OrderStatus:  statuses[i%len(statuses)],
//			PaymentType:  paymentTypes[i%len(paymentTypes)],
//			Amount:       float64(50 + (i * 10)),
//			ItemCount:    i%5 + 1,
//			ShippingCode: fmt.Sprintf("SHIP%d", i+1000),
//			CreatedAt:    time.Now(),
//			UpdatedAt:    time.Now(),
//		}
//
//		// Insert directly into the sharded table
//		tableName := fmt.Sprintf("indexed_orders_%d", shardIndex)
//		err := dbGi.Table(tableName).Create(&order).Error
//		tassert.NoError(t, err)
//
//		orders = append(orders, order)
//	}
//
//	return orders
//}
//
//func createOrphanedIndexEntries(t *testing.T, count int) {
//	// Create index entries that point to non-existent records
//	for i := 0; i < count; i++ {
//		fakeID := rand.Int63n(1000000) + 9000000 // Unlikely to exist
//		tableSuffix := fmt.Sprintf("_%d", i%4)
//
//		dbGi.Create(&GlobalIndexRecord{
//			TableSuffix: tableSuffix,
//			RecordID:    fakeID,
//			IndexColumn: "order_status",
//			IndexValue:  "orphaned",
//			CreatedAt:   time.Now().Unix(),
//			UpdatedAt:   time.Now().Unix(),
//		})
//	}
//}
//
//func waitForTask(t *testing.T, manager *IndexMaintenanceManager, taskID string) (*IndexMaintenanceTask, error) {
//	maxWait := 5 * time.Second
//	startTime := time.Now()
//
//	for {
//		task, err := manager.GetTaskStatus(taskID)
//		if err != nil {
//			return nil, err
//		}
//
//		if task.Status == "completed" || task.Status == "failed" {
//			return task, nil
//		}
//
//		if time.Since(startTime) > maxWait {
//			return nil, fmt.Errorf("task timed out after %s", maxWait)
//		}
//
//		// Progress report
//		t.Logf("Task %s in progress: %.1f%% complete", taskID, task.Progress*100)
//
//		time.Sleep(100 * time.Millisecond)
//	}
//}
//
//func extractIDs(orders []IndexedOrder) []int64 {
//	ids := make([]int64, len(orders))
//	for i, order := range orders {
//		ids[i] = order.ID
//	}
//	sort.Slice(ids, func(i, j int) bool {
//		return ids[i] < ids[j]
//	})
//	return ids
//}
//
//func TestGlobalIndexComplexQuery(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data
//	insertTestOrders(t, 20)
//
//	// Complex query with multiple conditions, ORDER BY, and LIMIT
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ? AND payment_type = ?", "pending", "credit_card").
//		Where("amount > ?", 100).
//		Order("amount DESC").
//		Limit(5).
//		Find(&orders)
//
//	// Verify the query used the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Verify results
//	tassert.NotEmpty(t, orders)
//	tassert.LessOrEqual(t, len(orders), 5) // Shouldn't exceed the LIMIT
//
//	// Verify conditions were applied
//	for _, order := range orders {
//		tassert.Equal(t, "pending", order.OrderStatus)
//		tassert.Equal(t, "credit_card", order.PaymentType)
//		tassert.Greater(t, order.Amount, 100.0)
//	}
//
//	// Verify ORDER BY was applied
//	for i := 0; i < len(orders)-1; i++ {
//		tassert.GreaterOrEqual(t, orders[i].Amount, orders[i+1].Amount)
//	}
//}
//
//func TestGlobalIndexCaseInsensitiveSearch(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data with mixed case
//	dbGi.Create(&IndexedOrder{
//		UserID:      100,
//		OrderStatus: "Pending", // Capital P
//		PaymentType: "Credit_Card",
//		Amount:      99.99,
//	})
//
//	dbGi.Create(&IndexedOrder{
//		UserID:      101,
//		OrderStatus: "pending", // Lowercase
//		PaymentType: "credit_card",
//		Amount:      199.99,
//	})
//
//	// Query with case-insensitive search
//	var orders []IndexedOrder
//	dbGi.Where("LOWER(order_status) = LOWER(?)", "pending").Find(&orders)
//
//	// This should use cross-shard query, not global index
//	// because our simple index doesn't handle functions like LOWER()
//	tassert.NotContains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Should find both records
//	tassert.Equal(t, 2, len(orders))
//
//	// Extra test for exact matches which should use the index
//	var exactOrders []IndexedOrder
//	dbGi.Where("order_status = ?", "Pending").Find(&exactOrders)
//
//	// Verify this uses global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//
//	// Should find only one record
//	tassert.Equal(t, 1, len(exactOrders))
//	tassert.Equal(t, "Pending", exactOrders[0].OrderStatus)
//}
//
//func TestGlobalIndexPartialUpdate(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert a test order
//	order := IndexedOrder{
//		UserID:       100,
//		OrderStatus:  "pending",
//		PaymentType:  "credit_card",
//		Amount:       99.99,
//		ShippingCode: "SHIP1000",
//	}
//	dbGi.Create(&order)
//
//	// Update a non-indexed field
//	dbGi.Model(&IndexedOrder{}).
//		Where("id = ?", order.ID).
//		Update("amount", 199.99)
//
//	// Give a moment for async updates
//	time.Sleep(50 * time.Millisecond)
//
//	// Verify the index remains valid
//	var indexEntries []GlobalIndexRecord
//	dbGi.Where("record_id = ?", order.ID).Find(&indexEntries)
//
//	// Should still have entries for both indexed columns
//	var columns []string
//	for _, entry := range indexEntries {
//		columns = append(columns, entry.IndexColumn)
//	}
//	tassert.Contains(t, columns, "order_status")
//	tassert.Contains(t, columns, "payment_type")
//
//	// Now update an indexed field
//	dbGi.Model(&IndexedOrder{}).
//		Where("id = ?", order.ID).
//		Update("order_status", "shipped")
//
//	// Give a moment for async updates
//	time.Sleep(50 * time.Millisecond)
//
//	// Verify the index was updated
//	var statusIndex GlobalIndexRecord
//	dbGi.Where("record_id = ? AND index_column = ?", order.ID, "order_status").First(&statusIndex)
//	tassert.Equal(t, "shipped", statusIndex.IndexValue)
//}
//
//func TestGlobalIndexBulkOperations(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Create test data
//	//orders := insertTestOrders(t, 10)
//
//	// Bulk update
//	dbGi.Model(&IndexedOrder{}).
//		Where("order_status = ?", "pending").
//		Update("order_status", "processing")
//
//	// Give a moment for async updates
//	time.Sleep(100 * time.Millisecond)
//
//	// Verify index was updated
//	var processingEntries []GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ?", "order_status", "processing").Find(&processingEntries)
//	tassert.NotEmpty(t, processingEntries)
//
//	// Verify the query can find them with the new status
//	var updatedOrders []IndexedOrder
//	dbGi.Where("order_status = ?", "processing").Find(&updatedOrders)
//	tassert.NotEmpty(t, updatedOrders)
//
//	// Bulk delete
//	dbGi.Where("order_status = ?", "processing").Delete(&IndexedOrder{})
//
//	// Give a moment for async updates
//	time.Sleep(100 * time.Millisecond)
//
//	// Verify index entries were removed
//	var remainingEntries []GlobalIndexRecord
//	dbGi.Where("index_column = ? AND index_value = ?", "order_status", "processing").Find(&remainingEntries)
//	tassert.Empty(t, remainingEntries)
//}
//
//func TestGlobalIndexAsyncOperations(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//
//	// Create global index with async updates
//	options := DefaultGlobalIndexOptions()
//	options.AsyncUpdates = true
//	err := giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"}, options)
//	tassert.NoError(t, err)
//
//	// Insert test data
//	order := IndexedOrder{
//		UserID:      100,
//		OrderStatus: "pending",
//		PaymentType: "credit_card",
//		Amount:      99.99,
//	}
//	dbGi.Create(&order)
//
//	// Wait for async update to complete
//	time.Sleep(100 * time.Millisecond)
//
//	// Verify index entries
//	var indexEntries []GlobalIndexRecord
//	dbGi.Where("record_id = ?", order.ID).Find(&indexEntries)
//	tassert.GreaterOrEqual(t, len(indexEntries), 2)
//}
//
//func TestGlobalIndexDetectAndRepair(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Insert test data
//	orders := createOrdersDirectly(t, 5)
//
//	// Manually create some index entries
//	for _, order := range orders {
//		dbGi.Create(&GlobalIndexRecord{
//			TableSuffix: fmt.Sprintf("_%d", order.UserID%4),
//			RecordID:    order.ID,
//			IndexColumn: "order_status",
//			IndexValue:  order.OrderStatus,
//			CreatedAt:   time.Now().Unix(),
//			UpdatedAt:   time.Now().Unix(),
//		})
//	}
//
//	// Create some orphaned entries
//	createOrphanedIndexEntries(t, 3)
//
//	// Create maintenance manager
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//
//	// Get the global index
//	gi := giMiddleware.globalIndices.Get("indexed_orders", "order_status")
//	tassert.NotNil(t, gi)
//
//	// Initial index state
//	var initialCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&initialCount)
//
//	// Detect and repair
//	repairedCount, err := maintenanceManager.DetectAndRepairOrphanedRecords(gi)
//	tassert.NoError(t, err)
//	tassert.Greater(t, repairedCount, int64(0))
//
//	// Final index state
//	var finalCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&finalCount)
//
//	// Orphaned records should be removed
//	tassert.NotEqual(t, initialCount, finalCount)
//
//	// Missing index entries should be added (payment_type was not indexed before)
//	var paymentTypeEntries []GlobalIndexRecord
//	dbGi.Where("index_column = ?", "payment_type").Find(&paymentTypeEntries)
//	tassert.GreaterOrEqual(t, len(paymentTypeEntries), len(orders))
//}
//
//func TestGlobalIndexSynchronize(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Create orders directly (bypassing index)
//	createOrdersDirectly(t, 5)
//
//	// Create some orphaned entries
//	createOrphanedIndexEntries(t, 3)
//
//	// Maintenance manager
//	maintenanceManager := NewIndexMaintenanceManager(giMiddleware)
//
//	// Initial state
//	var initialCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&initialCount)
//
//	// Run synchronization
//	taskID, err := maintenanceManager.SynchronizeIndex("indexed_orders")
//	tassert.NoError(t, err)
//
//	// Wait for synchronization to complete
//	task, err := waitForTask(t, maintenanceManager, taskID)
//	tassert.NoError(t, err)
//	tassert.Equal(t, "completed", task.Status)
//
//	// Final state
//	var finalCount int64
//	dbGi.Model(&GlobalIndexRecord{}).Count(&finalCount)
//
//	// The index should be clean after synchronization
//	t.Logf("Initial index size: %d, Final index size: %d", initialCount, finalCount)
//
//	// Verify queries work properly with the synchronized index
//	var orders []IndexedOrder
//	dbGi.Where("order_status = ?", "pending").Find(&orders)
//
//	// Verify the query uses the global index
//	tassert.Contains(t, giMiddleware.LastQuery(), "via_global_index")
//}
//
////func TestGlobalIndexTester(t *testing.T) {
////	initGlobalIndexTests()
////	cleanGlobalIndexTestData(t)
////	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
////
////	// Insert test data
////	insertTestOrders(t, 10)
////
////	// Create a global index tester
////	tester := NewGlobalIndexTester(giMiddleware)
////
////	// Run the test suite
////	results, err := tester.TestGlobalIndex(context.Background())
////	tassert.NoError(t, err)
////
////	// Verify we got test results
////	tassert.NotEmpty(t, results)
////
////	// Check individual test results
////	var coverageTest, consistencyTest, perfTest *IndexTestResult
////
////	for _, result := range results {
////		if strings.Contains(result.TestName, "Coverage") {
////			coverageTest = result
////		} else if strings.Contains(result.TestName, "Consistency") {
////			consistencyTest = result
////		} else if strings.Contains(result.TestName, "Performance") {
////			perfTest = result
////		}
////	}
////
////	// Verify all tests ran
////	tassert.NotNil(t, coverageTest)
////	tassert.NotNil(t, consistencyTest)
////	tassert.NotNil(t, perfTest)
////
////	// All tests should pass for a fresh index
////	tassert.True(t, coverageTest.Success)
////	tassert.True(t, consistencyTest.Success)
////	tassert.True(t, perfTest.Success)
////
////	// Spot check some metrics
////	tassert.Contains(t, coverageTest.Details, "coverage_percent")
////	tassert.Contains(t, consistencyTest.Details, "sample_size")
////	tassert.Contains(t, perfTest.Details, "performance_ratio")
////}
//
//func TestGlobalIndexInTransaction(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Start a transaction
//	tx := dbGi.Begin()
//
//	// Create an order in the transaction
//	order := IndexedOrder{
//		UserID:      100,
//		OrderStatus: "pending",
//		PaymentType: "credit_card",
//		Amount:      99.99,
//	}
//	tx.Create(&order)
//
//	// Commit the transaction
//	tx.Commit()
//
//	// Give a moment for async updates
//	time.Sleep(100 * time.Millisecond)
//
//	// Verify index entries were created
//	var indexEntries []GlobalIndexRecord
//	dbGi.Where("record_id = ?", order.ID).Find(&indexEntries)
//	tassert.GreaterOrEqual(t, len(indexEntries), 2)
//
//	// Test rollback scenario
//	tx = dbGi.Begin()
//
//	// Create another order
//	rollbackOrder := IndexedOrder{
//		UserID:      101,
//		OrderStatus: "pending",
//		PaymentType: "paypal",
//		Amount:      199.99,
//	}
//	tx.Create(&rollbackOrder)
//
//	// Get the ID
//	rollbackID := rollbackOrder.ID
//
//	// Rollback the transaction
//	tx.Rollback()
//
//	// Give a moment for any async operations
//	time.Sleep(100 * time.Millisecond)
//
//	// Verify the order doesn't exist
//	var count int64
//	dbGi.Model(&IndexedOrder{}).Where("id = ?", rollbackID).Count(&count)
//	tassert.Equal(t, int64(0), count)
//
//	// Verify no index entries exist for the rolled back order
//	var rollbackEntries int64
//	dbGi.Model(&GlobalIndexRecord{}).Where("record_id = ?", rollbackID).Count(&rollbackEntries)
//	tassert.Equal(t, int64(0), rollbackEntries)
//}
//
//func TestGlobalIndexCostBasedDecisions(t *testing.T) {
//	initGlobalIndexTests()
//	cleanGlobalIndexTestData(t)
//	giMiddleware.EnableGlobalIndex("indexed_orders", []string{"order_status", "payment_type"})
//
//	// Create data with differing selectivity
//	// Many with same status (low selectivity)
//	for i := 0; i < 20; i++ {
//		dbGi.Create(&IndexedOrder{
//			UserID:      int64(100 + i%4),
//			OrderStatus: "common_status",
//			PaymentType: "credit_card",
//			Amount:      float64(i*10) + 50,
//		})
//	}
//
//	// Few with unique status (high selectivity)
//	dbGi.Create(&IndexedOrder{
//		UserID:      100,
//		OrderStatus: "rare_status",
//		PaymentType: "paypal",
//		Amount:      999.99,
//	})
//
//	// Create query rewriter with cost-based decisions
//	options := DefaultQueryRewriteOptions()
//	options.EnableCost = true
//	options.CostThreshold = 0.5 // Use index only when it's at least 50% better
//	rewriter := NewQueryRewriter(giMiddleware, options)
//
//	// Test query with good selectivity (should use index)
//	query1 := "SELECT * FROM indexed_orders WHERE order_status = ?"
//	args1 := []interface{}{"rare_status"}
//	_, _, usedIndex1 := rewriter.RewriteQuery(query1, args1)
//
//	// Test query with poor selectivity (may not use index)
//	query2 := "SELECT * FROM indexed_orders WHERE order_status = ?"
//	args2 := []interface{}{"common_status"}
//	rewrittenQuery2, _, usedIndex2 := rewriter.RewriteQuery(query2, args2)
//
//	// Log results
//	t.Logf("Selective query used index: %v", usedIndex1)
//	t.Logf("Non-selective query used index: %v", usedIndex2)
//
//	// The selective query should definitely use the index
//	tassert.True(t, usedIndex1)
//
//	// The cost-based decision could go either way for the non-selective query
//	// We're just testing the mechanism works, not the specific decision
//	if usedIndex2 {
//		tassert.Contains(t, rewrittenQuery2, "via_global_index")
//	} else {
//		tassert.NotContains(t, rewrittenQuery2, "via_global_index")
//	}
//}
