package sharding

//
//import (
//	"fmt"
//	tassert "github.com/stretchr/testify/assert"
//	"gorm.io/gorm"
//	"strings"
//	"testing"
//)
//
//func TestListPartitionInsert(t *testing.T) {
//	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
//	// Create ERC20 contract
//	dbList.Create(&Contract{
//		Name:    "TokenA",
//		Type:    "ERC20",
//		IsERC20: true,
//		Data:    "TokenA Data",
//	})
//
//	// Should go to partition 0 (ERC20)
//	assertQueryResult(t, `INSERT INTO contracts_0 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)
//}
//
//func TestListPartitionInsertDifferentTypes(t *testing.T) {
//	// Insert different contract types to verify they go to different partitions
//	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
//	// ERC721 contract
//	dbList.Create(&Contract{
//		Name:     "NFT Collection",
//		Type:     "ERC721",
//		IsERC721: true,
//		Data:     "NFT Collection Data",
//	})
//	// Should go to partition 1 (ERC721)
//	assertQueryResult(t, `INSERT INTO contracts_1 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)
//
//	// ERC1155 contract
//	dbList.Create(&Contract{
//		Name:      "Multi-Token",
//		Type:      "ERC1155",
//		IsERC1155: true,
//		Data:      "Multi-Token Data",
//	})
//	// Should go to partition 2 (ERC1155)
//	assertQueryResult(t, `INSERT INTO contracts_2 ("name", "type", "is_erc20", "is_erc721", "is_erc1155", "data", "created_at", "updated_at", "id") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING "id"`, listMiddleware)
//}
//
////func TestListPartitionInsertInvalidType(t *testing.T) {
////	// Try to insert with an invalid type that's not in the list
////	err := dbList.Create(&Contract{
////		Name: "Invalid Contract",
////		Type: "ERC777", // Not in the list values
////		Data: "Invalid Data",
////	}).Error
////
////	tassert.Error(t, err, "Expected an error for invalid type")
////	if err != nil {
////		tassert.Contains(t, err.Error(), "partition list")
////	}
////}
//
//func TestListPartitionFillID(t *testing.T) {
//	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
//	// Test auto ID generation with list partitioning
//	contract := &Contract{
//		Name:    "Auto ID Contract",
//		Type:    "ERC20",
//		IsERC20: true,
//	}
//
//	dbList.Create(contract)
//
//	// Verify ID was generated
//	tassert.Greater(t, contract.ID, int64(0))
//
//	// Check the query
//	expectedQuery := `INSERT INTO contracts_0 (name, type, is_erc20, is_erc721, is_erc1155, data, created_at, updated_at, id) VALUES`
//	lastQuery := listMiddleware.LastQuery()
//	tassert.Contains(t, lastQuery, expectedQuery)
//}
//
//func TestListPartitionSelect(t *testing.T) {
//	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
//	// Create test data
//	contracts := []Contract{
//		{Name: "Token1", Type: "ERC20", IsERC20: true, Data: "Token1 Data"},
//		{Name: "Token2", Type: "ERC20", IsERC20: true, Data: "Token2 Data"},
//		{Name: "NFT1", Type: "ERC721", IsERC721: true, Data: "NFT1 Data"},
//	}
//
//	for _, c := range contracts {
//		dbList.Create(&c)
//	}
//
//	// Query by type (should use partition pruning)
//	var erc20Contracts []Contract
//	dbList.Where("type = ?", "ERC20").Find(&erc20Contracts)
//
//	// Should query only the ERC20 partition
//	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1`, listMiddleware)
//
//	// Verify results
//	tassert.Equal(t, 2, len(erc20Contracts))
//	for _, c := range erc20Contracts {
//		tassert.Equal(t, "ERC20", c.Type)
//	}
//}
//
//func TestListPartitionSelectById(t *testing.T) {
//	truncateTables(dbList, "contracts_0", "contracts_1", "contracts_2")
//	// Create a contract and get its ID
//	contract := Contract{Name: "IdTest", Type: "ERC20", IsERC20: true}
//	dbList.Create(&contract)
//
//	// Query by ID
//	var foundContract Contract
//	dbList.Where("type = ?", "ERC20").First(&foundContract, contract.ID)
//
//	// Should be able to find it in the correct partition even without type in WHERE clause
//	// With list partitioning, this may need to check multiple partitions
//	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 AND contracts_0."id" = $1 ORDER BY "contracts_0"."id" LIMIT 1`, listMiddleware)
//
//	// Verify results
//	tassert.Equal(t, contract.ID, foundContract.ID)
//	tassert.Equal(t, "ERC20", foundContract.Type)
//}
//
//func TestListPartitionUpdate(t *testing.T) {
//	// Create a contract to update
//	contract := Contract{Name: "UpdateTest", Type: "ERC20", IsERC20: true}
//	dbList.Create(&contract)
//
//	// Update the contract
//	dbList.Model(&Contract{}).Where("id = ?", contract.ID).Where("type = ?", "ERC20").Update("name", "UpdatedName")
//
//	// Should update in the correct partition
//	assertQueryResult(t, `UPDATE contracts_0 SET "name" = $1, updated_at = $2 WHERE "id" = $3 AND "type" = $4`, listMiddleware)
//
//	// Verify the update
//	var updated Contract
//	dbList.Where("type = ?", "ERC20").First(&updated, contract.ID)
//	tassert.Equal(t, "UpdatedName", updated.Name)
//}
//
//func TestListPartitionDelete(t *testing.T) {
//	// Create a contract to delete
//	contract := Contract{Name: "DeleteTest", Type: "ERC721", IsERC721: true}
//	dbList.Create(&contract)
//
//	// Delete the contract
//	dbList.Where("type = ?", "ERC721").Delete(&Contract{}, contract.ID)
//
//	// Should delete from the correct partition
//	assertQueryResult(t, `DELETE FROM "contracts_1" WHERE type = $1 AND "contracts_1"."id" = $2`, listMiddleware)
//
//	// Verify deletion
//	var count int64
//	dbList.Model(&Contract{}).Where("type = ?", "ERC721").Where("id = ?", contract.ID).Count(&count)
//	tassert.Equal(t, int64(0), count)
//}
//
//func TestListPartitionJoin(t *testing.T) {
//	// Create contract and contract data
//	contract := Contract{ID: 4, Name: "JoinTest", Type: "ERC20", IsERC20: true}
//	dbList.Create(&contract)
//
//	contractData := ContractData{
//		ContractID: contract.ID,
//		Key:        "symbol",
//		Value:      "TKN",
//	}
//	dbList.Create(&contractData)
//
//	// Query with join
//	var result struct {
//		Contract
//		Value string
//	}
//
//	dbList.Model(&Contract{}).
//		Select("contracts.*, contract_data.value").
//		Joins("JOIN contract_data ON contract_data.contract_id = contracts.id").
//		Where("contracts.id = ?", contract.ID).Where("type = ?", "ERC20").
//		First(&result)
//
//	// Should join the correct partitions
//	assertQueryResult(t, `SELECT contracts_0.*, contract_data_1.value FROM contracts_0 JOIN contract_data_1 ON contract_data_1.contract_id = contracts_0.id WHERE contracts_0.id = $1 AND type = $2 ORDER BY "contracts_0"."id" LIMIT 1`, listMiddleware)
//
//	// Verify join results
//	tassert.Equal(t, contract.ID, result.ID)
//	tassert.Equal(t, "TKN", result.Value)
//}
//
//func TestListPartitionFilterByBoolean(t *testing.T) {
//	// Clear existing data
//	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")
//
//	// Insert test data
//	contracts := []Contract{
//		{Name: "Token1", Type: "ERC20", IsERC20: true},
//		{Name: "NFT1", Type: "ERC721", IsERC721: true},
//		{Name: "MultiToken1", Type: "ERC1155", IsERC1155: true},
//	}
//
//	for _, c := range contracts {
//		dbList.Create(&c)
//	}
//
//	// Query by boolean field
//	var erc721Contracts []Contract
//	dbList.Where("is_erc721 = ?", true).Find(&erc721Contracts)
//
//	// Should query the right partition based on the boolean
//	assertQueryResult(t, `SELECT * FROM contracts_1 WHERE "is_erc721" = $1`, listMiddleware)
//
//	// Verify results
//	tassert.Equal(t, 1, len(erc721Contracts))
//	//tassert.Equal(t, "ERC721", erc721Contracts[0].Type)
//}
//
//func TestListPartitionMultipleFilters(t *testing.T) {
//	// Create test data
//	dbList.Create(&Contract{Name: "Common", Type: "ERC20", IsERC20: true, Data: "Common data"})
//	dbList.Create(&Contract{Name: "Rare", Type: "ERC20", IsERC20: true, Data: "Rare data"})
//
//	// Query with multiple conditions
//	var contracts []Contract
//	dbList.Where("type = ?", "ERC20").Where("name = ?", "Rare").Find(&contracts)
//
//	// Should query the correct partition with all filters
//	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 AND "name" = $2`, listMiddleware)
//
//	// Verify results
//	tassert.Equal(t, 1, len(contracts))
//	tassert.Equal(t, "Rare", contracts[0].Name)
//}
//
//func TestListPartitionOrderAndLimit(t *testing.T) {
//	// Clear existing data
//	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")
//
//	// Create test data
//	for i := 1; i <= 5; i++ {
//		dbList.Create(&Contract{
//			Name:    fmt.Sprintf("Token%d", i),
//			Type:    "ERC20",
//			IsERC20: true,
//		})
//	}
//
//	// Query with order and limit
//	var contracts []Contract
//	dbList.Where("type = ?", "ERC20").Order("name DESC").Limit(3).Find(&contracts)
//
//	// Should have correct ORDER BY and LIMIT
//	assertQueryResult(t, `SELECT * FROM contracts_0 WHERE "type" = $1 ORDER BY name DESC LIMIT 3`, listMiddleware)
//
//	// Verify results
//	tassert.Equal(t, 3, len(contracts))
//	tassert.Equal(t, "Token5", contracts[0].Name) // DESC order
//}
//
//// todo fix this test
////func TestListPartitionCrossShardQuery(t *testing.T) {
////	// Clear existing data
////	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")
////
////	// Create test data across different types
////	dbList.Create(&Contract{Name: "SearchToken", Type: "ERC20", IsERC20: true})
////	dbList.Create(&Contract{Name: "SearchNFT", Type: "ERC721", IsERC721: true})
////	dbList.Create(&Contract{Name: "SearchMulti", Type: "ERC1155", IsERC1155: true})
////
////	// Query across all types without a partition key
////	var contracts []Contract
////	tx := dbList.Where("name LIKE ?", "Search%").Find(&contracts)
////
////	// Should generate a UNION ALL query across all partitions
////	assertPartialQueryResult(t, `UNION ALL`, tx)
////
////	// Verify we found records from multiple partitions
////	tassert.Equal(t, 3, len(contracts))
////
////	// Check we got each type
////	foundTypes := make(map[string]bool)
////	for _, c := range contracts {
////		foundTypes[c.Type] = true
////	}
////	tassert.Equal(t, 3, len(foundTypes))
////}
//
//func TestListPartitionAggregation(t *testing.T) {
//	// Clear existing data
//	dbList.Exec("TRUNCATE TABLE contracts_0, contracts_1, contracts_2")
//
//	// Create test data
//	for i := 1; i <= 3; i++ {
//		dbList.Create(&Contract{Name: fmt.Sprintf("ERC20_%d", i), Type: "ERC20", IsERC20: true})
//		dbList.Create(&Contract{Name: fmt.Sprintf("ERC721_%d", i), Type: "ERC721", IsERC721: true})
//	}
//
//	// Query with aggregation
//	var result struct {
//		Type  string
//		Count int
//	}
//
//	dbList.Model(&Contract{}).
//		Select("type, COUNT(*) as count").
//		Where("type = ?", "ERC20").
//		Group("type").
//		First(&result)
//
//	// Should use the correct partition
//	assertQueryResult(t, `SELECT type, count(*) AS count FROM contracts_0 WHERE "type" = $1 GROUP BY type ORDER BY "type" LIMIT 1`, listMiddleware)
//
//	// Verify aggregation result
//	tassert.Equal(t, "ERC20", result.Type)
//	tassert.Equal(t, 3, result.Count)
//}
//
//func TestListPartitionFallbackToDoubleWrite(t *testing.T) {
//	// We'll use the existing tables and configs for simplicity
//	// This will ensure we use the properly configured snowflake ID generator
//
//	// First, clear the test tables
//	truncateTables(dbList, "contracts", "contracts_0", "contracts_1", "contracts_2")
//
//	// Store original config to restore later
//	originalConfig := listShardingConfig
//	originalDoubleWrite := originalConfig.DoubleWrite
//	originalDefaultPartition := originalConfig.DefaultPartition
//
//	// Use defer to restore the original config when we're done
//	defer func() {
//		listShardingConfig.DoubleWrite = originalDoubleWrite
//		listShardingConfig.DefaultPartition = originalDefaultPartition
//
//		// Re-register the middleware with original settings
//		listConfigs = map[string]Config{
//			"contracts":     listShardingConfig,
//			"contract_data": contractDataConfig,
//		}
//		listMiddleware = Register(listConfigs, &Contract{}, &ContractData{})
//		dbList.Use(listMiddleware)
//	}()
//
//	// CASE 1: Test with double-write enabled and default partition set
//	// ----------------------------------------------------------------
//	// Enable double-write and set a default partition
//	listShardingConfig.DoubleWrite = true
//	listShardingConfig.DefaultPartition = 0 // Default to partition 0
//
//	// Update the config and re-register the middleware
//	listConfigs := map[string]Config{
//		"contracts": listShardingConfig,
//	}
//	testMiddleware := Register(listConfigs, &Contract{})
//
//	// Use a new DB session with our test middleware
//	testDB := dbList.Session(&gorm.Session{})
//	testDB.Use(testMiddleware)
//
//	// 1.1: Insert with valid type (should go to correct partition)
//	contract1 := &Contract{
//		Name:    "TokenA",
//		Type:    "ERC20",
//		IsERC20: true,
//		Data:    "Token data",
//	}
//
//	err := testDB.Create(contract1).Error
//	if err != nil {
//		t.Errorf("Failed to insert contract with valid type: %v", err)
//	} else {
//		t.Logf("Successfully inserted contract with ID: %d", contract1.ID)
//
//		// Check if record exists in the correct partition (contracts_0 for ERC20)
//		// IMPORTANT: Use Raw with Contract model to bypass the middleware constraints
//		var contract Contract
//		err = dbList.Raw("SELECT * FROM contracts_0 WHERE id = ? AND type = ?",
//			contract1.ID, "ERC20").First(&contract).Error
//
//		if err != nil {
//			t.Errorf("Error checking partition: %v", err)
//		} else if contract.ID == contract1.ID {
//			t.Logf("Successfully verified contract exists in partition 0 (ERC20)")
//		} else {
//			t.Errorf("Contract not found in partition 0 (ERC20)")
//		}
//	}
//
//	// 1.2: Insert with invalid type but default partition set
//	contract2 := &Contract{
//		Name: "Unknown Token",
//		Type: "UNKNOWN_TYPE", // Not in list values
//		Data: "Some data",
//	}
//
//	err = testDB.Create(contract2).Error
//	if err != nil {
//		t.Errorf("Failed to insert contract with unknown type: %v", err)
//	} else {
//		t.Logf("Successfully inserted contract with unknown type, ID: %d", contract2.ID)
//
//		// Check main table using Raw with a direct count query
//		var mainCount int64
//		err = dbList.Raw("SELECT COUNT(*) FROM contracts WHERE id = ?", contract2.ID).Scan(&mainCount).Error
//		if err != nil {
//			t.Errorf("Error checking main table: %v", err)
//		} else if mainCount == 1 {
//			t.Logf("Record correctly inserted into main table with DoubleWrite enabled")
//		} else {
//			t.Errorf("Record not found in main table, count: %d", mainCount)
//		}
//
//		// Check default partition using Raw with a direct count query
//		var partitionCount int64
//		err = dbList.Raw("SELECT COUNT(*) FROM contracts_0 WHERE id = ? AND type = ?",
//			contract2.ID, "UNKNOWN_TYPE").Scan(&partitionCount).Error
//		if err != nil {
//			t.Errorf("Error checking default partition: %v", err)
//		} else if partitionCount == 1 {
//			t.Logf("Record correctly inserted into default partition")
//		} else {
//			t.Errorf("Record not found in default partition, count: %d", partitionCount)
//		}
//	}
//
//	// 1.3: Insert with empty type
//	contract3 := &Contract{
//		Name: "Empty Type",
//		Type: "", // Empty type
//		Data: "Empty type data",
//	}
//
//	err = testDB.Create(contract3).Error
//	if err != nil {
//		t.Errorf("Failed to insert contract with empty type: %v", err)
//	} else {
//		t.Logf("Successfully inserted contract with empty type, ID: %d", contract3.ID)
//
//		// Check main table using Raw with a direct count query
//		var mainCount int64
//		err = dbList.Raw("SELECT COUNT(*) FROM contracts WHERE id = ?", contract3.ID).Scan(&mainCount).Error
//		if err != nil {
//			t.Errorf("Error checking main table: %v", err)
//		} else if mainCount == 1 {
//			t.Logf("Record with empty type inserted into main table with DoubleWrite enabled")
//		} else {
//			t.Errorf("Record not found in main table, count: %d", mainCount)
//		}
//
//		// Check default partition with empty type
//		var partitionCount int64
//		err = dbList.Raw("SELECT COUNT(*) FROM contracts_0 WHERE id = ? AND type = ?",
//			contract3.ID, "").Scan(&partitionCount).Error
//		if err != nil {
//			t.Errorf("Error checking default partition: %v", err)
//		} else if partitionCount == 1 {
//			t.Logf("Record correctly inserted into default partition")
//		} else {
//			t.Errorf("Record not found in default partition, count: %d", partitionCount)
//		}
//	}
//
//	// CASE 2: Test with double-write disabled and no default partition
//	// ---------------------------------------------------------------
//	// Disable double-write and remove default partition
//	listShardingConfig.DoubleWrite = false
//	listShardingConfig.DefaultPartition = -1 // No default partition
//
//	// Update the config and re-register the middleware
//	strictConfigs := map[string]Config{
//		"contracts": listShardingConfig,
//	}
//	strictMiddleware := Register(strictConfigs, &Contract{})
//
//	// Use a new DB session with our strict middleware
//	strictDB := dbList.Session(&gorm.Session{})
//	strictDB.Use(strictMiddleware)
//
//	// 2.1: Insert with valid type (should still work)
//	contractValid := &Contract{
//		Name:     "Valid Token",
//		Type:     "ERC721",
//		IsERC721: true,
//		Data:     "Valid data",
//	}
//
//	err = strictDB.Create(contractValid).Error
//	if err != nil {
//		t.Errorf("Failed to insert contract with valid type in strict mode: %v", err)
//	} else {
//		t.Logf("Successfully inserted contract with valid type in strict mode, ID: %d", contractValid.ID)
//
//		// Check correct partition for ERC721 (partition 1)
//		var partitionCount int64
//		err = dbList.Raw("SELECT COUNT(*) FROM contracts_1 WHERE id = ? AND type = ?",
//			contractValid.ID, "ERC721").Scan(&partitionCount).Error
//		if err != nil {
//			t.Errorf("Error checking partition: %v", err)
//		} else if partitionCount == 1 {
//			t.Logf("Successfully verified contract exists in partition 1 (ERC721)")
//		} else {
//			t.Errorf("Contract not found in partition 1 (ERC721), count: %d", partitionCount)
//		}
//	}
//
//	// 2.2: Insert with invalid type (should fail with partition error)
//	contractInvalid := &Contract{
//		Name: "Invalid Token",
//		Type: "INVALID_TYPE",
//		Data: "Invalid data",
//	}
//
//	err = strictDB.Create(contractInvalid).Error
//	if err == nil {
//		t.Errorf("Expected error when inserting invalid type without DoubleWrite, but got success")
//	} else {
//		t.Logf("Correctly failed to insert invalid type without DoubleWrite: %v", err)
//
//		// Verify error mentions partition
//		if strings.Contains(err.Error(), "partition list") ||
//			strings.Contains(err.Error(), "sharding key") {
//			t.Logf("Error correctly mentions partition or sharding key")
//		} else {
//			t.Errorf("Expected error to mention 'partition list' or 'sharding key', got: %v", err)
//		}
//	}
//}
