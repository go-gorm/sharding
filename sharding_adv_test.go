package sharding

import (
	"fmt"
	tassert "github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"testing"
	"time"
)

type TokenWithHashPartition struct {
	ID             int64  `gorm:"primarykey"`
	Contract       string `gorm:"index:idx_contract"`
	TokenID        string `gorm:"index:idx_token_id"`
	TokenURIStatus string
	TokenURI       string
	Name           string
	Description    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ContractWithHashPartition represents a blockchain contract with hash partitioning
type ContractWithHashPartition struct {
	ID        int64  `gorm:"primarykey"`
	Address   string `gorm:"uniqueIndex"`
	Name      string `gorm:"index:idx_name"`
	Type      string `gorm:"index:idx_type"`
	IsERC20   bool
	IsERC721  bool
	IsERC1155 bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestHashPartitioningWithLowerFunction(t *testing.T) {
	// Create a test DB with proper configuration
	testDB, err := gorm.Open(postgres.New(dbConfig), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		Logger:                                   logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Set up hash partitioning with contract as the sharding key for tokens
	hashTokenConfig := Config{
		DoubleWrite:         false,
		ShardingKey:         "contract",
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Set up hash partitioning with address as the sharding key for contracts
	hashContractConfig := Config{
		DoubleWrite:         false,
		ShardingKey:         "address",
		PartitionType:       PartitionTypeHash,
		NumberOfShards:      4,
		ShardingAlgorithm:   shardingHasher4Algorithm,
		PrimaryKeyGenerator: PKSnowflake,
		ShardingSuffixs: func() []string {
			return []string{"_0", "_1", "_2", "_3"}
		},
	}

	// Register the middleware with both configurations
	hashConfigs := map[string]Config{
		"token_with_hash_partitions":    hashTokenConfig,
		"contract_with_hash_partitions": hashContractConfig,
	}

	hashMiddleware := Register(hashConfigs, &TokenWithHashPartition{}, &ContractWithHashPartition{})
	testDB.Use(hashMiddleware)

	// Drop and recreate tables
	testDB.Exec("DROP TABLE IF EXISTS token_with_hash_partitions")
	testDB.Exec("DROP TABLE IF EXISTS contract_with_hash_partitions")
	for i := 0; i < 4; i++ {
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS token_with_hash_partitions_%d", i))
		testDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS contract_with_hash_partitions_%d", i))
	}

	// Auto migrate to create the tables
	err = testDB.AutoMigrate(&TokenWithHashPartition{}, &ContractWithHashPartition{})
	if err != nil {
		t.Fatalf("Failed to migrate tables: %v", err)
	}

	// Create sharded tables manually
	for i := 0; i < 4; i++ {
		// Create token tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS token_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			contract text,
			token_id text,
			token_uri_status text,
			token_uri text,
			name text,
			description text,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))

		// Create contract tables
		testDB.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS contract_with_hash_partitions_%d (
			id bigint PRIMARY KEY,
			address text,
			name text,
			type text,
			is_erc20 boolean,
			is_erc721 boolean,
			is_erc1155 boolean,
			created_at timestamp with time zone,
			updated_at timestamp with time zone
		)`, i))
	}

	// Insert test contracts with different case variations of the same name
	contracts := []ContractWithHashPartition{
		{Address: "0xabc123", Name: "MTKN", Type: "ERC721", IsERC721: true},
		{Address: "0xdef456", Name: "mtkn", Type: "ERC721", IsERC721: true},
		{Address: "0xghi789", Name: "Mtkn", Type: "ERC721", IsERC721: true},
		{Address: "0xjkl012", Name: "MTkn", Type: "ERC721", IsERC721: true},
		{Address: "0xmno345", Name: "DifferentToken", Type: "ERC721", IsERC721: true},
	}

	// Insert the contracts
	for _, contract := range contracts {
		err := testDB.Create(&contract).Error
		tassert.NoError(t, err, "Failed to insert contract")
		t.Logf("Created contract with address %s, name %s, ID %d", contract.Address, contract.Name, contract.ID)
	}

	// Insert tokens for each contract
	for _, contract := range contracts {
		// Create multiple tokens per contract
		for i := 1; i <= 3; i++ {
			token := TokenWithHashPartition{
				Contract:       contract.Address,
				TokenID:        fmt.Sprintf("%d", i),
				TokenURIStatus: "READY",
				Name:           fmt.Sprintf("%s #%d", contract.Name, i),
				Description:    fmt.Sprintf("Token %d for contract %s", i, contract.Name),
				CreatedAt:      time.Now(),
				UpdatedAt:      time.Now(),
			}

			err := testDB.Create(&token).Error
			tassert.NoError(t, err, "Failed to insert token")
			t.Logf("Created token with ID %d, contract %s, tokenID %s", token.ID, token.Contract, token.TokenID)
		}
	}

	// Test 1: Join contracts and tokens using LOWER function with contract address as sharding key
	t.Run("JoinWithLowerFunctionAndContractAddress", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// First, pick a specific contract address to query with
		targetContract := contracts[0]

		// Execute the query with a specific contract address (sharding key) and LOWER function
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract = ?", targetContract.Address).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify no errors and we found the right results
		tassert.NoError(t, err, "Query should execute without errors")
		tassert.Equal(t, 3, len(results), "Should find 3 tokens for the contract")

		t.Logf("Last query: %s", hashMiddleware.LastQuery())

		// Verify the data
		for _, result := range results {
			t.Logf("Found token: ID=%d, Contract=%s, TokenID=%s, ContractName=%s",
				result.ID, result.Contract, result.TokenID, result.ContractName)
			tassert.Equal(t, targetContract.Address, result.Contract, "Contract address should match")
		}
	})

	// Test 2: Query all MTKN tokens across all contracts (simulates your error case)
	t.Run("QueryAllMTKNTokens", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Execute the query without a specific contract
		err := testDB.
			Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify no errors with nosharding hint
		tassert.NoError(t, err, "Query with nosharding should execute without errors")

		// Should find tokens for all 4 contract variations of "MTKN"
		t.Logf("Found %d tokens for MTKN contracts with nosharding", len(results))
		tassert.GreaterOrEqual(t, len(results), 12, "Should find at least 12 tokens (3 tokens × 4 contracts)")
	})

	// Test 3: Query by specific token ID and contract address
	t.Run("QueryByTokenIDAndContract", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Pick a specific contract and token ID
		targetContract := contracts[0]
		targetTokenID := "1"

		// Query with both contract address and token ID
		err := testDB.Table("token_with_hash_partitions").
			Select("token_with_hash_partitions.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partitions.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract = ?", targetContract.Address).
			Where("token_with_hash_partitions.token_id = ?", targetTokenID).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		// Verify results
		tassert.NoError(t, err, "Query should execute without errors")
		tassert.Equal(t, 1, len(results), "Should find exactly 1 token")

		t.Logf("Last query: %s", hashMiddleware.LastQuery())

		if len(results) > 0 {
			t.Logf("Found token: ID=%d, Contract=%s, TokenID=%s, ContractName=%s",
				results[0].ID, results[0].Contract, results[0].TokenID, results[0].ContractName)
			tassert.Equal(t, targetContract.Address, results[0].Contract, "Contract address should match")
			tassert.Equal(t, targetTokenID, results[0].TokenID, "Token ID should match")
		}
	})

	// Test 4: Query with IN clause for contract addresses
	t.Run("QueryWithContractAddressesIN", func(t *testing.T) {
		var results []struct {
			TokenWithHashPartition
			ContractName string
		}

		// Get addresses for contracts with "MTKN" variations
		var targetAddresses []string
		for _, c := range contracts[:4] { // First 4 are MTKN variations
			targetAddresses = append(targetAddresses, c.Address)
		}

		// Query using IN clause for contract addresses + LOWER() function
		// This requires nosharding because we're querying across shards
		err := testDB.
			Table("token_with_hash_partitions").
			Select("token_with_hash_partition.*, contract_with_hash_partitions.name as contract_name").
			Joins("JOIN contract_with_hash_partitions ON token_with_hash_partition.contract = contract_with_hash_partitions.address").
			Where("token_with_hash_partitions.contract IN ?", targetAddresses).
			Where("LOWER(contract_with_hash_partitions.name) = LOWER(?)", "MTKN").
			Find(&results).Error

		tassert.NoError(t, err, "Query with nosharding and IN clause should execute without errors")
		tassert.Equal(t, 12, len(results), "Should find 12 tokens (3 tokens × 4 contracts)")

		t.Logf("Found %d tokens with contract addresses IN clause", len(results))
	})

	// Test 5: Alternative approach with individual queries per contract
	t.Run("IndividualQueriesPerContract", func(t *testing.T) {
		// Get all contract addresses with "MTKN" (case-insensitive)
		var mtkContracts []ContractWithHashPartition
		err := testDB.
			Where("LOWER(name) = LOWER(?)", "MTKN").
			Find(&mtkContracts).Error
		tassert.NoError(t, err, "Query for contracts should succeed")

		// For each contract, query the tokens
		var allResults []TokenWithHashPartition
		for _, contract := range mtkContracts {
			var contractTokens []TokenWithHashPartition
			err := testDB.Where("contract = ?", contract.Address).Find(&contractTokens).Error
			tassert.NoError(t, err, "Query for tokens should succeed")

			t.Logf("Found %d tokens for contract %s (%s)", len(contractTokens), contract.Address, contract.Name)
			allResults = append(allResults, contractTokens...)
		}

		tassert.Equal(t, 12, len(allResults), "Should find 12 tokens total")
	})
}
