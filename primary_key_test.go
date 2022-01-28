package sharding

import (
	"testing"

	"github.com/longbridgeapp/assert"
)

func Test_pgSeqName(t *testing.T) {
	assert.Equal(t, "gorm_sharding_users_id_seq", pgSeqName("users"))
}
