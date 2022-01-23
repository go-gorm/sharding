package keygen

import (
	"testing"
	"time"

	"github.com/longbridgeapp/assert"
)

func TestNext(t *testing.T) {
	id := Next(24)
	ipv4, err := getIPv4()

	assert.Nil(t, err)
	assert.Equal(t, true, time.Now().UnixNano()/1e6 >= int64(id>>int64(timeLeft))+twepoch)
	assert.Equal(t, int(ipv4[3])%64, getWorkerNumber(id))
	assert.Equal(t, 24, TableIdx(id))
}

func TestNextWithLargerCheck(t *testing.T) {
	var lastId int64
	tableIdx := int64(1)

	for i := 0; i < 10000; i++ {
		newId := Next(tableIdx)

		if lastId >= newId {
			t.Errorf("Expect new: %d > last: %d, but not.", newId, lastId)
			break
		}

		lastId = newId
	}
}

func BenchmarkID(b *testing.B) {
	idx := int64(1)
	for i := 0; i < b.N; i++ {
		Next(idx)
	}
}
