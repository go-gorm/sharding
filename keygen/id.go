package keygen

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// | 1bit reserved | 41 bits timestamp | 6 bits worker ｜9 bits table ｜ 7 bits sequence |
const (
	timeLeft   = uint8(22)
	workerLeft = uint8(16)
	tableLeft  = uint8(7)
	twepoch    = int64(1624204800000) // start from 2021-06-21
	seqMax     = 127
)

type Sequence struct {
	number    int64
	timestamp int64
	sync.Mutex
}

func (s *Sequence) next(timestamp int64) int64 {
	s.Lock()
	defer s.Unlock()

	if timestamp > s.timestamp {
		s.timestamp = timestamp
		s.number = 1
	} else {
		s.number = s.number + 1
	}

	return s.number
}

var sequence Sequence
var worker int64

func init() {
	ipv4, err := getIPv4()
	if err != nil {
		log.Fatal(err)
	}
	worker = int64(ipv4[3]) % 64
	sequence = Sequence{}
}

// Next generate a distributed Primary Key
// tableIdx - table sharding index
func Next(tableIdx int64) int64 {
	var now int64
	var seq int64

	for i := 0; i <= 10; i++ {
		now = time.Now().UnixNano() / 1e6
		seq = sequence.next(now)
		if seq <= seqMax {
			break
		}
		time.Sleep(time.Microsecond * 100)
	}

	return int64(((now - twepoch) << timeLeft) |
		(worker << workerLeft) |
		(tableIdx << tableLeft) |
		seq)
}

// getWorkerNumber get the worker number from id
func getWorkerNumber(id int64) int {
	return int(id >> int64(workerLeft) & 63)
}

// TableIdx get the table index from id
// Give a ID return idx of table shard
func TableIdx(id int64) int {
	return int(id >> int64(tableLeft) & 511)
}

// getIPv4 get the IPv4 address
func getIPv4() (ip net.IP, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			ipv4 := ipnet.IP.To4()
			if ipv4 != nil {
				return ipv4, nil
			}
		}
	}

	return nil, errors.New("can not get ipv4")
}
