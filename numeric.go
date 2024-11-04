package sharding

import "math/big"

type Numeric struct {
	I *big.Int
}
type UInt256 struct {
	Numeric
}

func (u *UInt256) ToInt64() int64 {
	return u.I.Int64()
}
func (n *Numeric) ToInt64() int64 {
	return n.I.Int64()
}
