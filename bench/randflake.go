package bench

import (
	"time"

	"gosuda.org/randflake"
)

func NewRandflake(worker int) (*randflake.Generator, error) {
	now := time.Now().Unix()
	nodeID := int64(worker)
	leaseStart := now
	leaseEnd := now + 3600
	secret := []byte("0123456789ABCDEF")
	return randflake.NewGenerator(nodeID, leaseStart, leaseEnd, secret)
}
