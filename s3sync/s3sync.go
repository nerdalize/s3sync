package s3sync

import (
	"crypto/sha256"
	"fmt"
)

type K [sha256.Size]byte

func (k K) ToString() string {
	return fmt.Sprintf("%x", k)
}

var ZeroKey = K{}

type KeyWriter interface {
	Write(k K) error
}

type KeyReader interface {
	Read() (K, error)
}
