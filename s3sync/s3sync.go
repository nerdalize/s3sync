package s3sync

import "crypto/sha256"

type K [sha256.Size]byte

var ZeroKey = K{}

type KeyWriter interface {
	Write(k K) error
}

type KeyReader interface {
	Read() (K, error)
}
