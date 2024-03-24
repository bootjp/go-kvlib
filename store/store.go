package store

import (
	"context"
	"errors"
	"io"
)

var ErrKeyNotFound = errors.New("not found")
var ErrUnknownOp = errors.New("unknown op")
var ErrNotSupported = errors.New("not supported")

type KVPair struct {
	Key   []byte
	Value []byte
}

var Tombstone = []byte{0x00}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}

type ScanStore interface {
	Store
	Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error)
}

type TTLStore interface {
	Store
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
	TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error
}

type ScanTTLStore interface {
	ScanStore
	TTLStore
}

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type ScanTxn interface {
	Txn
	Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error)
}

type TTLTxn interface {
	Txn
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
}

type ScanTTLTxn interface {
	ScanTxn
	TTLTxn
}
