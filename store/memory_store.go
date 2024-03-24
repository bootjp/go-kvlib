package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spaolacci/murmur3"
)

type memoryStore struct {
	mtx sync.RWMutex
	// key -> value
	m map[uint64][]byte
	// key -> ttl
	ttl map[uint64]int64
	log *slog.Logger

	expire *time.Ticker
}

const (
	defaultExpireInterval = 30 * time.Second
)

func NewMemoryStore() Store {
	m := &memoryStore{
		mtx: sync.RWMutex{},
		m:   map[uint64][]byte{},
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),

		ttl: nil,
	}

	m.expire = nil

	return m
}

var _ TTLStore = (*memoryStore)(nil)

func NewMemoryStoreWithExpire(interval time.Duration) TTLStore {
	//nolint:forcetypeassert
	m := NewMemoryStore().(*memoryStore)
	m.expire = time.NewTicker(interval)
	m.ttl = map[uint64]int64{}

	go func() {
		for range m.expire.C {
			m.cleanExpired()
		}
	}()
	return m
}

func NewMemoryStoreDefaultTTL() TTLStore {
	return NewMemoryStoreWithExpire(defaultExpireInterval)
}

var _ Store = &memoryStore{}

func (s *memoryStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h, err := s.hash(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s.log.InfoContext(ctx, "Get",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
	)

	v, ok := s.m[h]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (s *memoryStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	s.m[h] = value
	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
		slog.String("value", string(value)),
	)

	return nil
}

func (s *memoryStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	delete(s.m, h)
	s.log.InfoContext(ctx, "Delete",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
	)

	return nil
}
func (s *memoryStore) Exists(ctx context.Context, key []byte) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h, err := s.hash(key)
	if err != nil {
		return false, errors.WithStack(err)
	}

	_, ok := s.m[h]
	return ok, nil
}

func (s *memoryStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTxn()

	err := f(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.m[op.h] = op.v
		case OpTypeDelete:
			delete(s.m, op.h)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}

	return nil
}

func (s *memoryStore) TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTTLTxn()

	err := f(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.m[op.h] = op.v
			s.ttl[op.h] = op.ttl
		case OpTypeDelete:
			delete(s.m, op.h)
			delete(s.ttl, op.h)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}

	return nil
}

func (s *memoryStore) hash(key []byte) (uint64, error) {
	h := murmur3.New64()
	if _, err := h.Write(key); err != nil {
		return 0, errors.WithStack(err)
	}
	return h.Sum64(), nil
}

func (s *memoryStore) Close() error {
	return nil
}

func (s *memoryStore) Snapshot() (io.ReadWriter, error) {
	s.mtx.RLock()
	cl := make(map[uint64][]byte, len(s.m))
	for k, v := range s.m {
		cl[k] = v
	}
	s.mtx.RUnlock()

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(cl)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return buf, nil
}
func (s *memoryStore) Restore(buf io.Reader) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.m = make(map[uint64][]byte)
	err := gob.NewDecoder(buf).Decode(&s.m)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *memoryStore) Expire(ctx context.Context, key []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	s.ttl[h] = time.Now().Unix() + ttl
	s.log.InfoContext(ctx, "Expire",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
		slog.Int64("ttl", ttl),
	)

	return nil
}

func (s *memoryStore) PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	s.m[h] = value
	s.ttl[h] = time.Now().Unix() + ttl
	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
		slog.String("value", string(value)),
	)

	return nil
}

func (s *memoryStore) cleanExpired() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now().Unix()
	for k, v := range s.ttl {
		if v > now {
			continue
		}
		delete(s.m, k)
		delete(s.ttl, k)
	}
}

type OpType uint8

const (
	OpTypePut OpType = iota
	OpTypeDelete
)

type memOp struct {
	opType OpType
	h      uint64
	v      []byte
	ttl    int64
}

type memoryStoreTxn struct {
	mu *sync.RWMutex
	// Memory Structure during Transaction
	m map[uint64][]byte
	// Time series operations during a transaction
	ops []memOp
	s   *memoryStore
}

func (s *memoryStore) NewTxn() *memoryStoreTxn {
	return &memoryStoreTxn{
		mu:  &sync.RWMutex{},
		m:   map[uint64][]byte{},
		ops: []memOp{},
		s:   s,
	}
}

func (s *memoryStore) NewTTLTxn() *memoryStoreTxn {
	return &memoryStoreTxn{
		mu:  &sync.RWMutex{},
		m:   map[uint64][]byte{},
		ops: []memOp{},
		s:   s,
	}
}

func (t *memoryStoreTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	h, err := t.s.hash(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	v, ok := t.m[h]
	if ok && !bytes.Equal(v, Tombstone) {
		return v, nil
	}

	// Returns NotFound if deleted during transaction and then get
	if bytes.Equal(v, Tombstone) {
		return nil, ErrKeyNotFound
	}

	v, ok = t.s.m[h]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (t *memoryStoreTxn) Put(_ context.Context, key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	t.m[h] = value
	t.ops = append(t.ops, memOp{
		h:      h,
		opType: OpTypePut,
		v:      value,
	})
	return nil
}

func (t *memoryStoreTxn) Delete(_ context.Context, key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	t.m[h] = Tombstone
	t.ops = append(t.ops, memOp{
		h:      h,
		opType: OpTypeDelete,
	})

	return nil
}

func (t *memoryStoreTxn) Exists(_ context.Context, key []byte) (bool, error) {
	h, err := t.s.hash(key)
	if err != nil {
		return false, errors.WithStack(err)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	_, ok := t.m[h]
	if ok {
		return true, nil
	}

	// Returns false if deleted during transaction
	for _, op := range t.ops {
		if op.h != h {
			continue
		}
		if op.opType == OpTypeDelete {
			return false, nil
		}
	}

	_, ok = t.s.m[h]
	return ok, nil
}

func (t *memoryStoreTxn) Expire(_ context.Context, key []byte, ttl int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	for i, o := range t.ops {
		if o.h != h {
			continue
		}
		t.ops[i].ttl = ttl
		return nil
	}

	return errors.WithStack(ErrKeyNotFound)
}

func (t *memoryStoreTxn) PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	t.m[h] = value
	t.ops = append(t.ops, memOp{
		h:      h,
		opType: OpTypePut,
		v:      value,
		ttl:    ttl,
	})

	return nil
}
