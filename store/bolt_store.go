package store

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"

	"go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")

type boltStore struct {
	log   *slog.Logger
	bbolt *bbolt.DB
}

const mode = 0666

func NewBoltStore(path string) (ScanStore, error) {
	db, err := bbolt.Open(path, mode, nil)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists(defaultBucket)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &boltStore{
		bbolt: db,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}, nil
}

var _ Store = (*boltStore)(nil)
var _ ScanStore = (*boltStore)(nil)

func (s *boltStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.log.InfoContext(ctx, "Get",
		slog.String("key", string(key)),
	)

	var v []byte

	err := s.bbolt.View(func(tx *bbolt.Tx) error {
		v = tx.Bucket(defaultBucket).Get(key)
		return nil
	})

	return v, err
}

func (s *boltStore) Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	s.log.InfoContext(ctx, "Scan",
		slog.String("start", string(start)),
		slog.String("end", string(end)),
		slog.Int("limit", limit),
	)

	var res []*KVPair

	err := s.bbolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		for k, v := c.Seek(start); k != nil && (end == nil || bytes.Compare(k, end) < 0); k, v = c.Next() {
			res = append(res, &KVPair{
				Key:   k,
				Value: v,
			})
			if len(res) >= limit {
				break
			}
		}
		return nil
	})

	return res, err
}

func (s *boltStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.log.InfoContext(ctx, "put",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
	)

	err := s.bbolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(defaultBucket)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})

	return err
}

func (s *boltStore) Delete(ctx context.Context, key []byte) error {
	s.log.InfoContext(ctx, "Delete",
		slog.String("key", string(key)),
	)

	err := s.bbolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}

		return b.Delete(key)
	})

	return err
}

func (s *boltStore) Exists(ctx context.Context, key []byte) (bool, error) {
	s.log.InfoContext(ctx, "exists",
		slog.String("key", string(key)),
	)

	var v []byte
	err := s.bbolt.View(func(tx *bbolt.Tx) error {
		v = tx.Bucket(defaultBucket).Get(key)
		return nil
	})

	return v != nil, err
}

type boltStoreTxn struct {
	bucket *bbolt.Bucket
}

func (s *boltStore) NewBoltStoreTxn(tx *bbolt.Tx) Txn {
	return &boltStoreTxn{
		bucket: tx.Bucket(defaultBucket),
	}
}

func (t *boltStoreTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	return t.bucket.Get(key), nil
}

func (t *boltStoreTxn) Put(_ context.Context, key []byte, value []byte) error {
	return t.bucket.Put(key, value)
}

func (t *boltStoreTxn) Delete(_ context.Context, key []byte) error {
	return t.bucket.Delete(key)
}
func (t *boltStoreTxn) Exists(_ context.Context, key []byte) (bool, error) {
	return t.bucket.Get(key) != nil, nil
}

func (s *boltStore) Txn(ctx context.Context, fn func(ctx context.Context, txn Txn) error) error {
	btxn, err := s.bbolt.Begin(true)
	if err != nil {
		return err
	}

	txn := s.NewBoltStoreTxn(btxn)
	err = fn(ctx, txn)
	if err != nil {
		return err
	}

	return btxn.Commit()
}

func (s *boltStore) Close() error {
	return s.bbolt.Close()
}

func (s *boltStore) Snapshot() (io.ReadWriter, error) {
	return nil, ErrNotSupported
}

func (s *boltStore) Restore(_ io.Reader) error {
	return ErrNotSupported
}
