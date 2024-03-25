package store

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mustStore[T any](store T, err error) T {
	if err != nil {
		panic(err)
	}
	return store
}

func TestBoltStore(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	d := t.TempDir()
	st := mustStore(NewBoltStore(d + "/bolt.db"))

	for i := 0; i < 999; i++ {
		key := []byte("foo" + strconv.Itoa(i))
		err := st.Put(ctx, key, []byte("bar"))
		assert.NoError(t, err)

		res, err := st.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, []byte("bar"), res)
		assert.NoError(t, st.Delete(ctx, key))
		// bolt store does not support NotFound
		res, err = st.Get(ctx, []byte("aaaaaa"))
		assert.NoError(t, err)
		assert.Nil(t, res)
	}
}

func TestBoltStore_Scan(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	st := mustStore(NewBoltStore(t.TempDir() + "/bolt.db"))

	for i := 0; i < 999; i++ {
		keyStr := "prefix " + strconv.Itoa(i) + "foo"
		key := []byte(keyStr)
		b := make([]byte, 8)
		binary.PutVarint(b, int64(i))
		err := st.Put(ctx, key, b)
		assert.NoError(t, err)
	}

	res, err := st.Scan(ctx, []byte("prefix"), []byte("z"), 100)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	sortedKVPairs := make([]*KVPair, 999)

	for _, re := range res {
		str := string(re.Key)
		i, err := strconv.Atoi(str[7 : len(str)-3])
		assert.NoError(t, err)
		sortedKVPairs[i] = re
	}

	cnt := 0
	for i, v := range sortedKVPairs {
		if v == nil {
			continue
		}
		cnt++
		n, _ := binary.Varint(v.Value)
		assert.NoError(t, err)

		assert.Equal(t, int64(i), n)
		assert.Equal(t, []byte("prefix "+strconv.Itoa(i)+"foo"), v.Key)
	}

	assert.Equal(t, 100, cnt)
}

func TestBoltStore_Txn(t *testing.T) {
	t.Parallel()
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			return nil
		})
		assert.NoError(t, err)
	})
	t.Run("error", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
			err := txn.Put(ctx, []byte("foo"), []byte("bar"))
			assert.NoError(t, err)

			res, err := txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)

			assert.Equal(t, []byte("bar"), res)
			assert.NoError(t, txn.Delete(ctx, []byte("foo")))

			res, err = txn.Get(ctx, []byte("foo"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			res, err = txn.Get(ctx, []byte("aaaaaa"))
			assert.NoError(t, err)
			assert.Nil(t, res)
			return errors.New("error")
		})
		assert.Error(t, err)
	})

	t.Run("parallel", func(t *testing.T) {
		ctx := context.Background()
		d := t.TempDir()
		st := mustStore(NewBoltStore(d + "bolt.db"))
		wg := &sync.WaitGroup{}

		for i := 0; i < 9999; i++ {
			wg.Add(1)
			go func(i int) {
				key := []byte("foo" + strconv.Itoa(i))
				err := st.Txn(ctx, func(ctx context.Context, txn Txn) error {
					err := txn.Put(ctx, key, []byte("bar"))
					assert.NoError(t, err)

					res, err := txn.Get(ctx, key)
					assert.NoError(t, err)

					assert.Equal(t, []byte("bar"), res)
					assert.NoError(t, txn.Delete(ctx, key))

					res, err = txn.Get(ctx, key)
					assert.NoError(t, err)
					assert.Nil(t, res)
					res, err = txn.Get(ctx, []byte("aaaaaa"))
					assert.NoError(t, err)
					assert.Nil(t, res)
					return nil
				})
				assert.NoError(t, err)
				wg.Done()
			}(i)
		}
		wg.Wait()
	})
}
