package bolt

import (
	"bytes"
	"context"
	"fmt"
	"github.com/SirMetathyst/go-kv"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
	"log"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestKVBolt_StoreKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.StoreKV(context.Background(), nil)
	assert.Nil(t, err)

	err = db.StoreKV(context.Background(), kv.Bucket{})
	assert.Nil(t, err)

	err = db.StoreKV(context.Background(), kv.Bucket{}, kv.Pair{})
	assert.Nil(t, err)

	err = db.StoreKV(context.Background(), nil, kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_StoreKV_DoesNotErrorWithoutKV(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.StoreKV(context.Background(), kv.Bucket("default"))
	assert.Nil(t, err)

	err = db.StoreKV(context.Background(), kv.Bucket("default"), kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_StoreKV_CreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("value3")},
	}

	err := db.StoreKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_StoreKV_OverwritesKeysWhenTheyExistAndCreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("value3")},
	}

	err := db.StoreKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key3"), kv.Value("new_value3")}, // Overwrite
		{kv.Key("key4"), kv.Value("new_value4")}, // Overwrite
		{kv.Key("key5"), kv.Value("value5")},     // Create
		{kv.Key("key6"), kv.Value("value6")},     // Create
	}

	err = db.StoreKV(context.Background(), bucket, newData...)
	assert.Nil(t, err)

	allPairs := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("new_value3")},
		{kv.Key("key4"), kv.Value("new_value4")},
		{kv.Key("key5"), kv.Value("value5")},
		{kv.Key("key6"), kv.Value("value6")},
	}

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(allPairs)...)
	assert.Nil(t, err)
	assert.Exactly(t, allPairs, newList)
}

func TestKVBolt_StoreKV_OverwritesKeysWhenTheyExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("value3")},
	}

	err := db.StoreKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Overwrite
		{kv.Key("key2"), kv.Value("new_value2")}, // Overwrite
		{kv.Key("key3"), kv.Value("new_value3")}, // Overwrite
	}

	err = db.StoreKV(context.Background(), bucket, newData...)
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(newData)...)
	assert.Nil(t, err)
	assert.Exactly(t, newData, newList)
}

func TestKVBolt_StoreKV_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.StoreKV(ctx, bucket, data...)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_StoreKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.StoreKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_StoreKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.StoreKVFn(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = db.StoreKVFn(context.Background(), kv.Bucket{}, nil)
	assert.Nil(t, err)

	err = db.StoreKVFn(context.Background(), kv.Bucket{}, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)

	err = db.StoreKVFn(context.Background(), nil, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_StoreKVFn_CreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.StoreKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

// todo: three states: none exist? some exist? all exist?
// todo: storekv: createAll, overwriteSome, overwriteAll

func TestKVBolt_StoreKVFn_OverwritesKeysWhenTheyExistAndCreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.StoreKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key3"), kv.Value("new_value3")}, // Overwrite
		{kv.Key("key4"), kv.Value("new_value4")}, // Overwrite
		{kv.Key("key5"), kv.Value("value5")},     // Create
		{kv.Key("key6"), kv.Value("value6")},     // Create
	}

	err = db.StoreKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	allPairs := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("new_value3")},
		{kv.Key("key4"), kv.Value("new_value4")},
		{kv.Key("key5"), kv.Value("value5")},
	}

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(allPairs)...)
	assert.Nil(t, err)
	assert.Exactly(t, allPairs, newList)
}

func TestKVBolt_StoreKVFn_OverwritesKeysWhenTheyExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.StoreKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Overwrite
		{kv.Key("key2"), kv.Value("new_value2")}, // Overwrite
		{kv.Key("key3"), kv.Value("new_value3")}, // Overwrite
	}

	err = db.StoreKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(newData)...)
	assert.Nil(t, err)
	assert.Exactly(t, newData, newList)
}

func TestKVBolt_StoreKVFn_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.StoreKVFn(ctx, bucket, func(ctx kv.PutContext) error {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := ctx.Put(data[i].Key, data[i].Value); err != nil {
						return err
					}
					i++
					if i >= len(data) {
						return nil
					}
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_CreateKV_DoesNotErrorWithoutKV(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.CreateKV(context.Background(), kv.Bucket("default"))
	assert.Nil(t, err)

	err = db.CreateKV(context.Background(), kv.Bucket("default"), kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_CreateKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.CreateKV(context.Background(), nil)
	assert.Nil(t, err)

	err = db.CreateKV(context.Background(), kv.Bucket{})
	assert.Nil(t, err)

	err = db.CreateKV(context.Background(), kv.Bucket{}, kv.Pair{})
	assert.Nil(t, err)

	err = db.CreateKV(context.Background(), nil, kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_CreateKV_CreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_CreateKV_FailsWhenAllKeysExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Exists
		{kv.Key("key2"), kv.Value("new_value2")}, // Exists
		{kv.Key("key3"), kv.Value("new_value3")}, // Exists
	}

	err = db.CreateKV(context.Background(), bucket, newData...)
	assert.ErrorIs(t, err, kv.ErrKeyFound)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, newList)
}

func TestKVBolt_CreateKV_FailsWhenEvenOneKeyExists(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Exists
		{kv.Key("key3"), kv.Value("value3")}, // Exists
		{kv.Key("key4"), kv.Value("value4")}, // Create
	}

	err = db.CreateKV(context.Background(), bucket, newData...)
	assert.ErrorIs(t, err, kv.ErrKeyFound)

	newList, err := db.ReadKV(context.Background(), bucket, kv.Key("key1"), kv.Key("key4"))
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, newList)
}

func TestKVBolt_CreateKV_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.CreateKV(ctx, bucket, data...)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_CreateKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.CreateKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_CreateKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.CreateKVFn(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = db.CreateKVFn(context.Background(), kv.Bucket{}, nil)
	assert.Nil(t, err)

	err = db.CreateKVFn(context.Background(), kv.Bucket{}, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)

	err = db.CreateKVFn(context.Background(), nil, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_CreateKVFn_CreatesKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			if err := ctx.Put(n.Key, n.Value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_CreateKVFn_FailsWhenAllKeysExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Exists
		{kv.Key("key2"), kv.Value("new_value2")}, // Exists
		{kv.Key("key3"), kv.Value("new_value3")}, // Exists
	}

	err = db.CreateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			if err := ctx.Put(n.Key, n.Value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.ErrorIs(t, err, kv.ErrKeyFound)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, newList)
}

func TestKVBolt_CreateKVFn_FailsWhenEvenOneKeyExists(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Exists
		{kv.Key("key3"), kv.Value("value3")}, // Exists
		{kv.Key("key4"), kv.Value("value4")}, // Create
	}

	err = db.CreateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			if err := ctx.Put(n.Key, n.Value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.ErrorIs(t, err, kv.ErrKeyFound)

	newList, err := db.ReadKV(context.Background(), bucket, kv.Key("key1"), kv.Key("key4"))
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, newList)
}

func TestKVBolt_CreateKVFn_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.CreateKVFn(ctx, bucket, func(ctx kv.PutContext) error {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := ctx.Put(data[i].Key, data[i].Value); err != nil {
						return err
					}
					i++
					if i >= len(data) {
						return nil
					}
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_ReadKV_DoesNotErrorWithoutKV(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	list, err := db.ReadKV(context.Background(), kv.Bucket("default"))
	assert.Nil(t, err)
	assert.Nil(t, list)

	list, err = db.ReadKV(context.Background(), kv.Bucket("default"), [][]byte{}...)
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_ReadKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.ReadKVFn(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), kv.Bucket{}, nil)
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), kv.Bucket{}, func(ctx kv.GetContext) error { return nil })
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), nil, func(ctx kv.GetContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_ReadKV_StopsExecutionWhenContextIsCanceledAndDoesNotReturnList(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		list, err := db.ReadKV(ctx, bucket, extractKeys(data)...)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, list)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()
}

func TestKVBolt_ReadKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.ReadKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_ReadKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.ReadKVFn(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), kv.Bucket{}, nil)
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), kv.Bucket{}, func(ctx kv.GetContext) error { return nil })
	assert.Nil(t, err)

	err = db.ReadKVFn(context.Background(), nil, func(ctx kv.GetContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_ReadKVFn_ReturnsAllKeyValuesWhenTheyAllExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},
		{kv.Key("key2"), kv.Value("value2")},
		{kv.Key("key3"), kv.Value("value3")},
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	var list []kv.Pair
	err = db.ReadKVFn(context.Background(), bucket, func(ctx kv.GetContext) error {
		for _, n := range data {
			v, err := ctx.Get(n.Key, true)
			if err != nil {
				return err
			}
			list = append(list, kv.Pair{Key: n.Key, Value: v})
		}
		return nil
	})
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_ReadKVFn_StopsExecutionWhenContextIsCanceled(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		var list []kv.Pair
		err := db.ReadKVFn(ctx, bucket, func(ctx kv.GetContext) error {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					v, err := ctx.Get(data[i].Key, true)
					if err != nil {
						return err
					}
					list = append(list, kv.Pair{Key: data[i].Key, Value: v})
					i++
					if i >= len(data) {
						return nil
					}
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, list)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()
}

func TestKVBolt_UpdateKV_DoesNotErrorWithoutKV(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.UpdateKV(context.Background(), kv.Bucket("default"))
	assert.Nil(t, err)

	err = db.UpdateKV(context.Background(), kv.Bucket("default"), kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_UpdateKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.UpdateKV(context.Background(), nil)
	assert.Nil(t, err)

	err = db.UpdateKV(context.Background(), kv.Bucket{})
	assert.Nil(t, err)

	err = db.UpdateKV(context.Background(), kv.Bucket{}, kv.Pair{})
	assert.Nil(t, err)

	err = db.UpdateKV(context.Background(), nil, kv.Pair{})
	assert.Nil(t, err)
}

func TestKVBolt_UpdateKV_FailsToUpdateKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Does Not Exist
		{kv.Key("key2"), kv.Value("value2")}, // Does Not Exist
		{kv.Key("key3"), kv.Value("value3")}, // Does Not Exist
	}

	err := db.UpdateKV(context.Background(), bucket, data...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, list)
}

func TestKVBolt_UpdateKV_UpdatesAllKeysWhenTheyAllExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Exists
		{kv.Key("key2"), kv.Value("new_value2")}, // Exists
		{kv.Key("key3"), kv.Value("new_value3")}, // Exists
	}

	err = db.UpdateKV(context.Background(), bucket, newData...)
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, newData, newList)
}

func TestKVBolt_UpdateKV_FailsWhenEvenOneKeyDoesNotExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},     // Does Not Exist
		{kv.Key("key2"), kv.Value("new_value2")}, // Update
		{kv.Key("key3"), kv.Value("new_value3")}, // Update
		{kv.Key("key4"), kv.Value("value4")},     // Does Not Exist
	}

	err = db.UpdateKV(context.Background(), bucket, newData...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)

	allPairs := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Exists
		{kv.Key("key3"), kv.Value("value3")}, // Exists
	}

	newList, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, allPairs, newList)
}

func TestKVBolt_UpdateKV_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var newData []kv.Pair
	for i := 0; i < max; i++ {
		newData = append(newData, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("new_value%d", i))})
	}

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.UpdateKV(ctx, bucket, newData...)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	// Bolt automatically sorts bytes so we need to match the output
	sort.SliceStable(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) == -1
	})

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_UpdateKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.UpdateKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_UpdateKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.UpdateKVFn(context.Background(), nil, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)

	err = db.UpdateKVFn(context.Background(), kv.Bucket{}, func(ctx kv.PutContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_UpdateKVFn_FailsToUpdateKeysWhenTheyDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Does Not Exist
		{kv.Key("key2"), kv.Value("value2")}, // Does Not Exist
		{kv.Key("key3"), kv.Value("value3")}, // Does Not Exist
	}

	err := db.UpdateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range data {
			if err := ctx.Put(n.Key, n.Value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, list)
}

func TestKVBolt_UpdateKVFn_UpdatesAllKeysWhenTheyAllExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("new_value1")}, // Exists
		{kv.Key("key2"), kv.Value("new_value2")}, // Exists
		{kv.Key("key3"), kv.Value("new_value3")}, // Exists
	}

	err = db.UpdateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			_ = ctx.Put(n.Key, n.Value)
		}
		return nil
	})
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, newData, newList)
}

func TestKVBolt_UpdateKVFn_FailsWhenEvenOneKeyDoesNotExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	newData := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")},     // Does Not Exist
		{kv.Key("key2"), kv.Value("new_value2")}, // Update
		{kv.Key("key3"), kv.Value("new_value3")}, // Update
		{kv.Key("key4"), kv.Value("value4")},     // Does Not Exist
	}

	err = db.UpdateKVFn(context.Background(), bucket, func(ctx kv.PutContext) error {
		for _, n := range newData {
			if err := ctx.Put(n.Key, n.Value); err != nil {
				return err
			}
		}
		return nil
	})
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)

	allPairs := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Exists
		{kv.Key("key3"), kv.Value("value3")}, // Exists
	}

	newList, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, allPairs, newList)
}

func TestKVBolt_UpdateKVFn_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var newData []kv.Pair
	for i := 0; i < max; i++ {
		newData = append(newData, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("new_value%d", i))})
	}

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.UpdateKVFn(ctx, bucket, func(ctx kv.PutContext) error {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if err := ctx.Put(newData[i].Key, newData[i].Value); err != nil {
						return err
					}
					i++
					if i >= len(newData) {
						return nil
					}
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	// Bolt automatically sorts bytes so we need to match the output
	sort.SliceStable(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) == -1
	})

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_DeleteKV_DoesNotErrorWithoutKV(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.DeleteKV(context.Background(), kv.Bucket("default"))
	assert.Nil(t, err)

	err = db.DeleteKV(context.Background(), kv.Bucket("default"), kv.Key{})
	assert.Nil(t, err)
}

func TestKVBolt_DeleteKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.DeleteKV(context.Background(), nil)
	assert.Nil(t, err)

	err = db.DeleteKV(context.Background(), kv.Bucket{})
	assert.Nil(t, err)

	err = db.DeleteKV(context.Background(), kv.Bucket{}, kv.Key{})
	assert.Nil(t, err)

	err = db.DeleteKV(context.Background(), nil, kv.Key{})
	assert.Nil(t, err)
}

func TestKVBolt_DeleteKV_DoesNotErrorWhenKeysDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Does Not Exist
		{kv.Key("key2"), kv.Value("value2")}, // Does Not Exist
		{kv.Key("key3"), kv.Value("value3")}, // Does Not Exist
	}

	err := db.DeleteKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, list)
}

func TestKVBolt_DeleteKV_DeletesAllKeysWhenTheyAllExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	deleteData := [][]byte{
		kv.Key("key1"), // Exists
		kv.Key("key2"), // Exists
		kv.Key("key3"), // Exists
	}

	err = db.DeleteKV(context.Background(), bucket, deleteData...)
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, newList)
}

func TestKVBolt_DeleteKV_DoesNotFailWhenEvenOneKeyDoesNotExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	deleteData := [][]byte{
		kv.Key("key1"), // Does Not Exist
		kv.Key("key2"), // Delete
		kv.Key("key3"), // Delete
		kv.Key("key4"), // Does Not Exist
	}

	err = db.DeleteKV(context.Background(), bucket, deleteData...)
	assert.Nil(t, err)

	newList, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, newList)
}

func TestKVBolt_DeleteKV_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.DeleteKV(ctx, bucket, extractKeys(data)...)
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	// Bolt automatically sorts bytes so we need to match the output
	sort.SliceStable(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) == -1
	})

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_DeleteKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.DeleteKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_DeleteKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.DeleteKVFn(context.Background(), nil, func(ctx kv.DeleteContext) error { return nil })
	assert.Nil(t, err)

	err = db.DeleteKVFn(context.Background(), kv.Bucket{}, func(ctx kv.DeleteContext) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_DeleteKVFn_DoesNotErrorWhenKeysDontExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Does Not Exist
		{kv.Key("key2"), kv.Value("value2")}, // Does Not Exist
		{kv.Key("key3"), kv.Value("value3")}, // Does Not Exist
	}

	err := db.DeleteKVFn(context.Background(), bucket, func(ctx kv.DeleteContext) error {
		for _, n := range data {
			_ = ctx.Delete(n.Key)
		}
		return nil
	})
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, list)
}

func TestKVBolt_DeleteKVFn_DeletesAllKeysWhenTheyAllExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key1"), kv.Value("value1")}, // Create
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	deleteData := [][]byte{
		kv.Key("key1"), // Exists
		kv.Key("key2"), // Exists
		kv.Key("key3"), // Exists
	}

	err = db.DeleteKVFn(context.Background(), bucket, func(ctx kv.DeleteContext) error {
		for _, key := range deleteData {
			_ = ctx.Delete(key)
		}
		return nil
	})
	assert.Nil(t, err)

	newList, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	assert.Nil(t, newList)
}

func TestKVBolt_DeleteKVFn_DoesNotFailWhenEvenOneKeyDoesNotExist(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	data := []kv.Pair{
		{kv.Key("key2"), kv.Value("value2")}, // Create
		{kv.Key("key3"), kv.Value("value3")}, // Create
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	list, err := db.ReadKV(context.Background(), bucket, extractKeys(data)...)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)

	deleteData := [][]byte{
		kv.Key("key1"), // Does Not Exist
		kv.Key("key2"), // Delete
		kv.Key("key3"), // Delete
		kv.Key("key4"), // Does Not Exist
	}

	err = db.DeleteKVFn(context.Background(), bucket, func(ctx kv.DeleteContext) error {
		for _, key := range deleteData {
			_ = ctx.Delete(key)
		}
		return nil
	})
	assert.Nil(t, err)

	newList, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Nil(t, newList)
}

func TestKVBolt_DeleteKVFn_StopsExecutionWhenContextIsCanceledAndDoesntCommitChanges(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		err := db.DeleteKVFn(ctx, bucket, func(ctx kv.DeleteContext) error {
			i := 0
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					err := ctx.Delete(data[i].Key)
					if err != nil {
						return err
					}
					i++
					if i >= len(data) {
						return nil
					}
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()

	// Bolt automatically sorts bytes so we need to match the output
	sort.SliceStable(data, func(i, j int) bool {
		return bytes.Compare(data[i].Key, data[j].Key) == -1
	})

	list, err := db.ListKV(context.Background(), bucket)
	assert.Nil(t, err)
	assert.Exactly(t, data, list)
}

func TestKVBolt_ListKV_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	list, err := db.ListKV(context.Background(), nil)
	assert.Nil(t, err)
	assert.Nil(t, list)

	list, err = db.ListKV(context.Background(), kv.Bucket{})
	assert.Nil(t, err)
	assert.Nil(t, list)
}

func TestKVBolt_ListKV_StopsExecutionWhenContextIsCanceledAndDoesNotReturnList(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		list, err := db.ListKV(ctx, bucket)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, list)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()
}

func TestKVBolt_ListKVFn_DoesNotErrorWithoutBucket(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.ListKVFn(context.Background(), nil, nil)
	assert.Nil(t, err)

	err = db.ListKVFn(context.Background(), kv.Bucket{}, nil)
	assert.Nil(t, err)

	err = db.ListKVFn(context.Background(), kv.Bucket{}, func(k []byte, v []byte) error { return nil })
	assert.Nil(t, err)

	err = db.ListKVFn(context.Background(), nil, func(k []byte, v []byte) error { return nil })
	assert.Nil(t, err)
}

func TestKVBolt_ListKVFn_DoesNotErrorWithoutFn(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	err := db.ListKVFn(context.Background(), kv.Bucket("default"), nil)
	assert.Nil(t, err)
}

func TestKVBolt_ListKVFn_StopsExecutionWhenContextIsCanceledAndDoesNotReturnList(t *testing.T) {
	db, closeFn := BoltDB()
	defer closeFn()

	bucket := kv.Bucket("default")
	max := 10_000
	var data []kv.Pair
	for i := 0; i < max; i++ {
		data = append(data, kv.Pair{Key: kv.Key(fmt.Sprintf("key%d", i)), Value: kv.Value(fmt.Sprintf("value%d", i))})
	}

	err := db.CreateKV(context.Background(), bucket, data...)
	assert.Nil(t, err)

	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	workerFn := func() {
		var list []kv.Pair
		err := db.ListKVFn(ctx, bucket, func(k []byte, v []byte) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					list = append(list, kv.Pair{Key: k, Value: v})
				}
			}
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, list)
		wg.Done()
	}

	wg.Add(1)
	go workerFn()
	<-time.After(1 * time.Millisecond)
	cancelFunc()
	wg.Wait()
}

//func TestKVBolt_ListBetweenKV_DoesNotErrorWithoutMinMax(t *testing.T) {
//	db, closeFn := BoltDB()
//	defer closeFn()
//
//	list, err := db.ListBetweenKV(context.Background(), kv.Bucket("default"), nil, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket("default"), nil, kv.Key{})
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket("default"), kv.Key{}, kv.Key{})
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket("default"), kv.Key{}, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//}
//
//func TestKVBolt_ListBetweenKV_DoesNotErrorWithoutBucket(t *testing.T) {
//	db, closeFn := BoltDB()
//	defer closeFn()
//
//	list, err := db.ListBetweenKV(context.Background(), nil, nil, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), nil, kv.Key{}, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), nil, kv.Key{}, kv.Key{})
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket{}, nil, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket{}, kv.Key{}, nil)
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//
//	list, err = db.ListBetweenKV(context.Background(), kv.Bucket{}, kv.Key{}, kv.Key{})
//	assert.Nil(t, err)
//	assert.Nil(t, list)
//}

func BoltDB() (*DB, func()) {

	db, err := Open("temp.db", 0600, bbolt.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}

	closer := func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
		err = os.Remove("temp.db")
		if err != nil {
			panic(err)
		}
	}

	return db, closer
}

func extractKeys(v []kv.Pair) (keys [][]byte) {
	for _, n := range v {
		keys = append(keys, n.Key)
	}
	return
}
