package bolt

import (
	"context"
	"github.com/SirMetathyst/go-kv"
	"go.etcd.io/bbolt"
	"os"
	"sync"
	"unsafe"
)

var _ kv.Store = new(DB)

type DB struct {
	*bbolt.DB
	bucketMap map[string]struct{}
	lock      sync.Mutex
}

func (s *DB) StoreKV(ctx context.Context, b []byte, v ...kv.Pair) error {

	if len(b) == 0 || len(v) == 0 {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFor(ctx, tx, b, v, store)
	})
}

func (s *DB) StoreKVFn(ctx context.Context, b []byte, fn func(ctx kv.PutContext) error) error {

	if len(b) == 0 || fn == nil {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFn(ctx, tx, b, fn, store)
	})
}

func (s *DB) CreateKV(ctx context.Context, b []byte, v ...kv.Pair) error {

	if len(b) == 0 || len(v) == 0 {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFor(ctx, tx, b, v, create)
	})
}

func (s *DB) CreateKVFn(ctx context.Context, b []byte, fn func(ctx kv.PutContext) error) error {

	if len(b) == 0 || fn == nil {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFn(ctx, tx, b, fn, create)
	})
}

func (s *DB) ReadKV(ctx context.Context, b []byte, v ...[]byte) (list []kv.Pair, err error) {

	if v == nil {
		return nil, nil
	}

	if err := s.hasBucket(b); err != nil {
		return nil, err
	}

	return list, s.View(func(tx *bbolt.Tx) error {

		bkt := tx.Bucket(b)
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}

		getContext := acquireGetContext(ctx, bkt)
		defer releaseGetContext(getContext)

		return getContext.GetFor(v, &list)
	})
}

func (s *DB) ReadKVFn(ctx context.Context, b []byte, fn func(ctx kv.GetContext) error) error {

	if len(b) == 0 || fn == nil {
		return nil
	}

	if err := s.hasBucket(b); err != nil {
		return err
	}

	return s.View(func(tx *bbolt.Tx) error {

		bkt := tx.Bucket(b)
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}

		getContext := acquireGetContext(ctx, bkt)
		defer releaseGetContext(getContext)

		return fn(getContext)
	})
}

func (s *DB) UpdateKV(ctx context.Context, b []byte, v ...kv.Pair) error {

	if len(b) == 0 || len(v) == 0 {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFor(ctx, tx, b, v, update)
	})
}

func (s *DB) UpdateKVFn(ctx context.Context, b []byte, fn func(ctx kv.PutContext) error) error {

	if len(b) == 0 || fn == nil {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {
		return s.putFn(ctx, tx, b, fn, update)
	})
}

func (s *DB) DeleteKV(ctx context.Context, b []byte, v ...[]byte) error {

	if len(b) == 0 || len(v) == 0 {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {

		bkt, err := s.createBucketIfNotExists(tx, b)
		if err != nil {
			return err
		}

		deleteContext := acquireDeleteContext(ctx, bkt)
		defer releaseDeleteContext(deleteContext)

		return deleteContext.DeleteFor(v)
	})
}

func (s *DB) DeleteKVFn(ctx context.Context, b []byte, fn func(ctx kv.DeleteContext) error) error {

	if len(b) == 0 || fn == nil {
		return nil
	}

	return s.Update(func(tx *bbolt.Tx) error {

		bkt, err := s.createBucketIfNotExists(tx, b)
		if err != nil {
			return err
		}

		deleteContext := acquireDeleteContext(ctx, bkt)
		defer releaseDeleteContext(deleteContext)

		return fn(deleteContext)
	})
}

func (s *DB) ListKV(ctx context.Context, b []byte) (list []kv.Pair, err error) {

	if len(b) == 0 {
		return nil, nil
	}

	if err := s.hasBucket(b); err != nil {
		return nil, err
	}

	return list, s.View(func(tx *bbolt.Tx) error {

		bkt := tx.Bucket(b)
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}

		c := bkt.Cursor()
		k, v := c.First()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if k == nil {
					return nil
				}
				list = append(list, kv.Pair{Key: k, Value: v})
				k, v = c.Next()
			}
		}
	})
}

func (s *DB) ListKVFn(ctx context.Context, b []byte, fn func(k []byte, v []byte) error) error {

	if len(b) == 0 {
		return nil
	}

	if err := s.hasBucket(b); err != nil {
		return err
	}

	return s.View(func(tx *bbolt.Tx) error {

		bkt := tx.Bucket(b)
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}

		c := bkt.Cursor()
		k, v := c.First()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if k == nil {
					return nil
				}
				if err := fn(k, v); err != nil {
					return err
				}
				k, v = c.Next()
			}
		}
	})
}

//func (s *DB) ListBetweenKV(ctx context.Context, b []byte, min []byte, max []byte) (list []kv.Pair, err error) {
//
//	if len(b) == 0 {
//		return nil, nil
//	}
//
//	if err := s.hasBucket(b); err != nil {
//		return nil, err
//	}
//
//	return list, s.View(func(tx *bbolt.Tx) error {
//
//		bkt := tx.Bucket(b)
//		if bkt == nil {
//			return bbolt.ErrBucketNotFound
//		}
//
//		c := bkt.Cursor()
//		k, v := c.Seek(min)
//
//		for {
//			select {
//			case <-ctx.Done():
//				return ctx.Err()
//			default:
//				if k == nil || bytes.Compare(k, max) > 0 {
//					return nil
//				}
//				list = append(list, kv.Pair{Key: k, Value: v})
//				k, v = c.Next()
//			}
//		}
//	})
//}
//
//func (s *DB) ListBetweenKVFn(ctx context.Context, b []byte, min []byte, max []byte, fn func(k []byte, v []byte) error) error {
//
//	if len(b) == 0 {
//		return nil
//	}
//
//	if err := s.hasBucket(b); err != nil {
//		return err
//	}
//
//	return s.View(func(tx *bbolt.Tx) error {
//
//		bkt := tx.Bucket(b)
//		if bkt == nil {
//			return bbolt.ErrBucketNotFound
//		}
//
//		c := bkt.Cursor()
//		k, v := c.Seek(min)
//
//		for {
//			select {
//			case <-ctx.Done():
//				return ctx.Err()
//			default:
//				if k == nil || bytes.Compare(k, max) > 0 {
//					return nil
//				}
//				if err := fn(k, v); err != nil {
//					return err
//				}
//				k, v = c.Next()
//			}
//		}
//	})
//}

func (s *DB) putFor(ctx context.Context, tx *bbolt.Tx, b []byte, v []kv.Pair, mode putMode) error {

	bkt, err := s.createBucketIfNotExists(tx, b)
	if err != nil {
		return err
	}

	putContext := acquirePutContext(ctx, mode, bkt)
	defer releasePutContext(putContext)

	return putContext.PutFor(v)
}

func (s *DB) putFn(ctx context.Context, tx *bbolt.Tx, b []byte, fn func(ctx kv.PutContext) error, mode putMode) error {

	bkt, err := s.createBucketIfNotExists(tx, b)
	if err != nil {
		return err
	}

	putContext := acquirePutContext(ctx, mode, bkt)
	defer releasePutContext(putContext)

	return fn(putContext)
}

func (s *DB) hasBucket(b []byte) error {

	if _, ok := s.bucketMap[byteToString(b)]; !ok {

		s.lock.Lock()
		err := s.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(b)
			if err == nil {
				s.bucketMap[string(b)] = struct{}{}
			}
			return err
		})
		s.lock.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *DB) createBucketIfNotExists(tx *bbolt.Tx, b []byte) (*bbolt.Bucket, error) {

	bkt, err := tx.CreateBucketIfNotExists(b)
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

type putMode int

const (
	store putMode = iota
	create
	update
)

type putContext struct {
	context.Context
	bkt  *bbolt.Bucket
	mode putMode
}

func (s *putContext) Put(k []byte, v []byte) error {

	if k == nil {
		return nil
	}

	switch s.mode {
	case create:
		if skey := s.bkt.Get(k); skey == nil {
			return s.bkt.Put(k, v)
		}
		return kv.ErrKeyFound
	case update:
		if skey := s.bkt.Get(k); skey != nil {
			return s.bkt.Put(k, v)
		}
		return kv.ErrKeyNotFound
	}

	return s.bkt.Put(k, v)
}

func (s *putContext) PutFor(v []kv.Pair) error {

	if len(v) == 0 {
		return nil
	}

	i := 0
	for {
		select {
		case <-s.Context.Done():
			return s.Context.Err()
		default:
			if err := s.Put(v[i].Key, v[i].Value); err != nil {
				return err
			}
			i++
			if i >= len(v) {
				return nil
			}
		}
	}
}

var putContextPool sync.Pool

func acquirePutContext(ctx context.Context, mode putMode, bkt *bbolt.Bucket) *putContext {
	v := putContextPool.Get()
	if v == nil {
		pc := &putContext{}
		v = pc
	}
	pc := v.(*putContext)
	pc.Context = ctx
	pc.bkt = bkt
	pc.mode = mode
	return pc
}

func releasePutContext(pc *putContext) {
	pc.Context = nil
	pc.bkt = nil
	pc.mode = store
	putContextPool.Put(pc)
}

type getContext struct {
	context.Context
	bkt *bbolt.Bucket
}

func (s *getContext) Get(k []byte, doCopy bool) ([]byte, error) {

	vv := s.bkt.Get(k)
	if vv == nil {
		return nil, kv.ErrKeyNotFound
	}

	if doCopy {
		vvCopy := make([]byte, len(vv))
		copy(vvCopy, vv)
		return vvCopy, nil
	}

	return vv, nil
}

func (s *getContext) GetFor(v [][]byte, list *[]kv.Pair) error {

	if len(v) == 0 {
		return nil
	}

	i := 0
	for {
		select {
		case <-s.Context.Done():
			return s.Context.Err()
		default:
			vv, err := s.Get(v[i], true)
			if err != nil {
				return err
			}
			*list = append(*list, kv.Pair{Key: v[i], Value: vv})
			i++
			if i >= len(v) {
				return nil
			}
		}
	}
}

var getContextPool sync.Pool

func acquireGetContext(ctx context.Context, bkt *bbolt.Bucket) *getContext {
	v := getContextPool.Get()
	if v == nil {
		gc := &getContext{}
		v = gc
	}
	gc := v.(*getContext)
	gc.Context = ctx
	gc.bkt = bkt
	return gc
}

func releaseGetContext(gc *getContext) {
	gc.Context = nil
	gc.bkt = nil
	getContextPool.Put(gc)
}

type deleteContext struct {
	context.Context
	bkt *bbolt.Bucket
}

func (s *deleteContext) Delete(k []byte) error {
	return s.bkt.Delete(k)
}

func (s *deleteContext) DeleteFor(v [][]byte) error {

	if len(v) == 0 {
		return nil
	}

	i := 0
	for {
		select {
		case <-s.Context.Done():
			return s.Context.Err()
		default:
			err := s.Delete(v[i])
			if err != nil {
				return err
			}
			i++
			if i >= len(v) {
				return nil
			}
		}
	}
}

var deleteContextPool sync.Pool

func acquireDeleteContext(ctx context.Context, bkt *bbolt.Bucket) *deleteContext {
	v := deleteContextPool.Get()
	if v == nil {
		dc := &deleteContext{}
		v = dc
	}
	dc := v.(*deleteContext)
	dc.Context = ctx
	dc.bkt = bkt
	return dc
}

func releaseDeleteContext(dc *deleteContext) {
	dc.Context = nil
	dc.bkt = nil
	deleteContextPool.Put(dc)
}

func byteToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func New(db *bbolt.DB) (*DB, error) {
	return &DB{DB: db, bucketMap: map[string]struct{}{}}, nil
}

func Open(path string, mode os.FileMode, options *bbolt.Options) (*DB, error) {

	db, err := bbolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}

	return New(db)
}
