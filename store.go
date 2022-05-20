package kv

import (
	"context"
	"errors"
)

var (
	ErrKeyFound    = errors.New("key: key found")
	ErrKeyNotFound = errors.New("key: key not found")
)

type Store interface {
	Storer
	Creater
	Reader
	Updater
	Deleter
	Lister
	//ListPrefixer
	//ListBetweener
}

type Storer interface {
	StoreKV(ctx context.Context, b []byte, v ...Pair) error
	StoreKVFn(ctx context.Context, b []byte, fn func(ctx PutContext) error) error
}

type Creater interface {
	CreateKV(ctx context.Context, b []byte, v ...Pair) error
	CreateKVFn(ctx context.Context, b []byte, fn func(ctx PutContext) error) error
}

type Reader interface {
	ReadKV(ctx context.Context, b  []byte, v ...[]byte) ([]Pair, error)
	ReadKVFn(ctx context.Context, b  []byte, fn func(ctx GetContext) error) error
}

type Updater interface {
	UpdateKV(ctx context.Context, b []byte, v ...Pair) error
	UpdateKVFn(ctx context.Context, b []byte, fn func(ctx PutContext) error) error
}

type Deleter interface {
	DeleteKV(ctx context.Context, b []byte, v ...[]byte) error
	DeleteKVFn(ctx context.Context, b []byte, fn func(ctx DeleteContext) error) error
}

type Lister interface {
	ListKV(ctx context.Context, b []byte) ([]Pair, error)
	ListKVFn(ctx context.Context, b []byte, fn func(k []byte, v []byte) error) error
}

//type ListPrefixer interface {
//	ListPrefixKV(ctx context.Context, b Bucket, v []byte) ([]Pair, error)
//	ListPrefixKVFn(ctx context.Context, b Bucket, v []byte, fn func(k []byte, v []byte) error) error
//}

//type ListBetweener interface {
//	ListBetweenKV(ctx context.Context, b Bucket, min []byte, max []byte) ([]Pair, error)
//	ListBetweenKVFn(ctx context.Context, b Bucket, min []byte, max []byte, fn func(k []byte, v []byte) error) error
//}

type Bucket []byte

type PutContext interface {
	context.Context
	Put(k []byte, v []byte) error
}

type GetContext interface {
	context.Context
	Get(k []byte, copy bool) ([]byte, error)
}

type DeleteContext interface {
	context.Context
	Delete(k []byte) error
}

type Pair struct {
	Key   []byte
	Value []byte
}

type Key []byte

type Value []byte
