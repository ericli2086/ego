package utils

import (
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type KVStore struct {
	db *badger.DB
}

func Open(path string) (*KVStore, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(path).
		WithLogger(nil) // 关闭默认日志

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &KVStore{db: db}, nil
}

func (kv *KVStore) Close() error {
	return kv.db.Close()
}

// Set 带 TTL 的写入。如果 ttl <= 0，则永久保存。
func (kv *KVStore) Set(key, value []byte, ttl time.Duration) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value)
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// Get 读取 key 的值。如果 key 不存在或已过期，则返回 badger.ErrKeyNotFound。
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

// Delete 删除 key
func (kv *KVStore) Delete(key []byte) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Has 检查 key 是否存在（注意：如果 key 过期也会视为不存在）
func (kv *KVStore) Has(key []byte) (bool, error) {
	err := kv.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return false, nil
	}

	return err == nil, err
}
