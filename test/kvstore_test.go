package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"

	"gogo/utils"
)

func TestKVStore_SetGetDelete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "badger_test")
	defer os.RemoveAll(path)

	kv, err := utils.Open(path)
	assert.NoError(t, err)
	defer kv.Close()

	key := []byte("foo")
	value := []byte("bar")

	// Set
	err = kv.Set(key, value, 0)
	assert.NoError(t, err)

	// Get
	val, err := kv.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	// Has
	exists, err := kv.Has(key)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Delete
	err = kv.Delete(key)
	assert.NoError(t, err)

	// Has after delete
	exists, err = kv.Has(key)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestKVStore_TTL(t *testing.T) {
	path := filepath.Join(os.TempDir(), "badger_test_ttl")
	defer os.RemoveAll(path)

	kv, err := utils.Open(path)
	assert.NoError(t, err)
	defer kv.Close()

	key := []byte("temp")
	value := []byte("123")

	// Set with TTL
	err = kv.Set(key, value, 2*time.Second)
	assert.NoError(t, err)

	// Get before expiry
	val, err := kv.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Get after expiry
	val, err = kv.Get(key)
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}
