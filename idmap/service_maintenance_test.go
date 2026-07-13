package idmap

import (
	"fmt"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
)

func TestClearBucketFastRecreatesBucketAndPreservesOtherBuckets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idmap.db")
	testDB, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db = testDB
	t.Cleanup(func() {
		_ = CloseDBWithError()
	})

	const entryCount = 250
	if err := db.Update(func(tx *bbolt.Tx) error {
		cache, err := tx.CreateBucket([]byte(CacheBucketName))
		if err != nil {
			return err
		}
		other, err := tx.CreateBucket([]byte(BucketName))
		if err != nil {
			return err
		}
		if err := other.Put([]byte("keep"), []byte("value")); err != nil {
			return err
		}
		for i := 0; i < entryCount; i++ {
			if err := cache.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte("value")); err != nil {
				return err
			}
		}
		nested, err := cache.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := nested.Put([]byte("nested-key"), []byte("nested-value")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := ClearBucketFast(CacheBucketName); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bbolt.Tx) error {
		cache := tx.Bucket([]byte(CacheBucketName))
		if cache == nil {
			t.Fatal("cache bucket was not recreated")
		}
		if got := cache.Stats().KeyN; got != 0 {
			t.Errorf("cache still has %d entries", got)
		}
		if got := tx.Bucket([]byte(BucketName)).Get([]byte("keep")); string(got) != "value" {
			t.Errorf("unrelated bucket was modified: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestClearBucketFastMissingBucketIsSuccessfulNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idmap.db")
	testDB, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db = testDB
	t.Cleanup(func() {
		_ = CloseDBWithError()
	})

	if err := ClearBucketFast(CacheBucketName); err != nil {
		t.Fatal(err)
	}
}
