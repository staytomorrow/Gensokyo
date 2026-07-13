package idmap

import (
	"fmt"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
)

func TestClearBucketDeletesEveryEntryAndReportsProgress(t *testing.T) {
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
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	lastCurrent := -1
	lastTotal := -1
	deleted, err := ClearBucket(CacheBucketName, func(current, total int) {
		if current < lastCurrent {
			t.Errorf("progress moved backwards: %d -> %d", lastCurrent, current)
		}
		lastCurrent = current
		lastTotal = total
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != entryCount {
		t.Fatalf("deleted %d entries, want %d", deleted, entryCount)
	}
	if lastCurrent != entryCount || lastTotal != entryCount {
		t.Fatalf("final progress = %d/%d, want %d/%d", lastCurrent, lastTotal, entryCount, entryCount)
	}

	if err := db.View(func(tx *bbolt.Tx) error {
		if got := tx.Bucket([]byte(CacheBucketName)).Stats().KeyN; got != 0 {
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

func TestClearBucketMissingBucketIsSuccessfulNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idmap.db")
	testDB, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db = testDB
	t.Cleanup(func() {
		_ = CloseDBWithError()
	})

	called := false
	deleted, err := ClearBucket(CacheBucketName, func(current, total int) {
		called = true
		if current != 0 || total != 0 {
			t.Errorf("progress = %d/%d, want 0/0", current, total)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 || !called {
		t.Fatalf("deleted=%d, callback called=%v; want 0, true", deleted, called)
	}
}
