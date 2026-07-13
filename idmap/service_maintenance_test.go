package idmap

import (
	"bytes"
	"fmt"
	"os"
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

func TestRebuildDatabaseWithoutBucketCopiesRetainedDataAndReplacesSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idmap.db")
	testDB, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db = testDB
	t.Cleanup(func() {
		_ = CloseDBWithError()
	})

	if err := db.Update(func(tx *bbolt.Tx) error {
		cache, err := tx.CreateBucket([]byte(CacheBucketName))
		if err != nil {
			return err
		}
		largeValue := bytes.Repeat([]byte("x"), 4096)
		for i := 0; i < 512; i++ {
			if err := cache.Put([]byte(fmt.Sprintf("cache-%04d", i)), largeValue); err != nil {
				return err
			}
		}

		ids, err := tx.CreateBucket([]byte(BucketName))
		if err != nil {
			return err
		}
		if err := ids.SetSequence(42); err != nil {
			return err
		}
		if err := ids.Put([]byte("keep-id"), []byte("keep-value")); err != nil {
			return err
		}

		configBucket, err := tx.CreateBucket([]byte(ConfigBucket))
		if err != nil {
			return err
		}
		nested, err := configBucket.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		return nested.Put([]byte("nested-key"), []byte("nested-value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	seenPhases := make(map[RebuildPhase]bool)
	result, err := rebuildDatabaseWithoutBucket(path, CacheBucketName, func(state RebuildProgress) {
		seenPhases[state.Phase] = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.BackupPath != "" {
		t.Fatalf("unexpected retained backup: %s", result.BackupPath)
	}
	for _, phase := range []RebuildPhase{RebuildPreparing, RebuildCopying, RebuildSyncing, RebuildReplacing, RebuildValidating} {
		if !seenPhases[phase] {
			t.Errorf("progress phase %d was not reported", phase)
		}
	}

	if err := db.View(func(tx *bbolt.Tx) error {
		cache := tx.Bucket([]byte(CacheBucketName))
		if cache == nil {
			return fmt.Errorf("cache bucket is missing")
		}
		if key, _ := cache.Cursor().First(); key != nil {
			return fmt.Errorf("cache bucket still contains %q", key)
		}
		ids := tx.Bucket([]byte(BucketName))
		if ids == nil || string(ids.Get([]byte("keep-id"))) != "keep-value" {
			return fmt.Errorf("retained ids data is missing")
		}
		if ids.Sequence() != 42 {
			return fmt.Errorf("ids sequence = %d, want 42", ids.Sequence())
		}
		configBucket := tx.Bucket([]byte(ConfigBucket))
		if configBucket == nil || configBucket.Bucket([]byte("nested")) == nil {
			return fmt.Errorf("nested retained bucket is missing")
		}
		if got := configBucket.Bucket([]byte("nested")).Get([]byte("nested-key")); string(got) != "nested-value" {
			return fmt.Errorf("nested retained value = %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	rebuiltInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if rebuiltInfo.Size() >= originalInfo.Size() {
		t.Fatalf("rebuilt database size %d is not smaller than original %d", rebuiltInfo.Size(), originalInfo.Size())
	}
}
