package idmap

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

// RebuildPhase identifies the current database rebuild stage.
type RebuildPhase int

const (
	RebuildPreparing RebuildPhase = iota
	RebuildCopying
	RebuildSyncing
	RebuildReplacing
	RebuildValidating
)

// RebuildProgress describes completed copy work. Entries counts key/value
// pairs; bucket metadata is copied separately.
type RebuildProgress struct {
	Phase   RebuildPhase
	Entries uint64
	Bytes   uint64
}

// RebuildResult describes the compact database produced by a rebuild.
type RebuildResult struct {
	Entries    uint64
	Bytes      uint64
	Buckets    uint64
	BackupPath string
}

type rebuildKV struct {
	key   []byte
	value []byte
}

const rebuildBatchSize = 4096

// RebuildDatabaseWithoutBucket creates a compact replacement for idmap.db by
// copying every top-level bucket except excludedBucket. An empty replacement
// bucket is created so the database remains immediately usable.
func RebuildDatabaseWithoutBucket(excludedBucket string, progress func(RebuildProgress)) (RebuildResult, error) {
	return rebuildDatabaseWithoutBucket(DBName, excludedBucket, progress)
}

func rebuildDatabaseWithoutBucket(sourcePath, excludedBucket string, progress func(RebuildProgress)) (RebuildResult, error) {
	var result RebuildResult
	if db == nil {
		return result, fmt.Errorf("idmap database is not open")
	}
	if excludedBucket == "" {
		return result, fmt.Errorf("excluded bucket name is empty")
	}

	absSourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		return result, fmt.Errorf("resolve source database path: %w", err)
	}
	if progress != nil {
		progress(RebuildProgress{Phase: RebuildPreparing})
	}

	tempFile, err := os.CreateTemp(filepath.Dir(absSourcePath), ".idmap-rebuild-*.db")
	if err != nil {
		return result, fmt.Errorf("create temporary database: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return result, fmt.Errorf("close temporary database file: %w", err)
	}
	defer func() {
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
	}()

	targetDB, err := bbolt.Open(tempPath, 0600, &bbolt.Options{
		Timeout:      3 * time.Second,
		FreelistType: bbolt.FreelistMapType,
	})
	if err != nil {
		return result, fmt.Errorf("open temporary database: %w", err)
	}
	targetOpen := true
	defer func() {
		if targetOpen {
			_ = targetDB.Close()
		}
	}()

	var retainedBuckets [][]byte
	err = db.View(func(sourceTx *bbolt.Tx) error {
		return sourceTx.ForEach(func(name []byte, sourceBucket *bbolt.Bucket) error {
			if bytes.Equal(name, []byte(excludedBucket)) {
				return nil
			}
			nameCopy := append([]byte(nil), name...)
			retainedBuckets = append(retainedBuckets, nameCopy)
			result.Buckets++
			return copyBucketForRebuild(targetDB, sourceBucket, [][]byte{nameCopy}, &result, progress)
		})
	})
	if err != nil {
		return result, fmt.Errorf("copy retained database data: %w", err)
	}

	// Preserve the schema while intentionally leaving the excluded bucket empty.
	err = targetDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(excludedBucket))
		return err
	})
	if err != nil {
		return result, fmt.Errorf("create empty %s bucket: %w", excludedBucket, err)
	}

	if progress != nil {
		progress(RebuildProgress{Phase: RebuildSyncing, Entries: result.Entries, Bytes: result.Bytes})
	}
	if err := targetDB.Sync(); err != nil {
		return result, fmt.Errorf("sync temporary database: %w", err)
	}
	if err := targetDB.Close(); err != nil {
		return result, fmt.Errorf("close temporary database: %w", err)
	}
	targetOpen = false

	if err := CloseDBWithError(); err != nil {
		return result, fmt.Errorf("close source database: %w", err)
	}
	if progress != nil {
		progress(RebuildProgress{Phase: RebuildReplacing, Entries: result.Entries, Bytes: result.Bytes})
	}

	backupPath := fmt.Sprintf("%s.rebuild-backup-%s", absSourcePath, time.Now().Format("20060102-150405.000000000"))
	if err := os.Rename(absSourcePath, backupPath); err != nil {
		_ = reopenMaintenanceDatabase(absSourcePath)
		return result, fmt.Errorf("move source database to backup: %w", err)
	}
	if err := os.Rename(tempPath, absSourcePath); err != nil {
		rollbackErr := os.Rename(backupPath, absSourcePath)
		_ = reopenMaintenanceDatabase(absSourcePath)
		if rollbackErr != nil {
			return result, fmt.Errorf("install rebuilt database: %v; restore original database: %w", err, rollbackErr)
		}
		return result, fmt.Errorf("install rebuilt database: %w", err)
	}
	tempPath = ""

	if progress != nil {
		progress(RebuildProgress{Phase: RebuildValidating, Entries: result.Entries, Bytes: result.Bytes})
	}
	if err := reopenMaintenanceDatabase(absSourcePath); err != nil {
		rollbackErr := rollbackRebuiltDatabase(absSourcePath, backupPath)
		if rollbackErr != nil {
			return result, fmt.Errorf("open rebuilt database: %v; restore original database: %w", err, rollbackErr)
		}
		return result, fmt.Errorf("open rebuilt database: %w", err)
	}

	validationErr := db.View(func(tx *bbolt.Tx) error {
		cache := tx.Bucket([]byte(excludedBucket))
		if cache == nil {
			return fmt.Errorf("empty replacement bucket %s is missing", excludedBucket)
		}
		if key, _ := cache.Cursor().First(); key != nil {
			return fmt.Errorf("replacement bucket %s is not empty", excludedBucket)
		}
		for _, name := range retainedBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("retained bucket %q is missing", name)
			}
		}
		return nil
	})
	if validationErr != nil {
		_ = CloseDBWithError()
		rollbackErr := rollbackRebuiltDatabase(absSourcePath, backupPath)
		if rollbackErr != nil {
			return result, fmt.Errorf("validate rebuilt database: %v; restore original database: %w", validationErr, rollbackErr)
		}
		return result, fmt.Errorf("validate rebuilt database: %w", validationErr)
	}

	if err := os.Remove(backupPath); err != nil {
		result.BackupPath = backupPath
	}
	return result, nil
}

func copyBucketForRebuild(targetDB *bbolt.DB, sourceBucket *bbolt.Bucket, path [][]byte, result *RebuildResult, progress func(RebuildProgress)) error {
	if err := targetDB.Update(func(tx *bbolt.Tx) error {
		targetBucket, err := createBucketPath(tx, path)
		if err != nil {
			return err
		}
		return targetBucket.SetSequence(sourceBucket.Sequence())
	}); err != nil {
		return err
	}

	batch := make([]rebuildKV, 0, rebuildBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := targetDB.Update(func(tx *bbolt.Tx) error {
			targetBucket, err := findBucketPath(tx, path)
			if err != nil {
				return err
			}
			for _, pair := range batch {
				if err := targetBucket.Put(pair.key, pair.value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, pair := range batch {
			result.Entries++
			result.Bytes += uint64(len(pair.key) + len(pair.value))
		}
		batch = batch[:0]
		if progress != nil {
			progress(RebuildProgress{Phase: RebuildCopying, Entries: result.Entries, Bytes: result.Bytes})
		}
		return nil
	}

	err := sourceBucket.ForEach(func(key, value []byte) error {
		if value == nil {
			if err := flush(); err != nil {
				return err
			}
			child := sourceBucket.Bucket(key)
			if child == nil {
				return fmt.Errorf("nested bucket %q cannot be opened", key)
			}
			childPath := appendBucketPath(path, key)
			result.Buckets++
			return copyBucketForRebuild(targetDB, child, childPath, result, progress)
		}

		batch = append(batch, rebuildKV{
			key:   append([]byte(nil), key...),
			value: append([]byte(nil), value...),
		})
		if len(batch) >= rebuildBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

func createBucketPath(tx *bbolt.Tx, path [][]byte) (*bbolt.Bucket, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("bucket path is empty")
	}
	bucket, err := tx.CreateBucketIfNotExists(path[0])
	if err != nil {
		return nil, err
	}
	for _, name := range path[1:] {
		bucket, err = bucket.CreateBucketIfNotExists(name)
		if err != nil {
			return nil, err
		}
	}
	return bucket, nil
}

func findBucketPath(tx *bbolt.Tx, path [][]byte) (*bbolt.Bucket, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("bucket path is empty")
	}
	bucket := tx.Bucket(path[0])
	for _, name := range path[1:] {
		if bucket == nil {
			break
		}
		bucket = bucket.Bucket(name)
	}
	if bucket == nil {
		return nil, fmt.Errorf("target bucket path does not exist")
	}
	return bucket, nil
}

func appendBucketPath(path [][]byte, name []byte) [][]byte {
	childPath := make([][]byte, len(path)+1)
	copy(childPath, path)
	childPath[len(path)] = append([]byte(nil), name...)
	return childPath
}

func reopenMaintenanceDatabase(path string) error {
	opened, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout:      3 * time.Second,
		FreelistType: bbolt.FreelistMapType,
	})
	if err != nil {
		return err
	}
	db = opened
	return nil
}

func rollbackRebuiltDatabase(sourcePath, backupPath string) error {
	_ = os.Remove(sourcePath)
	if err := os.Rename(backupPath, sourcePath); err != nil {
		return err
	}
	return reopenMaintenanceDatabase(sourcePath)
}
