package services

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"s3-storage/model"
)

const metaFileName = ".meta"

// MetaStore manages metadata stored in a .meta file with in-memory caching
type MetaStore struct {
	mu       sync.RWMutex
	path     string
	metadata model.MetaData
}

// NewMetaStore creates a new MetaStore and loads existing metadata
// storageDir is the directory where the .meta file will be stored
// Parameters:
// - storageDir: The directory path where the .meta file is located or will be created
// Returns:
// - *MetaStore: A pointer to the initialized MetaStore instance
// - error: An error if the metadata could not be loaded, or nil on success
// Example usage:
// metaStore, err := NewMetaStore("/path/to/storage")
//
//	if err != nil {
//	    log.Fatalf("Failed to initialize MetaStore: %v", err)
//	}
func NewMetaStore(storageDir string) (*MetaStore, error) {
	ms := &MetaStore{
		path: filepath.Join(storageDir, metaFileName),
	}
	if err := ms.Load(); err != nil {
		return nil, err
	}
	return ms, nil
}

// Load reads metadata from disk, or initializes empty if not exists
// This method should be called during initialization to load existing metadata into memory
// Parameters:
// - None
// Returns:
// - error: An error if the metadata could not be loaded, or nil on success
// Example usage:
// err := metaStore.Load()
//
//	if err != nil {
//	    log.Fatalf("Failed to load metadata: %v", err)
//	}
func (ms *MetaStore) Load() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	data, err := os.ReadFile(ms.path)
	if err != nil {
		if os.IsNotExist(err) {
			// Initialize with empty metadata and generate disk UUID
			ms.metadata = model.MetaData{
				Version:    1,
				DiskUUID:   uuid.New().String(),
				UpdatedAt:  time.Now().Unix(),
				Buckets:    []model.BucketMeta{},
				Multiparts: []model.Multipart{},
				Healing:    []model.HealingLock{},
			}
			return ms.saveUnlocked()
		}
		return err
	}

	// Decode msgpack
	_, err = ms.metadata.UnmarshalMsg(data)
	return err
}

// Save writes metadata to disk atomically
// This method should be called after any modification to the metadata to persist changes to disk
// Parameters:
// - None
// Returns:
// - error: An error if the metadata could not be saved, or nil on success
// Example usage:
// err := metaStore.Save()
//
//	if err != nil {
//	    log.Fatalf("Failed to save metadata: %v", err)
//	}
func (ms *MetaStore) Save() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.saveUnlocked()
}

// saveUnlocked writes to disk without acquiring lock (caller must hold lock)
// This internal method performs the actual disk write operation. It should only be called by methods that have already acquired the necessary locks to ensure thread safety.
// Parameters:
// - None
// Returns:
// - error: An error if the metadata could not be saved, or nil on success
// Example usage:
// err := metaStore.saveUnlocked()
//
//	if err != nil {
//	    log.Fatalf("Failed to save metadata: %v", err)
//	}
func (ms *MetaStore) saveUnlocked() error {
	ms.metadata.UpdatedAt = time.Now().Unix()
	ms.metadata.Version++

	data, err := ms.metadata.MarshalMsg(nil)
	if err != nil {
		return err
	}

	// Ensure directory exists
	dir := filepath.Dir(ms.path)
	if err = os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Atomic write: write to temp file, sync, then rename
	tmpPath := ms.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if _, err = f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err = f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err = f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, ms.path)
}

// GetBuckets returns all bucket metadata
// This method provides a thread-safe way to retrieve the list of all buckets currently stored in the metadata. It returns a copy of the bucket metadata to prevent
// Parameters:
// - None
// Returns:
// - []model.BucketMeta: A slice containing the metadata of all buckets
// Example usage:
// buckets := metaStore.GetBuckets()
//
//	for _, bucket := range buckets {
//	    fmt.Printf("Bucket Name: %s, Created At: %d\n", bucket.Name, bucket.CreationDate)
//	}
func (ms *MetaStore) GetBuckets() []model.BucketMeta {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Return a copy to avoid data races
	result := make([]model.BucketMeta, len(ms.metadata.Buckets))
	copy(result, ms.metadata.Buckets)
	return result
}

// BucketExists checks if a bucket exists
// This method provides a thread-safe way to check if a bucket with the specified name exists in the metadata. It iterates through the list of buckets and returns true if a match is found, or false otherwise.
// Parameters:
// - name: The name of the bucket to check for existence
// Returns:
// - bool: True if the bucket exists, false otherwise
// Example usage:
// exists := metaStore.BucketExists("my-bucket")
//
//	if exists {
//	    fmt.Println("Bucket exists!")
//	} else {
//
//	    fmt.Println("Bucket does not exist.")
//	}
func (ms *MetaStore) BucketExists(name string) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, b := range ms.metadata.Buckets {
		if b.Name == name {
			return true
		}
	}
	return false
}

// CreateBucket creates a new bucket
// This method provides a thread-safe way to create a new bucket with the specified name.
// It first checks if a bucket with the same name already exists and returns an error if it does.
// If the bucket name is unique, it appends the new bucket metadata to the list and saves the updated metadata to disk.
// Parameters:
// - name: The name of the bucket to be created
// Returns:
// - error: An error if the bucket already exists or if there was an issue saving the metadata, or nil on success
// Example usage:
// err := metaStore.CreateBucket("my-new-bucket")
//
//	if err != nil {
//	    fmt.Printf("Failed to create bucket: %v\n", err)
//	} else {
//
//	    fmt.Println("Bucket created successfully!")
//	}
func (ms *MetaStore) CreateBucket(name string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Check if bucket already exists
	for _, b := range ms.metadata.Buckets {
		if b.Name == name {
			return ErrBucketAlreadyOwnedByYou
		}
	}

	ms.metadata.Buckets = append(ms.metadata.Buckets, model.BucketMeta{
		Name:         name,
		CreationDate: time.Now().Unix(),
	})

	return ms.saveUnlocked()
}

// DeleteBucket deletes a bucket (does not check if empty - caller should verify)
// This method provides a thread-safe way to delete a bucket with the specified name.
// It searches for the bucket in the list and removes it if found. If the bucket does not exist, it returns an error. After deletion, it saves the updated metadata to disk.
// Parameters:
// - name: The name of the bucket to be deleted
// Returns:
// - error: An error if the bucket does not exist or if there was an issue saving the metadata, or nil on success
// Example usage:
// err := metaStore.DeleteBucket("my-old-bucket")
//
//	if err != nil {
//	    fmt.Printf("Failed to delete bucket: %v\n", err)
//	} else {
//
//	    fmt.Println("Bucket deleted successfully!")
//	}
func (ms *MetaStore) DeleteBucket(name string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	idx := -1
	for i, b := range ms.metadata.Buckets {
		if b.Name == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrNoSuchBucket
	}

	// O(1) delete using swap-and-truncate pattern
	lastIdx := len(ms.metadata.Buckets) - 1
	ms.metadata.Buckets[idx] = ms.metadata.Buckets[lastIdx]
	ms.metadata.Buckets = ms.metadata.Buckets[:lastIdx]

	return ms.saveUnlocked()
}

// GetBucket returns metadata for a specific bucket
// This method provides a thread-safe way to retrieve the metadata of a specific bucket by its name.
// It iterates through the list of buckets and returns a copy of the metadata if a match is found, or an error if the bucket does not exist.
// Parameters:
// - name: The name of the bucket for which to retrieve metadata
// Returns:
// - *model.BucketMeta: A pointer to the metadata of the specified bucket, or nil if the bucket does not exist
// - error: An error if the bucket does not exist, or nil on success
// Example usage:
// bucketMeta, err := metaStore.GetBucket("my-bucket")
//
//	if err != nil {
//	    fmt.Printf("Failed to get bucket metadata: %v\n", err)
//	} else {
//
//	    fmt.Printf("Bucket Name: %s, Created At: %d\n", bucketMeta.Name, bucketMeta.CreationDate)
//	}
func (ms *MetaStore) GetBucket(name string) (*model.BucketMeta, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, b := range ms.metadata.Buckets {
		if b.Name == name {
			// Return a copy
			result := b
			return &result, nil
		}
	}
	return nil, ErrNoSuchBucket
}

// UpdateBucketStats updates the size and object count for a bucket
// sizeDelta is the change in size (positive for add, negative for delete)
// countDelta is the change in object count (positive for add, negative for delete)
// Parameters:
// - name: The name of the bucket to update
// - sizeDelta: The change in total size for the bucket (positive for increase, negative for decrease)
// - countDelta: The change in object count for the bucket (positive for increase, negative for decrease)
// Returns:
// - error: An error if the bucket does not exist or if there was an issue saving the metadata, or nil on success
// Example usage:
// err := metaStore.UpdateBucketStats("my-bucket", 1024, 1) // Add 1KB and increment object count by 1
//
//	if err != nil {
//	    fmt.Printf("Failed to update bucket stats: %v\n", err)
//	} else {
//
//	    fmt.Println("Bucket stats updated successfully!")
//	}
func (ms *MetaStore) UpdateBucketStats(name string, sizeDelta int64, countDelta int64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for i := range ms.metadata.Buckets {
		if ms.metadata.Buckets[i].Name == name {
			ms.metadata.Buckets[i].TotalSize += sizeDelta
			ms.metadata.Buckets[i].ObjectCount += countDelta
			return ms.saveUnlocked()
		}
	}
	return ErrNoSuchBucket
}

// GetDiskUUID returns the persistent disk UUID for this storage
// This method provides a thread-safe way to retrieve the unique identifier for the disk associated with this metadata store.
// The disk UUID is generated when the metadata is first initialized and remains constant for the lifetime of the storage.
// Parameters:
// - None
// Returns:
// - string: The disk UUID associated with this metadata store
// Example usage:
// diskUUID := metaStore.GetDiskUUID()
// fmt.Printf("Disk UUID: %s\n", diskUUID)
func (ms *MetaStore) GetDiskUUID() string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.metadata.DiskUUID
}

// CreateBucketWithOwner creates a new bucket with an owner
// This method provides a thread-safe way to create a new bucket with the specified name and owner ID.
// It first checks if a bucket with the same name already exists and returns an error if it does.
// If the bucket name is unique, it appends the new bucket metadata with the owner information to the list and saves the updated metadata to disk.
// Parameters:
// - name: The name of the bucket to be created
// - ownerID: The identifier of the owner of the bucket
// Returns:
// - error: An error if the bucket already exists or if there was an issue saving the metadata, or nil on success
// Example usage:
// err := metaStore.CreateBucketWithOwner("my-new-bucket", "user-123")
//
//	if err != nil {
//	    fmt.Printf("Failed to create bucket: %v\n", err)
//	} else {
//
//	    fmt.Println("Bucket created successfully with owner!")
//	}
func (ms *MetaStore) CreateBucketWithOwner(name, ownerID string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Check if bucket already exists
	for _, b := range ms.metadata.Buckets {
		if b.Name == name {
			return ErrBucketAlreadyOwnedByYou
		}
	}

	ms.metadata.Buckets = append(ms.metadata.Buckets, model.BucketMeta{
		Name:         name,
		CreationDate: time.Now().Unix(),
		Owner:        ownerID,
	})

	return ms.saveUnlocked()
}

// GetBucketOwner returns the owner ID for a bucket
// This method provides a thread-safe way to retrieve the owner identifier for a specific bucket by its name.
// It iterates through the list of buckets and returns the owner ID if a match is found, or an error if the bucket does not exist.
// Parameters:
// - name: The name of the bucket for which to retrieve the owner ID
// Returns:
// - string: The owner ID of the specified bucket, or an empty string if the bucket does not exist
// - error: An error if the bucket does not exist, or nil on success
// Example usage:
// ownerID, err := metaStore.GetBucketOwner("my-bucket")
//
//	if err != nil {
//	    fmt.Printf("Failed to get bucket owner: %v\n", err)
//	} else {
//
//	    fmt.Printf("Bucket Owner ID: %s\n", ownerID)
//	}
func (ms *MetaStore) GetBucketOwner(name string) (string, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, b := range ms.metadata.Buckets {
		if b.Name == name {
			return b.Owner, nil
		}
	}
	return "", ErrNoSuchBucket
}

// GetTotalStorageSize returns the total size of all buckets in bytes
// This method provides a thread-safe way to calculate the total storage size used by all buckets combined.
// It iterates through the list of buckets and sums up their total sizes, returning the aggregate value.
// Parameters:
// - None
// Returns:
// - int64: The total storage size used by all buckets in bytes
// Example usage:
// totalSize := metaStore.GetTotalStorageSize()
// fmt.Printf("Total Storage Size: %d bytes\n", totalSize)
func (ms *MetaStore) GetTotalStorageSize() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	var totalSize int64
	for _, b := range ms.metadata.Buckets {
		totalSize += b.TotalSize
	}
	return totalSize
}

// AddMultipartUpload records a new multipart upload
// This method provides a thread-safe way to add a new multipart upload to the metadata.
// It appends the new upload to the list and saves the updated metadata to disk.
// Parameters:
// - upload: The multipart upload metadata to be added
// Returns:
// - error: An error if there was an issue saving the metadata, or nil on success
func (ms *MetaStore) AddMultipartUpload(upload model.Multipart) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.metadata.Multiparts = append(ms.metadata.Multiparts, upload)
	return ms.saveUnlocked()
}

// GetMultipartUpload retrieves upload metadata by ID
// This method provides a thread-safe way to retrieve a specific multipart upload by its upload ID.
// It iterates through the list of multipart uploads and returns a copy if a match is found.
// Parameters:
// - uploadID: The ID of the multipart upload to retrieve
// Returns:
// - *model.Multipart: A pointer to the multipart upload metadata, or nil if not found
// - error: An error if the upload does not exist, or nil on success
func (ms *MetaStore) GetMultipartUpload(uploadID string) (*model.Multipart, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for _, upload := range ms.metadata.Multiparts {
		if upload.UploadID == uploadID {
			// Return a copy
			result := upload
			return &result, nil
		}
	}
	return nil, ErrNoSuchUpload
}

// UpdateMultipartPart updates a part in the upload
// This method provides a thread-safe way to update a specific part within a multipart upload.
// It finds the upload by ID, updates or adds the part, and saves the metadata to disk.
// Parameters:
// - uploadID: The ID of the multipart upload
// - partNum: The part number to update
// - part: The part upload metadata
// Returns:
// - error: An error if the upload does not exist or if there was an issue saving the metadata, or nil on success
func (ms *MetaStore) UpdateMultipartPart(uploadID string, partNum int, part model.PartUpload) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	for i := range ms.metadata.Multiparts {
		if ms.metadata.Multiparts[i].UploadID == uploadID {
			// Initialize Parts map if nil
			if ms.metadata.Multiparts[i].Parts == nil {
				ms.metadata.Multiparts[i].Parts = make(map[int]model.PartUpload)
			}
			ms.metadata.Multiparts[i].Parts[partNum] = part
			return ms.saveUnlocked()
		}
	}
	return ErrNoSuchUpload
}

// RemoveMultipartUpload deletes upload tracking
// This method provides a thread-safe way to remove a multipart upload from the metadata.
// It searches for the upload by ID, removes it if found, and saves the updated metadata to disk.
// Parameters:
// - uploadID: The ID of the multipart upload to remove
// Returns:
// - error: An error if the upload does not exist or if there was an issue saving the metadata, or nil on success
func (ms *MetaStore) RemoveMultipartUpload(uploadID string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	idx := -1
	for i, upload := range ms.metadata.Multiparts {
		if upload.UploadID == uploadID {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrNoSuchUpload
	}

	// O(1) delete using swap-and-truncate pattern
	lastIdx := len(ms.metadata.Multiparts) - 1
	ms.metadata.Multiparts[idx] = ms.metadata.Multiparts[lastIdx]
	ms.metadata.Multiparts = ms.metadata.Multiparts[:lastIdx]

	return ms.saveUnlocked()
}

// GetExpiredUploads returns uploads older than TTL
// This method provides a thread-safe way to retrieve multipart uploads that have exceeded the specified TTL.
// It iterates through all uploads and returns those that were initiated before the current time minus the TTL.
// Parameters:
// - ttlSeconds: The time-to-live in seconds for multipart uploads
// Returns:
// - []model.Multipart: A slice of expired multipart uploads
func (ms *MetaStore) GetExpiredUploads(ttlSeconds int64) []model.Multipart {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	cutoffTime := time.Now().Unix() - ttlSeconds
	var expired []model.Multipart

	for _, upload := range ms.metadata.Multiparts {
		if upload.Initiated < cutoffTime {
			expired = append(expired, upload)
		}
	}

	return expired
}
