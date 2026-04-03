package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"s3-storage/model"
	"s3-storage/vfs"

	"github.com/danbordeanu/go-logger"
)

const xlMetaVersion = 1

// PutObject stores an object in the bucket
// It creates the necessary directory structure, writes the part file, detects content type, and writes xl.meta atomically
// It also updates bucket stats asynchronously after a successful write
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket to store the object in
// - key: the object key (path within the bucket)
// - reader: a MultipartFile reader containing the object data
// - size: the size of the object in bytes (can be -1 if unknown, but Magika scanning may be less accurate)
// - etag: the ETag value for the object (should be provided by caller, e.g. as MD5 hash of content)
// Returns the ObjectMeta of the stored object or an error
// Example usage:
// meta, err := PutObject(ctx, "my-bucket", "path/to/object.txt", fileReader, fileSize, "etag-value")
//
//	if err != nil {
//	    // handle error
//	}
//
// fmt.Printf("Object stored with ETag: %s and Content-Type: %s\n", meta.ETag, meta.ContentType)
func PutObject(ctx context.Context, bucket, key string, reader vfs.MultipartFile, size int64, etag string) (*model.ObjectMeta, error) {
	log := logger.SugaredLogger()

	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return nil, ErrNoSuchBucket
	}

	// Get persistent disk UUID
	diskUUID := metaStore.GetDiskUUID()

	// Build paths
	objectPath := buildObjectPath(bucket, key)
	diskPath := filepath.Join(objectPath, diskUUID)
	partPath := filepath.Join(diskPath, "part.1")
	xlMetaPath := filepath.Join(objectPath, "xl.meta")

	// Clean up any existing object at this key to prevent conflicts
	if _, err := os.Stat(objectPath); err == nil {
		if err := os.RemoveAll(objectPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing object: %w", err)
		}
	}

	// Create directory structure
	if err := os.MkdirAll(diskPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// Write part.1 file
	partFile, err := os.Create(partPath)
	if err != nil {
		// Cleanup on failure
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to create part file: %w", err)
	}

	// Use context-aware copy to monitor for cancellation
	written, err := vfs.CopyWithContext(ctx, partFile, reader)
	if err != nil {
		partFile.Close()
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to write part data: %w", err)
	}

	if err = partFile.Sync(); err != nil {
		partFile.Close()
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to sync part file: %w", err)
	}
	partFile.Close()

	// Detect content type using Magika
	contentType := "application/octet-stream"
	if MagikaScanner != nil {
		// Rewind reader for scanning
		if _, err = reader.Seek(0, io.SeekStart); err == nil {
			// Use semaphore to limit concurrent scans
			select {
			case ScanSem <- struct{}{}:
				// Use actual written size (not request ContentLength which may be -1 for chunked encoding)
				result, scanErr := MagikaScanner.Scan(reader, int(written))
				<-ScanSem
				if scanErr == nil && result.MimeType != "" {
					contentType = result.MimeType
				}
			case <-ctx.Done():
				// Context canceled, use default content type
			}
		}
	}

	// Build object metadata
	now := time.Now().Unix()
	meta := &model.ObjectMeta{
		Version:      xlMetaVersion,
		Size:         written,
		ETag:         etag,
		LastModified: now,
		ContentType:  contentType,
		DiskUUID:     diskUUID,
		Parts: []model.Part{
			{
				Number: 1,
				Size:   written,
				ETag:   etag,
			},
		},
	}

	// Write xl.meta atomically (temp file + rename)
	if err = writeXLMetaAtomically(xlMetaPath, meta); err != nil {
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to write xl.meta: %w", err)
	}

	// Update bucket stats synchronously to ensure quota enforcement works correctly
	if err := metaStore.UpdateBucketStats(bucket, written, 1); err != nil {
		log.Errorf("Failed to update bucket stats for %s: %v", bucket, err)
	}

	return meta, nil
}

// GetObject retrieves an object's metadata and returns a reader for its data
// It validates bucket and object existence, reads xl.meta for metadata, and opens the part.1 file for reading
// Parameters:
// - bucket: the name of the bucket containing the object
// - key: the object key (path within the bucket)
// Returns the ObjectMeta, an os.File reader for the object's data, or an error
// Example usage:
// meta, file, err := GetObject("my-bucket", "path/to/object.txt")
//
//	if err != nil {
//	    // handle error
//	}
//
// defer file.Close()
// fmt.Printf("Object metadata: Size=%d, ETag=%s, Content-Type=%s\n", meta.Size, meta.ETag, meta.ContentType)
func GetObject(bucket, key string) (*model.ObjectMeta, vfs.MultipartFile, error) {
	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return nil, nil, ErrNoSuchBucket
	}

	// Build paths
	objectPath := buildObjectPath(bucket, key)
	xlMetaPath := filepath.Join(objectPath, "xl.meta")

	// Read and unmarshal xl.meta
	data, err := os.ReadFile(xlMetaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNoSuchKey
		}
		return nil, nil, err
	}

	var meta model.ObjectMeta
	if _, err = meta.UnmarshalMsg(data); err != nil {
		return nil, nil, err
	}

	// Open part.1 file
	partPath := filepath.Join(objectPath, meta.DiskUUID, "part.1")
	f, err := os.Open(partPath)
	if err != nil {
		return nil, nil, err
	}

	// Ensure file is positioned at the start
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}

	// Wrap os.File as vfs.MultipartFile (it already implements ReadSeekCloser)
	var mfile vfs.MultipartFile = f

	// Debug: Check actual file size vs metadata size
	fileInfo, _ := f.Stat()
	if fileInfo != nil {
		actualSize := fileInfo.Size()
		fmt.Printf("DEBUG GetObject: xl.meta Size=%d, actual file size=%d, path=%s\n", meta.Size, actualSize, partPath)
		if actualSize != meta.Size {
			fmt.Printf("DEBUG GetObject: WARNING - Size mismatch! Meta says %d but file is %d bytes\n", meta.Size, actualSize)
		}
	}

	return &meta, mfile, nil
}

// HeadObject retrieves only the metadata of an object without opening the data file
// It validates bucket and object existence and reads xl.meta for metadata
// Parameters:
// - bucket: the name of the bucket containing the object
// - key: the object key (path within the bucket)
// Returns the ObjectMeta or an error
// Example usage:
// meta, err := HeadObject("my-bucket", "path/to/object.txt")
//
//	if err != nil {
//	    // handle error
//	}
//
// fmt.Printf("Object metadata: Size=%d, ETag=%s, Content-Type=%s\n", meta.Size, meta.ETag, meta.ContentType)
func HeadObject(bucket, key string) (*model.ObjectMeta, error) {
	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return nil, ErrNoSuchBucket
	}

	// Build path to xl.meta
	objectPath := buildObjectPath(bucket, key)
	xlMetaPath := filepath.Join(objectPath, "xl.meta")

	// Read and unmarshal xl.meta
	data, err := os.ReadFile(xlMetaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}

	var meta model.ObjectMeta
	if _, err = meta.UnmarshalMsg(data); err != nil {
		return nil, err
	}

	return &meta, nil

}

// DeleteObject deletes an object (must exist)
// It validates bucket and object existence, reads xl.meta to get size for stats update, removes the entire object directory, and updates bucket stats asynchronously
// Parameters:
// - bucket: the name of the bucket containing the object
// - key: the object key (path within the bucket)
// Returns an error if deletion fails or if the bucket/object does not exist
// Example usage:
// err := DeleteObject("my-bucket", "path/to/object.txt")
//
//	if err != nil {
//	    // handle error
//	}
//
// fmt.Println("Object deleted successfully")
func DeleteObject(bucket, key string) error {
	log := logger.SugaredLogger()

	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return ErrNoSuchBucket
	}

	// Build paths
	objectPath := buildObjectPath(bucket, key)
	xlMetaPath := filepath.Join(objectPath, "xl.meta")

	// Read xl.meta to verify object exists and get size for stats update
	data, err := os.ReadFile(xlMetaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNoSuchKey
		}
		return err
	}

	// Parse metadata to get object size
	var meta model.ObjectMeta
	if _, err = meta.UnmarshalMsg(data); err != nil {
		return fmt.Errorf("failed to parse object metadata: %w", err)
	}
	objectSize := meta.Size

	// Remove the entire object directory (xl.meta + disk UUID subdirectory with part.1)
	if err = os.RemoveAll(objectPath); err != nil {
		return err
	}

	// Update bucket stats synchronously to ensure quota enforcement works correctly (decrement size and count)
	if err := metaStore.UpdateBucketStats(bucket, -objectSize, -1); err != nil {
		log.Errorf("Failed to update bucket stats for %s: %v", bucket, err)
	}

	return nil
}

// ObjectExists checks if an object exists by verifying xl.meta presence
// It validates bucket existence and checks for the presence of xl.meta file in the expected object directory
// Parameters:
// - bucket: the name of the bucket containing the object
// - key: the object key (path within the bucket)
// Returns true if the object exists, false otherwise
// Example usage:
// exists := ObjectExists("my-bucket", "path/to/object.txt")
func ObjectExists(bucket, key string) bool {
	objectPath := buildObjectPath(bucket, key)
	xlMetaPath := filepath.Join(objectPath, "xl.meta")
	_, err := os.Stat(xlMetaPath)
	return err == nil
}

// buildObjectPath returns the full path to an object directory
func buildObjectPath(bucket, key string) string {
	return filepath.Join(storageDir, bucket, key)
}

// ObjectInfo holds information about an object for listing
type ObjectInfo struct {
	Key          string `json:"key"`
	Name         string `json:"name"` // Last component of the key (file name)
	Size         int64  `json:"size"`
	LastModified int64  `json:"last_modified"`
	ContentType  string `json:"content_type"`
	ETag         string `json:"etag"`
}

// ListObjectsResult holds the result of a ListObjects call
type ListObjectsResult struct {
	Bucket         string       `json:"bucket"`
	Prefix         string       `json:"prefix"`
	Delimiter      string       `json:"delimiter"`
	IsTruncated    bool         `json:"is_truncated"`
	MaxKeys        int          `json:"max_keys"`
	Objects        []ObjectInfo `json:"objects"`
	CommonPrefixes []string     `json:"common_prefixes"` // "folders"
}

// ListObjectsPaginatedResult holds the result of a paginated ListObjects call
type ListObjectsPaginatedResult struct {
	Bucket         string       `json:"bucket"`
	Prefix         string       `json:"prefix"`
	Delimiter      string       `json:"delimiter"`
	Objects        []ObjectInfo `json:"objects"`
	CommonPrefixes []string     `json:"common_prefixes"`
	TotalObjects   int          `json:"total_objects"`
	TotalFolders   int          `json:"total_folders"`
	Page           int          `json:"page"`
	PerPage        int          `json:"per_page"`
	TotalPages     int          `json:"total_pages"`
	SortBy         string       `json:"sort_by"`
	SortOrder      string       `json:"sort_order"`
	Search         string       `json:"search"`
}

// ListObjects lists objects in a bucket with optional prefix filtering
// It validates bucket existence, walks the bucket directory to find xl.meta files, filters by prefix and delimiter, and returns a list of objects and common prefixes (folders) up to maxKeys
// Parameters:
// - bucket: the name of the bucket to list objects from
// - prefix: optional prefix to filter objects (only keys that start with this prefix will be included)
// - delimiter: optional delimiter to simulate folders (e.g. "/")
// - maxKeys: maximum number of objects to return (default 1000 if <= 0)
// Returns a ListObjectsResult containing the list of objects and common prefixes, or an error
// Example usage:
// result, err := ListObjects("my-bucket", "path/to/", "/", 100)
//
//	if err != nil {
//	    // handle error
//	}
//
// fmt.Printf("Found %d objects and %d common prefixes\n", len(result.Objects), len(result.CommonPrefixes))
func ListObjects(bucket, prefix, delimiter string, maxKeys int) (*ListObjectsResult, error) {
	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return nil, ErrNoSuchBucket
	}

	if maxKeys <= 0 {
		maxKeys = 1000
	}

	result := &ListObjectsResult{
		Bucket:         bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		MaxKeys:        maxKeys,
		Objects:        make([]ObjectInfo, 0),
		CommonPrefixes: make([]string, 0),
	}

	bucketPath := filepath.Join(storageDir, bucket)

	// Track common prefixes (directories) when using delimiter
	seenPrefixes := make(map[string]bool)

	// Walk the bucket directory
	err := filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Only process xl.meta files (they mark objects)
		if info.IsDir() || info.Name() != "xl.meta" {
			return nil
		}

		// Get relative path from bucket root
		relPath, err := filepath.Rel(bucketPath, filepath.Dir(path))
		if err != nil {
			return nil
		}

		// Convert to S3-style key
		key := filepath.ToSlash(relPath)

		// Filter by prefix
		if prefix != "" && !hasPrefix(key, prefix) {
			return nil
		}

		// Handle delimiter (folder simulation)
		if delimiter != "" {
			// Get the part of the key after the prefix
			keyAfterPrefix := key
			if prefix != "" {
				keyAfterPrefix = key[len(prefix):]
			}

			// Check if there's a delimiter in the remaining key
			delimIdx := indexOf(keyAfterPrefix, delimiter)
			if delimIdx >= 0 {
				// This is a "folder" - add to common prefixes
				commonPrefix := prefix + keyAfterPrefix[:delimIdx+len(delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
				return nil
			}
		}

		// Check if we've reached max keys
		if len(result.Objects) >= maxKeys {
			result.IsTruncated = true
			return filepath.SkipAll
		}

		// Read object metadata
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var meta model.ObjectMeta
		if _, err = meta.UnmarshalMsg(data); err != nil {
			return nil
		}

		// Extract the file name from the key
		name := key
		if lastSlash := lastIndexOf(key, "/"); lastSlash >= 0 {
			name = key[lastSlash+1:]
		}

		result.Objects = append(result.Objects, ObjectInfo{
			Key:          key,
			Name:         name,
			Size:         meta.Size,
			LastModified: meta.LastModified,
			ContentType:  meta.ContentType,
			ETag:         meta.ETag,
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// ListObjectsPaginated lists objects with pagination, sorting, and search support
// It validates bucket existence, walks the bucket directory to find xl.meta files, filters by prefix, delimiter, and search term, sorts by the specified field and order, and returns a paginated list of objects and common prefixes (folders)
// Parameters:
// - bucket: the name of the bucket to list objects from
// - prefix: optional prefix to filter objects (only keys that start with this prefix will be included)
// - delimiter: optional delimiter to simulate folders (e.g. "/")
// - page: page number to return (1-based, default 1)
// - perPage: number of objects per page (default 50)
// - sortBy: field to sort by ("name", "size", "date", "content_type")
// - sortOrder: "asc" or "desc" (default "asc")
// - search: optional search term to filter objects by name or key (case-insensitive substring match)
// Returns a ListObjectsPaginatedResult containing the paginated list of objects and common prefixes, or an error
// Example usage:
// result, err := ListObjectsPaginated("my-bucket", "path/to/", "/", 1, 20, "name", "asc", "report")
//
//	if err != nil {
//	    // handle error
//	}
//
// fmt.Printf("Page %d of %d: Found %d objects and %d common prefixes\n", result.Page, result.TotalPages, len(result.Objects), len(result.CommonPrefixes))
func ListObjectsPaginated(bucket, prefix, delimiter string, page, perPage int, sortBy, sortOrder, search string) (*ListObjectsPaginatedResult, error) {
	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return nil, ErrNoSuchBucket
	}

	// Defaults
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 50
	}
	if sortBy == "" {
		sortBy = "name"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	bucketPath := filepath.Join(storageDir, bucket)

	// Collect all objects and common prefixes
	allObjects := make([]ObjectInfo, 0)
	seenPrefixes := make(map[string]bool)
	commonPrefixes := make([]string, 0)

	// Walk the bucket directory
	err := filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if info.IsDir() || info.Name() != "xl.meta" {
			return nil
		}

		relPath, err := filepath.Rel(bucketPath, filepath.Dir(path))
		if err != nil {
			return nil
		}

		key := filepath.ToSlash(relPath)

		// Filter by prefix
		if prefix != "" && !hasPrefix(key, prefix) {
			return nil
		}

		// Handle delimiter (folder simulation)
		if delimiter != "" {
			keyAfterPrefix := key
			if prefix != "" {
				keyAfterPrefix = key[len(prefix):]
			}

			delimIdx := indexOf(keyAfterPrefix, delimiter)
			if delimIdx >= 0 {
				commonPrefix := prefix + keyAfterPrefix[:delimIdx+len(delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					commonPrefixes = append(commonPrefixes, commonPrefix)
				}
				return nil
			}
		}

		// Read object metadata
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var meta model.ObjectMeta
		if _, err = meta.UnmarshalMsg(data); err != nil {
			return nil
		}

		name := key
		if lastSlash := lastIndexOf(key, "/"); lastSlash >= 0 {
			name = key[lastSlash+1:]
		}

		allObjects = append(allObjects, ObjectInfo{
			Key:          key,
			Name:         name,
			Size:         meta.Size,
			LastModified: meta.LastModified,
			ContentType:  meta.ContentType,
			ETag:         meta.ETag,
		})

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Apply search filter (case-insensitive)
	if search != "" {
		searchLower := strings.ToLower(search)
		filtered := make([]ObjectInfo, 0)
		for _, obj := range allObjects {
			if strings.Contains(strings.ToLower(obj.Name), searchLower) ||
				strings.Contains(strings.ToLower(obj.Key), searchLower) {
				filtered = append(filtered, obj)
			}
		}
		allObjects = filtered

		// Also filter common prefixes
		filteredPrefixes := make([]string, 0)
		for _, p := range commonPrefixes {
			if strings.Contains(strings.ToLower(p), searchLower) {
				filteredPrefixes = append(filteredPrefixes, p)
			}
		}
		commonPrefixes = filteredPrefixes
	}

	// Sort objects
	sortObjects(allObjects, sortBy, sortOrder)

	// Sort common prefixes alphabetically
	sortPrefixes(commonPrefixes, sortOrder)

	// Calculate pagination
	totalObjects := len(allObjects)
	totalFolders := len(commonPrefixes)
	totalPages := (totalObjects + perPage - 1) / perPage
	if totalPages < 1 {
		totalPages = 1
	}

	// Apply pagination to objects
	start := (page - 1) * perPage
	end := start + perPage
	if start > totalObjects {
		start = totalObjects
	}
	if end > totalObjects {
		end = totalObjects
	}

	paginatedObjects := allObjects[start:end]

	return &ListObjectsPaginatedResult{
		Bucket:         bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		Objects:        paginatedObjects,
		CommonPrefixes: commonPrefixes,
		TotalObjects:   totalObjects,
		TotalFolders:   totalFolders,
		Page:           page,
		PerPage:        perPage,
		TotalPages:     totalPages,
		SortBy:         sortBy,
		SortOrder:      sortOrder,
		Search:         search,
	}, nil
}

// sortObjects sorts objects by the specified field and order
// Supported sortBy values: "name", "size", "date" (last modified), "content_type"
// sortOrder can be "asc" or "desc"
// It uses sort.Slice with a custom less function based on the sortBy field and sortOrder
// Example usage:
// sortObjects(objects, "size", "desc") // Sort objects by size in descending order
func sortObjects(objects []ObjectInfo, sortBy, sortOrder string) {
	sort.Slice(objects, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "size":
			less = objects[i].Size < objects[j].Size
		case "date", "last_modified":
			less = objects[i].LastModified < objects[j].LastModified
		case "type", "content_type":
			less = objects[i].ContentType < objects[j].ContentType
		default: // "name"
			less = strings.ToLower(objects[i].Name) < strings.ToLower(objects[j].Name)
		}
		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// sortPrefixes sorts common prefixes alphabetically
// sortOrder can be "asc" or "desc"
// It uses sort.Slice with a custom less function based on case-insensitive comparison and sortOrder
// Example usage:
// sortPrefixes(prefixes, "asc") // Sort prefixes in ascending order
func sortPrefixes(prefixes []string, sortOrder string) {
	sort.Slice(prefixes, func(i, j int) bool {
		less := strings.ToLower(prefixes[i]) < strings.ToLower(prefixes[j])
		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// hasPrefix checks if key starts with prefix
// It compares the beginning of the key with the prefix and returns true if they match
// Example usage:
// hasPrefix("path/to/object.txt", "path/to/") // returns true
// hasPrefix("path/to/object.txt", "other/") // returns false
func hasPrefix(key, prefix string) bool {
	return len(key) >= len(prefix) && key[:len(prefix)] == prefix
}

// indexOf finds the first occurrence of substr in s
// It iterates through s and checks for substr at each position, returning the index of the first match or -1 if not found
// Parameters:
// - s: the string to search within
// - substr: the substring to find
// Returns the index of the first occurrence of substr in s, or -1 if not found
// Example usage:
// indexOf("path/to/object.txt", "/") // returns 4
// indexOf("path/to/object.txt", "obj") // returns 8
// indexOf("path/to/object.txt", "xyz") // returns -1
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// lastIndexOf finds the last occurrence of substr in s
// It iterates through s from the end and checks for substr at each position, returning the index of the last match or -1 if not found
// Parameters:
// - s: the string to search within
// - substr: the substring to find
// Returns the index of the last occurrence of substr in s, or -1 if not found
// Example usage:
// lastIndexOf("path/to/object.txt", "/") // returns 7
// lastIndexOf("path/to/object.txt", "obj") // returns 8
// lastIndexOf("path/to/object.txt", "xyz") // returns -1
func lastIndexOf(s, substr string) int {
	for i := len(s) - len(substr); i >= 0; i-- {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// writeXLMetaAtomically writes xl.meta using temp file + rename for atomicity
// It creates a temporary file, writes the marshaled metadata to it, syncs and closes the file, and then renames it to the target xl.meta path. If any step fails, it cleans up the temp file and returns an error.
// Parameters:
// - xlMetaPath: the target path for xl.meta (e.g. /storage/bucket/key/xl.meta)
// - meta: the ObjectMeta to write to xl.meta
// Returns an error if writing fails at any step
// Example usage:
// err := writeXLMetaAtomically("/storage/my-bucket/path/to/object/xl.meta", meta)
//
//	if err != nil {
//	    // handle error
//	}
func writeXLMetaAtomically(xlMetaPath string, meta *model.ObjectMeta) error {
	tmpPath := xlMetaPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	data, err := meta.MarshalMsg(nil)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
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

	return os.Rename(tmpPath, xlMetaPath)
}
