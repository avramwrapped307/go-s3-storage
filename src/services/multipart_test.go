package services

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"s3-storage/configuration"
	"s3-storage/model"
	"strings"
	"testing"
	"time"
)

// mockMultipartFile implements vfs.MultipartFile for testing
type mockMultipartFile struct {
	*bytes.Reader
}

func (m *mockMultipartFile) Close() error {
	return nil
}

func newMockMultipartFile(data []byte) *mockMultipartFile {
	return &mockMultipartFile{
		Reader: bytes.NewReader(data),
	}
}

func TestInitiateMultipartUpload(t *testing.T) {
	// Setup test environment
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	contentType := "application/octet-stream"
	ownerID := "test-user"

	// Create bucket first
	if err := metaStore.CreateBucket(bucket); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test initiate multipart upload
	uploadID, err := InitiateMultipartUpload(ctx, bucket, key, contentType, ownerID)
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Verify upload ID format (should be {timestamp}-{uuid})
	if !strings.Contains(uploadID, "-") {
		t.Errorf("Upload ID format incorrect: %s", uploadID)
	}

	// Verify upload metadata in metastore
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		t.Fatalf("Failed to get upload metadata: %v", err)
	}

	if upload.Bucket != bucket {
		t.Errorf("Expected bucket %s, got %s", bucket, upload.Bucket)
	}
	if upload.Key != key {
		t.Errorf("Expected key %s, got %s", key, upload.Key)
	}
	if upload.Owner != ownerID {
		t.Errorf("Expected owner %s, got %s", ownerID, upload.Owner)
	}
	if upload.ContentType != contentType {
		t.Errorf("Expected content type %s, got %s", contentType, upload.ContentType)
	}

	// Verify multipart directory created
	multipartDir := filepath.Join(tmpDir, ".multipart", uploadID)
	if _, err := os.Stat(multipartDir); os.IsNotExist(err) {
		t.Errorf("Multipart directory not created: %s", multipartDir)
	}
}

func TestUploadPart(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	// Create bucket and initiate upload
	if err := metaStore.CreateBucket(bucket); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	uploadID, err := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Test upload part
	partNumber := 1
	partData := []byte("This is test data for part 1")
	partSize := int64(len(partData))
	reader := newMockMultipartFile(partData)

	etag, err := UploadPart(ctx, bucket, key, partNumber, uploadID, reader, partSize)
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Verify ETag (should be SHA256 hash)
	expectedHash := sha256.Sum256(partData)
	expectedETag := hex.EncodeToString(expectedHash[:])
	if etag != expectedETag {
		t.Errorf("Expected ETag %s, got %s", expectedETag, etag)
	}

	// Verify part metadata in metastore
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		t.Fatalf("Failed to get upload metadata: %v", err)
	}

	part, exists := upload.Parts[partNumber]
	if !exists {
		t.Fatalf("Part %d not found in upload metadata", partNumber)
	}

	if part.PartNumber != partNumber {
		t.Errorf("Expected part number %d, got %d", partNumber, part.PartNumber)
	}
	if part.Size != partSize {
		t.Errorf("Expected part size %d, got %d", partSize, part.Size)
	}
	if part.ETag != etag {
		t.Errorf("Expected part ETag %s, got %s", etag, part.ETag)
	}

	// Verify part file created
	partPath := filepath.Join(tmpDir, ".multipart", uploadID, fmt.Sprintf("part.%d", partNumber))
	if _, err := os.Stat(partPath); os.IsNotExist(err) {
		t.Errorf("Part file not created: %s", partPath)
	}

	// Verify part file content
	savedData, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("Failed to read part file: %v", err)
	}
	if !bytes.Equal(savedData, partData) {
		t.Errorf("Part file content mismatch")
	}
}

func TestUploadPartInvalidPartNumber(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Test invalid part numbers
	testCases := []int{0, -1, configuration.PartMaxCount + 1}
	for _, partNumber := range testCases {
		t.Run(fmt.Sprintf("PartNumber=%d", partNumber), func(t *testing.T) {
			partData := []byte("test data")
			reader := newMockMultipartFile(partData)

			_, err := UploadPart(ctx, bucket, key, partNumber, uploadID, reader, int64(len(partData)))
			if err != ErrInvalidPartNumber {
				t.Errorf("Expected ErrInvalidPartNumber, got %v", err)
			}
		})
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	// Create bucket and initiate upload
	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "text/plain", ownerID)

	// Upload 3 parts
	partsData := [][]byte{
		bytes.Repeat([]byte("Part 1 "), 1024*1024), // ~7MB
		bytes.Repeat([]byte("Part 2 "), 1024*1024), // ~7MB
		[]byte("Part 3 final"),                     // small last part
	}

	var completedParts []model.CompletedPartRequest
	for i, data := range partsData {
		partNumber := i + 1
		reader := newMockMultipartFile(data)
		etag, err := UploadPart(ctx, bucket, key, partNumber, uploadID, reader, int64(len(data)))
		if err != nil {
			t.Fatalf("Failed to upload part %d: %v", partNumber, err)
		}
		completedParts = append(completedParts, model.CompletedPartRequest{
			PartNumber: partNumber,
			ETag:       etag,
		})
	}

	// Complete multipart upload
	meta, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, completedParts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify object metadata
	totalSize := int64(0)
	for _, data := range partsData {
		totalSize += int64(len(data))
	}

	if meta.Size != totalSize {
		t.Errorf("Expected size %d, got %d", totalSize, meta.Size)
	}

	// Verify multipart ETag format (should be {hash}-{part_count})
	if !strings.Contains(meta.ETag, "-3") {
		t.Errorf("Expected multipart ETag with -3 suffix, got %s", meta.ETag)
	}

	// Verify ETag calculation
	var combinedETags string
	for _, part := range completedParts {
		combinedETags += part.ETag
	}
	hashBytes := md5.Sum([]byte(combinedETags))
	expectedETag := fmt.Sprintf("%s-%d", hex.EncodeToString(hashBytes[:]), len(completedParts))
	if meta.ETag != expectedETag {
		t.Errorf("Expected ETag %s, got %s", expectedETag, meta.ETag)
	}

	// Verify parts array
	if len(meta.Parts) != len(partsData) {
		t.Errorf("Expected %d parts, got %d", len(partsData), len(meta.Parts))
	}

	// Verify object file created
	objectPath := filepath.Join(tmpDir, bucket, key, meta.DiskUUID, "part.1")
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		t.Errorf("Object file not created: %s", objectPath)
	}

	// Verify object file content
	savedData, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatalf("Failed to read object file: %v", err)
	}

	expectedData := bytes.Join(partsData, nil)
	if !bytes.Equal(savedData, expectedData) {
		t.Errorf("Object file content mismatch (expected %d bytes, got %d bytes)", len(expectedData), len(savedData))
	}

	// Verify multipart directory cleaned up
	multipartDir := filepath.Join(tmpDir, ".multipart", uploadID)
	if _, err := os.Stat(multipartDir); !os.IsNotExist(err) {
		t.Errorf("Multipart directory not cleaned up: %s", multipartDir)
	}

	// Verify upload removed from metastore
	_, err = metaStore.GetMultipartUpload(uploadID)
	if err != ErrNoSuchUpload {
		t.Errorf("Expected ErrNoSuchUpload, got %v", err)
	}
}

func TestCompleteMultipartUploadInvalidParts(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Upload 2 parts
	part1Data := bytes.Repeat([]byte("Part 1 "), 1024*1024) // ~7MB
	part2Data := bytes.Repeat([]byte("Part 2 "), 1024*1024) // ~7MB

	etag1, _ := UploadPart(ctx, bucket, key, 1, uploadID, newMockMultipartFile(part1Data), int64(len(part1Data)))
	etag2, _ := UploadPart(ctx, bucket, key, 2, uploadID, newMockMultipartFile(part2Data), int64(len(part2Data)))

	// Test: Missing part
	t.Run("MissingPart", func(t *testing.T) {
		completedParts := []model.CompletedPartRequest{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 3, ETag: "invalid-etag"}, // Part 3 doesn't exist
		}

		_, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, completedParts)
		if err != ErrInvalidPart {
			t.Errorf("Expected ErrInvalidPart, got %v", err)
		}
	})

	// Test: Wrong ETag
	t.Run("WrongETag", func(t *testing.T) {
		completedParts := []model.CompletedPartRequest{
			{PartNumber: 1, ETag: "wrong-etag"},
			{PartNumber: 2, ETag: etag2},
		}

		_, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, completedParts)
		if err != ErrInvalidPart {
			t.Errorf("Expected ErrInvalidPart, got %v", err)
		}
	})

	// Test: Empty parts list
	t.Run("EmptyParts", func(t *testing.T) {
		_, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, []model.CompletedPartRequest{})
		if err != ErrInvalidPart {
			t.Errorf("Expected ErrInvalidPart, got %v", err)
		}
	})
}

func TestCompleteMultipartUploadTooSmallParts(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Upload 2 parts: first part is too small (< 5MB), second part is OK
	part1Data := []byte("Too small") // < 5MB
	part2Data := []byte("Last part can be any size")

	etag1, _ := UploadPart(ctx, bucket, key, 1, uploadID, newMockMultipartFile(part1Data), int64(len(part1Data)))
	etag2, _ := UploadPart(ctx, bucket, key, 2, uploadID, newMockMultipartFile(part2Data), int64(len(part2Data)))

	completedParts := []model.CompletedPartRequest{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	}

	// Should fail because part 1 is too small (and it's not the last part)
	_, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, completedParts)
	if err != ErrEntityTooSmall {
		t.Errorf("Expected ErrEntityTooSmall, got %v", err)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Upload one part
	partData := bytes.Repeat([]byte("test"), 1024*1024) // ~4MB
	_, err := UploadPart(ctx, bucket, key, 1, uploadID, newMockMultipartFile(partData), int64(len(partData)))
	if err != nil {
		t.Fatalf("Failed to upload part: %v", err)
	}

	// Abort upload
	err = AbortMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify multipart directory removed
	multipartDir := filepath.Join(tmpDir, ".multipart", uploadID)
	if _, err := os.Stat(multipartDir); !os.IsNotExist(err) {
		t.Errorf("Multipart directory not removed: %s", multipartDir)
	}

	// Verify upload removed from metastore
	_, err = metaStore.GetMultipartUpload(uploadID)
	if err != ErrNoSuchUpload {
		t.Errorf("Expected ErrNoSuchUpload, got %v", err)
	}
}

func TestListParts(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Upload 5 parts
	for i := 1; i <= 5; i++ {
		partData := []byte(fmt.Sprintf("Part %d data", i))
		_, err := UploadPart(ctx, bucket, key, i, uploadID, newMockMultipartFile(partData), int64(len(partData)))
		if err != nil {
			t.Fatalf("Failed to upload part %d: %v", i, err)
		}
	}

	// Test list all parts
	t.Run("ListAll", func(t *testing.T) {
		result, err := ListParts(ctx, bucket, key, uploadID, 1000, 0)
		if err != nil {
			t.Fatalf("ListParts failed: %v", err)
		}

		if len(result.Parts) != 5 {
			t.Errorf("Expected 5 parts, got %d", len(result.Parts))
		}

		if result.IsTruncated {
			t.Errorf("Expected IsTruncated=false, got true")
		}
	})

	// Test pagination
	t.Run("Pagination", func(t *testing.T) {
		result, err := ListParts(ctx, bucket, key, uploadID, 2, 0)
		if err != nil {
			t.Fatalf("ListParts failed: %v", err)
		}

		if len(result.Parts) != 2 {
			t.Errorf("Expected 2 parts, got %d", len(result.Parts))
		}

		if !result.IsTruncated {
			t.Errorf("Expected IsTruncated=true, got false")
		}

		if result.NextPartNumberMarker != 2 {
			t.Errorf("Expected NextPartNumberMarker=2, got %d", result.NextPartNumberMarker)
		}
	})

	// Test with marker
	t.Run("WithMarker", func(t *testing.T) {
		result, err := ListParts(ctx, bucket, key, uploadID, 1000, 2)
		if err != nil {
			t.Fatalf("ListParts failed: %v", err)
		}

		if len(result.Parts) != 3 {
			t.Errorf("Expected 3 parts (3, 4, 5), got %d", len(result.Parts))
		}

		if result.Parts[0].PartNumber != 3 {
			t.Errorf("Expected first part to be 3, got %d", result.Parts[0].PartNumber)
		}
	})
}

func TestCleanupExpiredUploads(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)

	// Create an old upload (simulate by directly modifying metastore)
	oldUploadID := "old-upload-id"
	oldUpload := model.Multipart{
		UploadID:  oldUploadID,
		Bucket:    bucket,
		Key:       "old-object.bin",
		Initiated: time.Now().Unix() - (25 * 60 * 60), // 25 hours ago
		Owner:     ownerID,
		Parts:     make(map[int]model.PartUpload),
	}

	// Create directory for old upload
	oldMultipartDir := filepath.Join(tmpDir, ".multipart", oldUploadID)
	os.MkdirAll(oldMultipartDir, 0755)

	// Add to metastore
	metaStore.AddMultipartUpload(oldUpload)

	// Create a recent upload (should not be cleaned up)
	recentUploadID, _ := InitiateMultipartUpload(ctx, bucket, "recent-object.bin", "", ownerID)

	// Run cleanup
	err := CleanupExpiredUploads(ctx)
	if err != nil {
		t.Fatalf("CleanupExpiredUploads failed: %v", err)
	}

	// Verify old upload removed
	_, err = metaStore.GetMultipartUpload(oldUploadID)
	if err != ErrNoSuchUpload {
		t.Errorf("Old upload should be removed, got error: %v", err)
	}

	// Verify old directory removed
	if _, err := os.Stat(oldMultipartDir); !os.IsNotExist(err) {
		t.Errorf("Old multipart directory should be removed")
	}

	// Verify recent upload still exists
	_, err = metaStore.GetMultipartUpload(recentUploadID)
	if err != nil {
		t.Errorf("Recent upload should still exist, got error: %v", err)
	}
}

// Helper function to setup test services
func setupTestServices(t *testing.T, tmpDir string) {
	var err error
	metaStore, err = NewMetaStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create metastore: %v", err)
	}

	storageDir = tmpDir
}

func TestUploadPartOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)
	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	partNumber := 1

	// Upload part 1 first time
	firstData := []byte("First upload")
	firstETag, err := UploadPart(ctx, bucket, key, partNumber, uploadID, newMockMultipartFile(firstData), int64(len(firstData)))
	if err != nil {
		t.Fatalf("First UploadPart failed: %v", err)
	}

	// Upload part 1 second time (should overwrite)
	secondData := []byte("Second upload - overwrites first")
	secondETag, err := UploadPart(ctx, bucket, key, partNumber, uploadID, newMockMultipartFile(secondData), int64(len(secondData)))
	if err != nil {
		t.Fatalf("Second UploadPart failed: %v", err)
	}

	// ETags should be different
	if firstETag == secondETag {
		t.Errorf("ETags should be different after overwrite")
	}

	// Verify only second upload data exists
	partPath := filepath.Join(tmpDir, ".multipart", uploadID, fmt.Sprintf("part.%d", partNumber))
	savedData, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("Failed to read part file: %v", err)
	}

	if !bytes.Equal(savedData, secondData) {
		t.Errorf("Part file should contain second upload data")
	}

	// Verify metadata has second ETag
	upload, _ := metaStore.GetMultipartUpload(uploadID)
	if upload.Parts[partNumber].ETag != secondETag {
		t.Errorf("Metadata should have second ETag")
	}
}

func TestCompleteMultipartUploadContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	setupTestServices(t, tmpDir)

	bucket := "test-bucket"
	key := "test-object.bin"
	ownerID := "test-user"

	metaStore.CreateBucket(bucket)

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	uploadID, _ := InitiateMultipartUpload(ctx, bucket, key, "", ownerID)

	// Upload 2 large parts
	part1Data := bytes.Repeat([]byte("Part 1 "), 1024*1024) // ~7MB
	part2Data := bytes.Repeat([]byte("Part 2 "), 1024*1024) // ~7MB

	etag1, _ := UploadPart(ctx, bucket, key, 1, uploadID, newMockMultipartFile(part1Data), int64(len(part1Data)))
	etag2, _ := UploadPart(ctx, bucket, key, 2, uploadID, newMockMultipartFile(part2Data), int64(len(part2Data)))

	completedParts := []model.CompletedPartRequest{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	}

	// Cancel context immediately
	cancel()

	// Complete should fail with context cancellation error
	_, err := CompleteMultipartUpload(ctx, bucket, key, uploadID, completedParts)
	if err == nil {
		t.Errorf("Expected context cancellation error, got nil")
	}
}
