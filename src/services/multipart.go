package services

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"s3-storage/configuration"
	"s3-storage/model"
	"s3-storage/vfs"

	"github.com/danbordeanu/go-logger"
)

// InitiateMultipartUpload creates a new multipart upload
// It validates bucket exists and permissions, generates an upload ID, creates temp directory, and adds to metastore
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket
// - key: the object key
// - contentType: the content type for the final object
// - ownerID: the ID of the user initiating the upload
// Returns the upload ID or an error
func InitiateMultipartUpload(ctx context.Context, bucket, key, contentType, ownerID string) (string, error) {
	// Validate bucket exists
	if !metaStore.BucketExists(bucket) {
		return "", ErrNoSuchBucket
	}

	// Generate upload ID: {timestamp}-{uuid}
	uploadID := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), uuid.New().String())

	// Create temp directory for parts: <storage>/.multipart/<uploadID>/
	multipartDir := filepath.Join(storageDir, ".multipart", uploadID)
	if err := os.MkdirAll(multipartDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create multipart directory: %w", err)
	}

	// Initialize upload metadata
	upload := model.Multipart{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		Initiated:   time.Now().Unix(),
		Owner:       ownerID,
		Parts:       make(map[int]model.PartUpload),
		ContentType: contentType,
	}

	// Add to metastore
	if err := metaStore.AddMultipartUpload(upload); err != nil {
		// Cleanup on failure
		os.RemoveAll(multipartDir)
		return "", fmt.Errorf("failed to add multipart upload: %w", err)
	}

	return uploadID, nil
}

// UploadPart uploads a single part of a multipart upload
// It validates part number and size, streams to temp file, calculates SHA256, and updates metastore
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket
// - key: the object key
// - partNumber: the part number (1-10000)
// - uploadID: the upload ID from InitiateMultipartUpload
// - reader: the part data reader (HTTP body stream)
// - expectedSize: expected size (from Content-Length header, or 0 for chunked encoding)
// Returns the ETag (SHA256 hash), actual bytes written, or an error
func UploadPart(ctx context.Context, bucket, key string, partNumber int, uploadID string, reader io.Reader, expectedSize int64) (string, int64, error) {
	log := logger.SugaredLogger()
	// Validate part number
	if partNumber < 1 || partNumber > configuration.PartMaxCount {
		return "", 0, ErrInvalidPartNumber
	}

	// Get upload metadata to verify bucket/key match
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		return "", 0, err
	}

	if upload.Bucket != bucket || upload.Key != key {
		return "", 0, ErrNoSuchUpload
	}

	// Build part path: <storage>/.multipart/<uploadID>/part.{partNumber}
	multipartDir := filepath.Join(storageDir, ".multipart", uploadID)
	partPath := filepath.Join(multipartDir, fmt.Sprintf("part.%d", partNumber))

	// Use temp file + rename for atomic write
	tmpPath := partPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp part file: %w", err)
	}

	// Calculate SHA256 while writing (using MultiWriter)
	hash := sha256.New()
	multiWriter := io.MultiWriter(tmpFile, hash)

	// Stream directly from HTTP body to part file
	// For Content-Length mode: limit to expectedSize and validate
	// For chunked encoding: read until EOF and validate afterwards
	var written int64
	if expectedSize > 0 {
		// Content-Length present - validate and limit
		if expectedSize > configuration.PartMaxSize {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, ErrEntityTooLarge
		}

		// Limit reader to expected size
		limitedReader := io.LimitReader(reader, expectedSize)
		written, err = vfs.CopyWithContext(ctx, multiWriter, limitedReader)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, fmt.Errorf("failed to write part data: %w", err)
		}

		// Verify we received exactly the expected amount
		if written != expectedSize {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", expectedSize, written)
		}
	} else {
		// Chunked encoding: read until EOF
		written, err = vfs.CopyWithContext(ctx, multiWriter, reader)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, fmt.Errorf("failed to write part data: %w", err)
		}

		// Validate size constraints after reading
		if written <= 0 {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, fmt.Errorf("no data received")
		}

		if written > configuration.PartMaxSize {
			tmpFile.Close()
			os.Remove(tmpPath)
			return "", 0, ErrEntityTooLarge
		}

		// Diagnostic: check if there's extra data beyond a clean 64MB boundary
		// AWS CLI typically uses 64MB (67108864 bytes) chunks
		cleanChunkSize := int64(64 * 1024 * 1024) // 67108864
		if written > cleanChunkSize {
			extraBytes := written - cleanChunkSize
			log.Debugf("UploadPart: Part %d has %d extra bytes beyond 64MB boundary (total: %d, clean: %d)",
				partNumber, extraBytes, written, cleanChunkSize)
		}
	}

	log.Debugf("UploadPart: Received %d bytes for part %d (expected: %d)", written, partNumber, expectedSize)

	// Check quota AFTER we know the actual size
	var existingPartsSize int64
	for _, part := range upload.Parts {
		existingPartsSize += part.Size
	}
	totalSizeAfterUpload := existingPartsSize + written
	if err := CheckStorageQuota(totalSizeAfterUpload); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", 0, err
	}

	// Truncate file to exact size to remove any potential extra bytes
	// This ensures the part file on disk is exactly the size we expect
	if err = tmpFile.Truncate(written); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", 0, fmt.Errorf("failed to truncate part file: %w", err)
	}

	if err = tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", 0, fmt.Errorf("failed to sync part file: %w", err)
	}

	if err = tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return "", 0, fmt.Errorf("failed to close part file: %w", err)
	}

	// Atomic rename
	if err = os.Rename(tmpPath, partPath); err != nil {
		os.Remove(tmpPath)
		return "", 0, fmt.Errorf("failed to rename part file: %w", err)
	}

	// Calculate ETag (SHA256 hex)
	etag := hex.EncodeToString(hash.Sum(nil))

	// Update part metadata in metastore
	partUpload := model.PartUpload{
		PartNumber:   partNumber,
		Size:         written,
		ETag:         etag,
		LastModified: time.Now().Unix(),
	}

	if err = metaStore.UpdateMultipartPart(uploadID, partNumber, partUpload); err != nil {
		return "", 0, fmt.Errorf("failed to update part metadata: %w", err)
	}

	// Debug: log stored part metadata
	log.Debugf("UploadPart: uploadID=%s part=%d storedETag=%s size=%d", uploadID, partNumber, etag, written)

	return etag, written, nil
}

// CompleteMultipartUpload assembles parts into final object
// It validates all parts exist and are in order, checks quota, assembles parts sequentially, calculates multipart ETag, detects content type, and writes xl.meta
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket
// - key: the object key
// - uploadID: the upload ID
// - parts: the list of completed parts (must be in ascending order)
// Returns the ObjectMeta of the completed object or an error
func CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []model.CompletedPartRequest) (*model.ObjectMeta, error) {
	log := logger.SugaredLogger()
	// Get upload metadata
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}

	if upload.Bucket != bucket || upload.Key != key {
		return nil, ErrNoSuchUpload
	}

	// Validate all parts exist and are in ascending order
	if len(parts) == 0 {
		return nil, ErrInvalidPart
	}

	// Sort parts by part number
	sortedParts := make([]model.CompletedPartRequest, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// Validate ascending order
	for i := 0; i < len(sortedParts)-1; i++ {
		if sortedParts[i].PartNumber >= sortedParts[i+1].PartNumber {
			return nil, ErrInvalidPartOrder
		}
	}

	// Debug: log stored parts and incoming parts for diagnosis
	storedParts := make([]int, 0, len(upload.Parts))
	for pn := range upload.Parts {
		storedParts = append(storedParts, pn)
	}
	sort.Ints(storedParts)
	inParts := make([]string, 0, len(sortedParts))
	for _, p := range sortedParts {
		inParts = append(inParts, fmt.Sprintf("%d:%s", p.PartNumber, strings.Trim(p.ETag, "\"")))
	}
	log.Debugf("CompleteMultipart: uploadID=%s stored parts=%v incoming parts=%v", uploadID, storedParts, inParts)

	// Validate all parts exist in upload metadata and match ETags
	var totalSize int64
	partMetadata := make([]model.PartUpload, len(sortedParts))
	for i, part := range sortedParts {
		partUpload, exists := upload.Parts[part.PartNumber]
		if !exists {
			log.Debugf("CompleteMultipart: missing part %d for upload %s", part.PartNumber, uploadID)
			return nil, ErrInvalidPart
		}

		// Normalize incoming ETag (clients often send quoted ETags like "abc...")
		incomingETag := strings.Trim(part.ETag, "\"")

		// Compare ETags case-insensitively
		if !strings.EqualFold(partUpload.ETag, incomingETag) {
			log.Debugf("CompleteMultipart: etag mismatch for part %d: stored=%s incoming=%s", part.PartNumber, partUpload.ETag, incomingETag)
			return nil, ErrInvalidPart
		}

		partMetadata[i] = partUpload
		totalSize += partUpload.Size
	}

	// Validate part sizes (all except last must be >= 5MB)
	for i, part := range partMetadata {
		isLast := (i == len(partMetadata)-1)
		if !isLast && part.Size < configuration.PartMinSize {
			return nil, ErrEntityTooSmall
		}
	}

	// Check quota on total size
	if err := CheckStorageQuota(totalSize); err != nil {
		return nil, err
	}

	// Get persistent disk UUID
	diskUUID := metaStore.GetDiskUUID()

	// Build paths for final object
	objectPath := buildObjectPath(bucket, key)
	diskPath := filepath.Join(objectPath, diskUUID)
	finalPartPath := filepath.Join(diskPath, "part.1")
	xlMetaPath := filepath.Join(objectPath, "xl.meta")

	// Check if object already exists - should not happen since handler checks this
	// but defensive check here to prevent data loss
	if _, err = os.Stat(xlMetaPath); err == nil {
		return nil, fmt.Errorf("object already exists at key %s", key)
	}

	// Create directory structure
	if err = os.MkdirAll(diskPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create object directory: %w", err)
	}

	// Assemble parts sequentially
	finalFile, err := os.Create(finalPartPath)
	if err != nil {
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to create final part file: %w", err)
	}

	multipartDir := filepath.Join(storageDir, ".multipart", uploadID)
	for i, part := range sortedParts {
		// Check context cancellation between parts
		select {
		case <-ctx.Done():
			finalFile.Close()
			os.RemoveAll(objectPath)
			return nil, ctx.Err()
		default:
		}

		// Open source part file
		partPath := filepath.Join(multipartDir, fmt.Sprintf("part.%d", part.PartNumber))
		partFile, err := os.Open(partPath)
		if err != nil {
			finalFile.Close()
			os.RemoveAll(objectPath)
			return nil, fmt.Errorf("failed to open part file: %w", err)
		}

		// Get part file size for logging and validation
		partInfo, _ := partFile.Stat()
		partFileSize := int64(0)
		if partInfo != nil {
			partFileSize = partInfo.Size()
		}

		// Get expected size from partMetadata (which has the Size field)
		expectedSize := partMetadata[i].Size
		log.Debugf("Assembling part %d from %s (file size: %d bytes, metadata size: %d bytes)",
			part.PartNumber, partPath, partFileSize, expectedSize)

		// Copy ONLY the expected number of bytes from part file to prevent extra data
		// Use LimitReader to ensure we don't copy more than what was originally uploaded
		limitedReader := io.LimitReader(partFile, expectedSize)
		copied, err := vfs.CopyWithContext(ctx, finalFile, limitedReader)
		partFile.Close()
		if err != nil {
			finalFile.Close()
			os.RemoveAll(objectPath)
			return nil, fmt.Errorf("failed to copy part data: %w", err)
		}

		// Validate we copied exactly the expected amount
		if copied != expectedSize {
			log.Errorf("Part %d: copied %d bytes but expected %d bytes", part.PartNumber, copied, expectedSize)
			finalFile.Close()
			os.RemoveAll(objectPath)
			return nil, fmt.Errorf("part %d size mismatch: copied %d bytes, expected %d bytes",
				part.PartNumber, copied, expectedSize)
		}
		log.Debugf("Successfully copied %d bytes for part %d", copied, part.PartNumber)
	}

	if err = finalFile.Sync(); err != nil {
		finalFile.Close()
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to sync final file: %w", err)
	}
	finalFile.Close()

	// Debug: Verify the assembled file size
	assembledInfo, _ := os.Stat(finalPartPath)
	if assembledInfo != nil {
		actualSize := assembledInfo.Size()
		log.Debugf("CompleteMultipart: Assembled file size on disk: %d bytes (expected: %d)", actualSize, totalSize)

		// Safety check: If file is larger than expected, truncate it
		if actualSize > totalSize {
			log.Debugf("CompleteMultipart: WARNING - File is larger than expected! Truncating to %d bytes", totalSize)
			if err = os.Truncate(finalPartPath, totalSize); err != nil {
				os.RemoveAll(objectPath)
				return nil, fmt.Errorf("failed to truncate oversized file: %w", err)
			}
		} else if actualSize < totalSize {
			log.Debugf("CompleteMultipart: ERROR - File is smaller than expected!")
			os.RemoveAll(objectPath)
			return nil, fmt.Errorf("assembled file size mismatch: got %d bytes, expected %d bytes", actualSize, totalSize)
		}
	}

	// Calculate multipart ETag: MD5(concat_part_etags)-{part_count}
	var combinedETags string
	for _, part := range partMetadata {
		combinedETags += part.ETag
	}
	hashBytes := md5.Sum([]byte(combinedETags))
	multipartETag := fmt.Sprintf("%s-%d", hex.EncodeToString(hashBytes[:]), len(partMetadata))

	// Detect content type using Magika
	contentType := upload.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	log.Debugf("CompleteMultipart: Initial contentType from upload metadata: %s", contentType)

	if MagikaScanner != nil {
		// Open final file for scanning
		scanFile, err := os.Open(finalPartPath)
		if err == nil {
			// Ensure file pointer is at the beginning
			scanFile.Seek(0, io.SeekStart)

			select {
			case ScanSem <- struct{}{}:
				result, scanErr := MagikaScanner.Scan(scanFile, int(totalSize))
				<-ScanSem
				if scanErr == nil && result.MimeType != "" {
					log.Debugf("CompleteMultipart: Magika detected type: %s (label: %s)",
						result.MimeType, result.Label)

					// Only override if content type was not provided or was generic
					if upload.ContentType == "" || upload.ContentType == "application/octet-stream" {
						contentType = result.MimeType
						log.Debugf("CompleteMultipart: Using Magika detected type: %s", contentType)
					} else {
						log.Debugf("CompleteMultipart: Keeping original content type %s, Magika suggested %s",
							contentType, result.MimeType)
					}
				} else if scanErr != nil {
					log.Debugf("CompleteMultipart: Magika scan error: %v", scanErr)
				}
			case <-ctx.Done():
				// Context canceled, use default content type
				log.Debugf("CompleteMultipart: Context canceled during Magika scan")
			}
			scanFile.Close()
		} else {
			log.Debugf("CompleteMultipart: Failed to open file for Magika scan: %v", err)
		}
	} else {
		log.Debugf("CompleteMultipart: Magika scanner not available")
	}

	log.Debugf("CompleteMultipart: Final contentType: %s", contentType)

	// Build object metadata with full Parts array
	now := time.Now().Unix()
	objectParts := make([]model.Part, len(partMetadata))
	for i, part := range partMetadata {
		objectParts[i] = model.Part{
			Number: part.PartNumber,
			Size:   part.Size,
			ETag:   part.ETag,
		}
	}

	meta := &model.ObjectMeta{
		Version:      xlMetaVersion,
		Size:         totalSize,
		ETag:         multipartETag,
		LastModified: now,
		ContentType:  contentType,
		DiskUUID:     diskUUID,
		Parts:        objectParts,
	}

	// Write xl.meta atomically
	if err = writeXLMetaAtomically(xlMetaPath, meta); err != nil {
		os.RemoveAll(objectPath)
		return nil, fmt.Errorf("failed to write xl.meta: %w", err)
	}

	// Cleanup multipart upload directory
	err = os.RemoveAll(multipartDir)
	if err != nil {
		return nil, fmt.Errorf("failed to remove multipart directory: %w", err)
	}

	// Remove from metastore (ignore error since object is already created)
	err = metaStore.RemoveMultipartUpload(uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to remove multipart upload from metastore: %w", err)
	}

	// Update bucket stats synchronously to ensure quota enforcement works correctly
	if err := metaStore.UpdateBucketStats(bucket, totalSize, 1); err != nil {
		log.Errorf("Failed to update bucket stats for %s: %v", bucket, err)
	}

	return meta, nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up
// It validates upload exists, removes directory, and removes from metastore
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket
// - key: the object key
// - uploadID: the upload ID
// Returns an error if the upload does not exist or cleanup fails
func AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Get upload metadata to verify it exists
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		return err
	}

	if upload.Bucket != bucket || upload.Key != key {
		return ErrNoSuchUpload
	}

	// Remove multipart directory
	multipartDir := filepath.Join(storageDir, ".multipart", uploadID)
	if err := os.RemoveAll(multipartDir); err != nil {
		// Log but don't fail if directory removal fails
		// Continue to remove from metastore
	}

	// Remove from metastore
	if err := metaStore.RemoveMultipartUpload(uploadID); err != nil {
		return err
	}

	return nil
}

// ListParts lists uploaded parts for a multipart upload
// It validates upload exists, extracts parts from metadata, applies pagination, and builds ListPartsResult
// Parameters:
// - ctx: context for cancellation and timeouts
// - bucket: the name of the bucket
// - key: the object key
// - uploadID: the upload ID
// - maxParts: maximum number of parts to return (default 1000)
// - partNumberMarker: part number to start listing from (for pagination)
// Returns the ListPartsResult or an error
func ListParts(ctx context.Context, bucket, key, uploadID string, maxParts, partNumberMarker int) (*model.ListPartsResult, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get upload metadata
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}

	if upload.Bucket != bucket || upload.Key != key {
		return nil, ErrNoSuchUpload
	}

	// Default maxParts
	if maxParts <= 0 {
		maxParts = 1000
	}

	// Extract and sort parts
	partNumbers := make([]int, 0, len(upload.Parts))
	for partNum := range upload.Parts {
		if partNum > partNumberMarker {
			partNumbers = append(partNumbers, partNum)
		}
	}
	sort.Ints(partNumbers)

	// Apply pagination
	isTruncated := false
	nextPartNumberMarker := 0
	if len(partNumbers) > maxParts {
		isTruncated = true
		nextPartNumberMarker = partNumbers[maxParts-1]
		partNumbers = partNumbers[:maxParts]
	}

	// Build part info list
	parts := make([]model.PartInfo, len(partNumbers))
	for i, partNum := range partNumbers {
		part := upload.Parts[partNum]
		parts[i] = model.PartInfo{
			PartNumber:   part.PartNumber,
			LastModified: time.Unix(part.LastModified, 0).Format(time.RFC3339),
			ETag:         part.ETag,
			Size:         part.Size,
		}
	}

	return &model.ListPartsResult{
		Bucket:               bucket,
		Key:                  key,
		UploadID:             uploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
	}, nil
}

// CleanupExpiredUploads removes multipart uploads older than TTL
// It gets expired uploads from metastore, removes directories, and removes from metastore
// Parameters:
// - ctx: context for cancellation and timeouts
// Returns an error if cleanup fails
func CleanupExpiredUploads(ctx context.Context) error {
	// Get uploads older than TTL (24 hours)
	expiredUploads := metaStore.GetExpiredUploads(configuration.MultipartUploadTTL)

	for _, upload := range expiredUploads {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Remove multipart directory
		multipartDir := filepath.Join(storageDir, ".multipart", upload.UploadID)
		os.RemoveAll(multipartDir) // Ignore errors

		// Remove from metastore
		metaStore.RemoveMultipartUpload(upload.UploadID) // Ignore errors
	}

	return nil
}
