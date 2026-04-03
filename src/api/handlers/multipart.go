package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/danbordeanu/go-logger"
	"github.com/danbordeanu/go-stats/concurrency"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"s3-storage/api/middleware"
	"s3-storage/api/response"
	"s3-storage/configuration"
	"s3-storage/model"
	"s3-storage/services"
)

// InitiateMultipartUploadHandler handles POST /:bucket/*key?uploads
// It initiates a new multipart upload and returns the upload ID
func InitiateMultipartUploadHandler(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "InitiateMultipartUpload")

	var (
		e             error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	// URL-decode the key
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	ctx, span := tracer.Start(ctx, "InitiateMultipartUpload handler")
	defer span.End()

	log.Debugf("Initiating multipart upload: bucket=%s, key=%s, correlationId=%s", bucket, key, correlationId)

	if key == "" {
		e = fmt.Errorf("invalid object key: %s", key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	span.AddEvent("Check Permissions",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have write access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot upload to bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	// Get content type from headers
	contentType := c.GetHeader("Content-Type")

	// Get owner ID (use user ID or default)
	ownerID := configuration.OwnerId
	if user != nil {
		ownerID = user.ID
	}

	span.AddEvent("Initiate Multipart Upload",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.String("CorrelationId", correlationId)))

	// Call service to initiate multipart upload
	uploadID, err := services.InitiateMultipartUpload(ctx, bucket, key, contentType, ownerID)
	if err != nil {
		e = fmt.Errorf("error initiating multipart upload: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Build XML response
	result := model.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}

	c.XML(200, result)
}

// UploadPartHandler handles PUT /:bucket/*key?partNumber=N&uploadId=X
// It uploads a single part of a multipart upload
func UploadPartHandler(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "UploadPart")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	key := c.Param("key")

	// Remove leading slash from key (Gin includes it with wildcard)
	key = strings.TrimPrefix(key, "/")

	// URL-decode the key
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	uploadID := c.Query("uploadId")
	partNumberStr := c.Query("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > configuration.PartMaxCount {
		e = fmt.Errorf("invalid part number: %s", partNumberStr)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidPartNumber, key)
		return
	}

	ctx, span := tracer.Start(ctx, "UploadPart handler")
	defer span.End()

	log.Debugf("Uploading part: bucket=%s, key=%s, uploadId=%s, partNumber=%d, correlationId=%s", bucket, key, uploadID, partNumber, correlationId)

	// Get part size from Content-Length header
	// Note: This will be -1 if using chunked transfer encoding (common with Cloudflare)
	declaredSize := c.Request.ContentLength

	// If we have a declared size, validate it doesn't exceed max
	if declaredSize > 0 && declaredSize > configuration.PartMaxSize {
		e = fmt.Errorf("part too large: size %d exceeds max limit", declaredSize)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrEntityTooLarge, key)
		return
	}

	// Log if using chunked encoding
	if declaredSize <= 0 {
		log.Debugf("UploadPart: Using chunked transfer encoding (Content-Length=%d), will determine size from actual data", declaredSize)
	}

	span.AddEvent("Check Permissions",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have write access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot upload to bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	span.AddEvent("Upload Part",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.Int("PartNumber", partNumber),
			attribute.String("CorrelationId", correlationId)))

	// Get request body
	body := c.Request.Body
	defer body.Close()

	// Get X-Amz-Content-SHA256 header for ETag, or calculate it server-side
	contentSHA256 := c.GetHeader("X-Amz-Content-SHA256")

	// Stream to temporary file
	tempFile, err := os.CreateTemp(configuration.AppConfig().StorageDirectory, "s3-part-upload-*")
	if err != nil {
		e = fmt.Errorf("error creating temporary file: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}
	tempFileName := tempFile.Name()
	defer os.Remove(tempFileName)

	// Prepare destination writer (with or without hashing)
	var destination io.Writer = tempFile
	var hasher hash.Hash

	if contentSHA256 == "" || contentSHA256 == "UNSIGNED-PAYLOAD" {
		// Calculate SHA256 while streaming to temp file
		hasher = sha256.New()
		destination = io.MultiWriter(tempFile, hasher)
	}

	// Stream request body to temp file
	// Handle both cases: Content-Length present or chunked encoding
	var bytesWritten int64
	if declaredSize > 0 {
		// Content-Length is present - limit reader to prevent reading extra data
		bytesWritten, err = io.Copy(destination, io.LimitReader(body, declaredSize))
	} else {
		// Chunked encoding (no Content-Length) - read entire body until EOF
		// Trust that Cloudflare/proxy will close the stream after the chunk
		// We'll validate size afterwards
		bytesWritten, err = io.Copy(destination, body)
	}

	if err != nil {
		tempFile.Close()
		e = fmt.Errorf("error streaming request body: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Validate actual size received
	if bytesWritten <= 0 {
		tempFile.Close()
		e = fmt.Errorf("no data received: bytesWritten=%d", bytesWritten)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidPart, key)
		return
	}

	// Check if actual size exceeds maximum
	if bytesWritten > configuration.PartMaxSize {
		tempFile.Close()
		e = fmt.Errorf("part too large: received %d bytes, exceeds max limit of %d", bytesWritten, configuration.PartMaxSize)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrEntityTooLarge, key)
		return
	}

	// If Content-Length was declared, verify it matches actual bytes
	if declaredSize > 0 && bytesWritten != declaredSize {
		tempFile.Close()
		e = fmt.Errorf("size mismatch: Content-Length=%d but received %d bytes (possible truncation)", declaredSize, bytesWritten)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidPart, key)
		return
	}

	log.Debugf("UploadPart: Successfully received part data - declared_size=%d, actual_size=%d", declaredSize, bytesWritten)

	// Get calculated SHA256 if we computed it
	if hasher != nil {
		contentSHA256 = hex.EncodeToString(hasher.Sum(nil))
	}

	// Close and reopen temp file for reading
	tempFile.Close()
	tempFile, err = os.Open(tempFileName)
	if err != nil {
		e = fmt.Errorf("error reopening temporary file: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}
	defer tempFile.Close()

	// Use the actual bytes written, not Content-Length (in case of discrepancy)
	actualSize := bytesWritten

	// Call service to upload part
	etag, err := services.UploadPart(ctx, bucket, key, partNumber, uploadID, tempFile, actualSize)
	if err != nil {
		e = fmt.Errorf("error uploading part: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Return success with ETag header
	c.Header("ETag", fmt.Sprintf("\"%s\"", etag))
	c.Status(200)
}

// CompleteMultipartUploadHandler handles POST /:bucket/*key?uploadId=X
// It completes a multipart upload by assembling all parts into a single object
func CompleteMultipartUploadHandler(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "CompleteMultipartUpload")

	var (
		e             error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	// URL-decode the key
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	uploadID := c.Query("uploadId")

	ctx, span := tracer.Start(ctx, "CompleteMultipartUpload handler")
	defer span.End()

	log.Debugf("Completing multipart upload: bucket=%s, key=%s, uploadId=%s, correlationId=%s", bucket, key, uploadID, correlationId)

	// Get multipart upload metadata to calculate total size for quota check
	metaStore := services.GetMetaStore()
	upload, err := metaStore.GetMultipartUpload(uploadID)
	if err != nil {
		e = fmt.Errorf("error getting multipart upload: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Calculate total size from all parts (for telemetry)
	var totalSize int64
	for _, part := range upload.Parts {
		totalSize += part.Size
	}

	span.AddEvent("Check Permissions",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have write access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		if metaStore != nil && !services.CanAccessBucket(user, bucket, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot upload to bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	// Check if object already exists to prevent quota duplication
	// Without this check, uploading the same object multiple times via multipart
	// would increment bucket stats each time, causing quota to grow incorrectly
	checkObject := services.ObjectExists(bucket, key)
	if checkObject {
		e = fmt.Errorf("object already exists: bucket=%s, key=%s", bucket, key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrObjectAlreadyExists, key)
		return
	}

	// Parse XML request body
	var req model.CompleteMultipartUploadRequest
	if err := xml.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		e = fmt.Errorf("error parsing complete multipart upload request: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidPart, key)
		return
	}

	span.AddEvent("Complete Multipart Upload",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.Int("PartCount", len(req.Parts)),
			attribute.Int64("TotalSize", totalSize),
			attribute.String("CorrelationId", correlationId)))

	// Call service to complete multipart upload
	meta, err := services.CompleteMultipartUpload(ctx, bucket, key, uploadID, req.Parts)
	if err != nil {
		e = fmt.Errorf("error completing multipart upload: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Build XML response
	result := model.CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     meta.ETag,
	}

	c.XML(200, result)
}

// AbortMultipartUploadHandler handles DELETE /:bucket/*key?uploadId=X
// It aborts a multipart upload and cleans up all uploaded parts
func AbortMultipartUploadHandler(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "AbortMultipartUpload")

	var (
		e             error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	// URL-decode the key
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	uploadID := c.Query("uploadId")

	ctx, span := tracer.Start(ctx, "AbortMultipartUpload handler")
	defer span.End()

	log.Debugf("Aborting multipart upload: bucket=%s, key=%s, uploadId=%s, correlationId=%s", bucket, key, uploadID, correlationId)

	span.AddEvent("Check Permissions",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot abort upload in bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	span.AddEvent("Abort Multipart Upload",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.String("CorrelationId", correlationId)))

	// Call service to abort multipart upload
	if err := services.AbortMultipartUpload(ctx, bucket, key, uploadID); err != nil {
		e = fmt.Errorf("error aborting multipart upload: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Return 204 No Content
	c.Status(204)
}

// ListPartsHandler handles GET /:bucket/*key?uploadId=X
// It lists all uploaded parts for a multipart upload
func ListPartsHandler(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "ListParts")

	var (
		e             error
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	key := c.Param("key")
	key = strings.TrimPrefix(key, "/")

	// URL-decode the key
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	uploadID := c.Query("uploadId")

	// Get pagination parameters
	maxPartsStr := c.Query("max-parts")
	partNumberMarkerStr := c.Query("part-number-marker")

	maxParts := 1000
	if maxPartsStr != "" {
		if mp, err := strconv.Atoi(maxPartsStr); err == nil {
			maxParts = mp
		}
	}

	partNumberMarker := 0
	if partNumberMarkerStr != "" {
		if pnm, err := strconv.Atoi(partNumberMarkerStr); err == nil {
			partNumberMarker = pnm
		}
	}

	ctx, span := tracer.Start(c.Request.Context(), "ListParts handler")
	defer span.End()

	log.Debugf("Listing parts: bucket=%s, key=%s, uploadId=%s, correlationId=%s", bucket, key, uploadID, correlationId)

	span.AddEvent("Check Permissions",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, false, metaStore) {
			e = fmt.Errorf("access denied: user cannot list parts in bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	span.AddEvent("List Parts",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.String("CorrelationId", correlationId)))

	// Call service to list parts
	result, err := services.ListParts(ctx, bucket, key, uploadID, maxParts, partNumberMarker)
	if err != nil {
		e = fmt.Errorf("error listing parts: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	c.XML(200, result)
}
