package handlers

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
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

// awsChunkReader decodes AWS Signature V4 chunked encoding format:
// <hex-byte-count>;chunk-signature=<signature>\r\n
// <data-bytes>\r\n
// ... repeats ...
// 0;chunk-signature=<signature>\r\n
// <optional-trailers>\r\n
type awsChunkReader struct {
	reader    *bufio.Reader
	buffer    []byte
	bufferPos int
	totalRead int64
	finished  bool
}

func newAWSChunkReader(r io.Reader) *awsChunkReader {
	return &awsChunkReader{
		reader: bufio.NewReader(r),
	}
}

func (r *awsChunkReader) Read(p []byte) (n int, err error) {
	if r.finished {
		return 0, io.EOF
	}

	// If we have buffered data, return it first
	if r.bufferPos < len(r.buffer) {
		n = copy(p, r.buffer[r.bufferPos:])
		r.bufferPos += n
		r.totalRead += int64(n)
		return n, nil
	}

	// Read next chunk
	// Format: <hex-size>;chunk-signature=<sig>\r\n
	chunkHeader, err := r.reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk header: %w", err)
	}

	// Parse chunk size (everything before semicolon or \r\n)
	chunkHeader = strings.TrimSpace(chunkHeader)
	sizeStr := chunkHeader
	if idx := strings.Index(chunkHeader, ";"); idx != -1 {
		sizeStr = chunkHeader[:idx]
	}

	chunkSize, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse chunk size '%s': %w", sizeStr, err)
	}

	// If chunk size is 0, we're done (read trailers and finish)
	if chunkSize == 0 {
		r.finished = true
		// Read and discard trailers until empty line
		for {
			line, err := r.reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return 0, err
			}
			if strings.TrimSpace(line) == "" || err == io.EOF {
				break
			}
		}
		return 0, io.EOF
	}

	// Read chunk data
	r.buffer = make([]byte, chunkSize)
	_, err = io.ReadFull(r.reader, r.buffer)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk data: %w", err)
	}

	// Read and discard trailing \r\n
	trailer := make([]byte, 2)
	_, err = io.ReadFull(r.reader, trailer)
	if err != nil {
		return 0, fmt.Errorf("failed to read chunk trailer: %w", err)
	}
	// Note: We expect \r\n but don't strictly validate it to be lenient

	// Copy from buffer to output
	r.bufferPos = 0
	n = copy(p, r.buffer)
	r.bufferPos = n
	r.totalRead += int64(n)

	return n, nil
}

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
	log.Debugf("InitiateMultipartUpload: Content-Type header: %q", contentType)

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

	// Check for AWS-specific headers that might contain the true content length
	// x-amz-decoded-content-length: actual content size before chunked encoding
	// If present and valid, use it instead of Content-Length (which may be -1 in chunked mode)
	decodedContentLength := c.GetHeader("x-amz-decoded-content-length")
	if decodedContentLength != "" {
		log.Debugf("UploadPart: Found x-amz-decoded-content-length header: %s", decodedContentLength)
		// Try to parse and use it
		if parsedSize, err := strconv.ParseInt(decodedContentLength, 10, 64); err == nil && parsedSize > 0 {
			log.Debugf("UploadPart: Using decoded content length %d instead of Content-Length %d", parsedSize, declaredSize)
			declaredSize = parsedSize
		}
	}

	// Log other AWS headers for diagnosis
	contentSHA256 := c.GetHeader("x-amz-content-sha256")
	if contentSHA256 != "" {
		log.Debugf("UploadPart: Found x-amz-content-sha256: %s", contentSHA256)
	}

	// Log Transfer-Encoding header
	transferEncoding := c.Request.Header.Get("Transfer-Encoding")
	if transferEncoding != "" {
		log.Debugf("UploadPart: Transfer-Encoding: %s", transferEncoding)
	}

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

	// Get request body - stream directly to service (no temp file in handler)
	body := c.Request.Body
	defer body.Close()

	// Prepare reader based on Content-Length presence and AWS chunk encoding
	var partReader io.Reader
	var expectedSize int64 = declaredSize

	// Check if AWS is using chunk-signature encoding
	if contentSHA256 == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || contentSHA256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" {
		// AWS Signature V4 chunked encoding - decode it
		log.Debugf("UploadPart: Detected AWS chunk-signature encoding, using chunk decoder")
		partReader = newAWSChunkReader(body)
		// Use decoded content length as expected size
		expectedSize = declaredSize
	} else if declaredSize > 0 {
		// Content-Length present - limit reader to prevent reading extra data
		partReader = io.LimitReader(body, declaredSize)
		expectedSize = declaredSize
		log.Debugf("UploadPart: Content-Length mode, limiting to %d bytes", declaredSize)
	} else {
		// Chunked encoding (no Content-Length) - pass body directly
		// Service will read until EOF and validate size
		partReader = body
		expectedSize = 0
		log.Debugf("UploadPart: Chunked encoding mode, reading until EOF")
	}

	// Call service to upload part directly from HTTP body
	// Service handles all validation, I/O, hashing, quota checks, and storage
	etag, actualSize, err := services.UploadPart(ctx, bucket, key, partNumber, uploadID, partReader, expectedSize)
	if err != nil {
		e = fmt.Errorf("error uploading part: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	log.Debugf("UploadPart: Successfully uploaded part %d - etag=%s, size=%d bytes",
		partNumber, etag, actualSize)

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
