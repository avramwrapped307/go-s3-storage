package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"s3-storage/configuration"
	"strconv"
	"strings"
	"time"

	"github.com/danbordeanu/go-logger"
	"github.com/danbordeanu/go-stats/concurrency"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"s3-storage/api/middleware"
	"s3-storage/api/response"
	"s3-storage/services"
	"s3-storage/vfs"
)

// PutObject godoc
// @Summary Upload an object
// @Description Uploads an object to the specified bucket with the given key
// @ID putobject
// @Tags objects
// @Accept */*
// @Produce xml
// @Param bucket path string true "Name of the bucket"
// @Param key path string true "Object key (path)"
// @Param X-Amz-Content-SHA256 header string true "SHA256 hash of the request payload"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 200 "Object uploaded successfully"
// @Header 200 {string} ETag "MD5 hash of the uploaded object"
// @Failure 400 {object} model.S3Error "Invalid request"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket}/{key} [put]
func PutObject(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "PutObject")

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

	// URL-decode the key to handle percent-encoded names (e.g., spaces encoded as %20)
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	// tracer
	ctx, span := tracer.Start(ctx, "PutObject handler")
	defer span.End()

	log.Debugf("Putting object: bucket=%s, key=%s, correlationId=%s", bucket, key, correlationId)

	if key == "" {
		e = fmt.Errorf("invalid object key: %s", key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	span.AddEvent("Check Storage Quota",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("CorrelationId", correlationId)))

	// check quota before reading body to prevent large uploads from consuming resources if user is over quota
	if err = services.CheckStorageQuota(c.Request.ContentLength); err != nil {
		e = fmt.Errorf("storage quota exceeded: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, bucket)
		return
	}
	// check if 100mb content length is exceeded (S3 limits single PUT to 5GB)
	if c.Request.ContentLength > configuration.ObjectMaxUploadSize {
		e = fmt.Errorf("entity too large: content length %d exceeds 100Mb limit", c.Request.ContentLength)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrEntityTooLarge, key)
		return
	}

	span.AddEvent("Object Validation",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
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

	// check if object exists and return 409 if it does (S3 does not allow overwriting existing objects with PUT)
	checkObject := services.ObjectExists(bucket, key)
	if checkObject {
		e = fmt.Errorf("object already exists: bucket=%s, key=%s", bucket, key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrObjectAlreadyExists, key)
		return
	}

	span.AddEvent("Put Object",
		oteltrace.WithAttributes(attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.String("CorrelationId", correlationId)))

	// Get request body
	body := c.Request.Body
	defer body.Close()

	// Get content length
	size := c.Request.ContentLength

	// Get X-Amz-Content-SHA256 header for ETag, or calculate it server-side
	contentSHA256 := c.GetHeader("X-Amz-Content-SHA256")

	var reader vfs.MultipartFile
	var tempFile *os.File
	var tempFileName string

	// Stream to temporary file to avoid memory exhaustion
	// io.Copy uses 32KB chunks internally, so only 32KB is in memory at a time
	// Use configured storage directory for temp files
	tempFile, err = os.CreateTemp(configuration.AppConfig().StorageDirectory, "s3-upload-*")
	if err != nil {
		e = fmt.Errorf("error creating temporary file: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}
	tempFileName = tempFile.Name()
	defer os.Remove(tempFileName)

	// Prepare destination writer (with or without hashing)
	var destination io.Writer = tempFile
	var hasher hash.Hash

	if contentSHA256 == "" || contentSHA256 == "UNSIGNED-PAYLOAD" {
		// Calculate SHA256 while streaming to temp file
		hasher = sha256.New()
		destination = io.MultiWriter(tempFile, hasher)
	}

	// Stream request body to temp file (with optional SHA256 calculation)
	// Limit to ContentLength to prevent reading extra data beyond what client declared
	_, err = io.Copy(destination, io.LimitReader(body, size))
	if err != nil {
		tempFile.Close()
		e = fmt.Errorf("error streaming request body: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Get calculated SHA256 if we computed it
	if hasher != nil {
		contentSHA256 = hex.EncodeToString(hasher.Sum(nil))
	}

	// Close and reopen temp file for reading (os.File implements MultipartFile interface)
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
	reader = tempFile
	defer tempFile.Close()

	// Call service to put object
	meta, err := services.PutObject(c.Request.Context(), bucket, key, reader, size, contentSHA256)
	if err != nil {
		e = fmt.Errorf("error putting object: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Return success with ETag header
	response.SuccessResponse(c, meta.ETag)
}

// GetObject godoc
// @Summary Get an object
// @Description Retrieves an object from the specified bucket
// @ID getobject
// @Tags objects
// @Produce octet-stream
// @Param bucket path string true "Name of the bucket"
// @Param key path string true "Object key (path)"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 200 {file} binary "Object data"
// @Header 200 {string} Content-Length "Size of the object in bytes"
// @Header 200 {string} ETag "MD5 hash of the object (quoted)"
// @Header 200 {string} Last-Modified "Last modification time (RFC1123 format)"
// @Header 200 {string} Content-Type "MIME type of the object"
// @Failure 404 {object} model.S3Error "Bucket or object not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket}/{key} [get]
func GetObject(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "GetObject")

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

	// tracer
	ctx, span := tracer.Start(ctx, "GetObject handler")
	defer span.End()

	// URL-decode key to handle percent-encoded names (e.g., spaces encoded as %20)
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	log.Debugf("Getting object: bucket=%s, key=%s, correlationId=%s", bucket, key, correlationId)

	if key == "" {
		e = fmt.Errorf("invalid object key: %s", key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	span.AddEvent("Get Object", oteltrace.WithAttributes(
		attribute.String("BucketName", bucket),
		attribute.String("ObjectKey", key),
		attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have read access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, false, metaStore) {
			e = fmt.Errorf("access denied: user cannot read from bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	// Get object metadata and file reader
	meta, file, err := services.GetObject(bucket, key)
	if err != nil {
		e = fmt.Errorf("error getting object: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}
	defer file.Close()

	// Serve content using http.ServeContent which properly supports Range requests
	// Set S3-compatible headers (ETag, Content-Type, Last-Modified)
	c.Header("ETag", "\""+meta.ETag+"\"")
	c.Header("Content-Type", meta.ContentType)
	c.Header("Last-Modified", time.Unix(meta.LastModified, 0).UTC().Format(http.TimeFormat))

	// Use file base name for ServeContent's name parameter
	name := filepath.Base(key)
	// http.ServeContent will handle Range requests and set Content-Length/206 responses
	http.ServeContent(c.Writer, c.Request, name, time.Unix(meta.LastModified, 0), file)
}

// HeadObject godoc
// @Summary Retrieve object metadata
// @Description Retrieves metadata for an object without downloading the object data
// @ID headobject
// @Tags objects
// @Produce json
// @Param bucket path string true "Name of the bucket"
// @Param key path string true "Object key (path)"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 200 "Object metadata retrieved successfully (no body)"
// @Header 200 {string} Content-Length "Size of the object in bytes"
// @Header 200 {string} ETag "MD5 hash of the object (quoted)"
// @Header 200 {string} Last-Modified "Last modification time (RFC1123 format)"
// @Header 200 {string} Content-Type "MIME type of the object"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket or object not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket}/{key} [head]
func HeadObject(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "HeadObject")

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

	// tracer
	ctx, span := tracer.Start(ctx, "HeadObject handler")
	defer span.End()

	// URL-decode key to handle percent-encoded names (e.g., spaces encoded as %20)
	if decodedKey, err := url.PathUnescape(key); err == nil {
		key = decodedKey
	} else {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	log.Debugf("Head object: bucket=%s, key=%s, correlationId=%s", bucket, key, c.MustGet("correlation_id").(string))

	if key == "" {
		e = fmt.Errorf("invalid object key: %s", key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	span.AddEvent("Head Object", oteltrace.WithAttributes(
		attribute.String("BucketName", bucket),
		attribute.String("ObjectKey", key),
		attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have read access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, false, metaStore) {
			e = fmt.Errorf("access denied: user cannot access objects in bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	meta, err := services.HeadObject(bucket, key)
	if err != nil {
		e = fmt.Errorf("error retrieving object metadata: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	response.SuccessHeadObjectResponse(c, meta)

}

// DeleteObject godoc
// @Summary Delete an object
// @Description Deletes an object from the specified bucket
// @ID deleteobject
// @Tags objects
// @Param bucket path string true "Name of the bucket"
// @Param key path string true "Object key (path)"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 204 "Object deleted successfully"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket or object not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket}/{key} [delete]
func DeleteObject(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "DeleteObject")

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

	ctx, span := tracer.Start(ctx, "DeleteObject handler")
	defer span.End()

	log.Debugf("Deleting object: bucket=%s, key=%s, correlationId=%s", bucket, key, correlationId)

	key, err = url.PathUnescape(key)
	if err != nil {
		e = fmt.Errorf("invalid object key encoding: %w", err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	if key == "" {
		e = fmt.Errorf("invalid object key: %s", key)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(e)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	span.AddEvent("Delete Object",
		oteltrace.WithAttributes(
			attribute.String("BucketName", bucket),
			attribute.String("ObjectKey", key),
			attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have write access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot delete objects in bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, key)
			return
		}
	}

	// Call service to delete object
	err = services.DeleteObject(bucket, key)
	if err != nil {
		e = fmt.Errorf("error deleting object: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Return 204 No Content on success
	response.SuccessNoContentResponse(c)
}

// ListObjects godoc
// @Summary List objects in a bucket
// @Description Returns a list of objects in the specified bucket (S3 ListObjectsV2 compatible)
// @ID listobjects
// @Tags objects
// @Produce xml
// @Param bucket path string true "Bucket name"
// @Param list-type query int false "S3 list API version (must be 2 for ListObjectsV2)" Enums(2)
// @Param prefix query string false "Limits the response to keys that begin with the specified prefix"
// @Param delimiter query string false "Character used to group keys (usually /)"
// @Param max-keys query int false "Maximum number of keys returned" default(1000)
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 200 {object} model.ListBucketResult "List of objects"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket} [get]
func ListObjects(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "ListObjects")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucket := c.Param("bucket")
	prefix := c.Query("prefix")
	delimiter := c.Query("delimiter")
	maxKeys, _ := strconv.Atoi(c.DefaultQuery("max-keys", "1000"))

	// tracer
	ctx, span := tracer.Start(ctx, "ListObjects handler")
	defer span.End()

	log.Debugf("Listing objects: bucket=%s, prefix=%s, delimiter=%s, maxKeys=%d, correlationId=%s", bucket, prefix, delimiter, maxKeys, correlationId)

	span.AddEvent("List Objects", oteltrace.WithAttributes(
		attribute.String("BucketName", bucket),
		attribute.String("Prefix", prefix),
		attribute.String("Delimiter", delimiter),
		attribute.Int("MaxKeys", maxKeys),
		attribute.String("CorrelationId", correlationId)))

	// Check permissions - user must have read access to the bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucket, false, metaStore) {
			e = fmt.Errorf("access denied: user cannot list objects in bucket %s", bucket)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, bucket)
			return
		}
	}

	result, err := services.ListObjects(bucket, prefix, delimiter, maxKeys)
	if err != nil {
		e = fmt.Errorf("error listing objects: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, bucket)
		return
	}

	response.SuccessListObjectsResponse(c, result)
}
