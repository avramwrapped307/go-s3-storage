package handlers

import (
	"fmt"
	"net/http"
	"s3-storage/configuration"
	"time"

	"github.com/danbordeanu/go-logger"
	"github.com/danbordeanu/go-stats/concurrency"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"s3-storage/api/middleware"
	"s3-storage/api/response"
	"s3-storage/model"
	"s3-storage/services"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// ListBuckets godoc
// @Summary List all buckets
// @Description Returns a list of all buckets owned by the authenticated sender of the request
// @ID listbuckets
// @Tags buckets
// @Produce xml
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Content-SHA256 header string false "SHA256 hash of request payload or 'UNSIGNED-PAYLOAD' (required if S3_AUTH_ENABLED=true)"
// @Success 200 {object} model.ListAllMyBucketsResult "List of all buckets with owner information"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Failure 503 {object} model.S3Error "Service unavailable"
// @Router / [get]
func ListBuckets(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "ListBuckets")

	var (
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	// tracer
	ctx, span := tracer.Start(ctx, "ListBuckets handler")
	defer span.End()

	log.Debugf("Listing buckets for correlation ID: %s", correlationId)

	buckets := services.ListBuckets()

	// Filter buckets based on user permissions
	user := middleware.GetUserFromContext(c)
	if user != nil {
		buckets = services.FilterBucketsForUser(user, buckets)
	}

	span.AddEvent("List Buckets",
		oteltrace.WithAttributes(attribute.String("CorrelationId", correlationId)))

	ownerID := configuration.OwnerId
	if accessKeyID, exists := c.Get("accessKeyId"); exists {
		ownerID = accessKeyID.(string)
	} else if user != nil {
		ownerID = user.ID
	}

	result := model.ListAllMyBucketsResult{
		Owner: model.Owner{
			ID:          ownerID,
			DisplayName: ownerID,
		},
		Buckets: model.Buckets{
			Bucket: make([]model.Bucket, len(buckets)),
		},
	}

	for i, b := range buckets {
		result.Buckets.Bucket[i] = model.Bucket{
			Name:         b.Name,
			CreationDate: time.Unix(b.CreationDate, 0).UTC().Format(time.RFC3339),
		}
	}

	response.SuccessXmlResponse(c, result)
}

// CreateBucket godoc
// @Summary Create a new bucket
// @Description Creates a new S3 bucket with the specified name
// @ID createbucket
// @Tags buckets
// @Produce xml
// @Param bucket path string true "Name of the bucket to create"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Content-SHA256 header string false "SHA256 hash of request payload or 'UNSIGNED-PAYLOAD' (required if S3_AUTH_ENABLED=true)"
// @Success 200 "Bucket created successfully"
// @Header 200 {string} Location "The path to the created bucket"
// @Failure 400 {object} model.S3Error "Invalid bucket name"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 409 {object} model.S3Error "Bucket already exists"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket} [put]
func CreateBucket(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "CreateBucket")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucketName := c.Param("bucket")

	// tracer
	ctx, span := tracer.Start(ctx, "CreateBucket handler")
	defer span.End()

	log.Debugf("Creating bucket: %s", bucketName)

	span.AddEvent("Create Bucket",
		oteltrace.WithAttributes(
			attribute.String("CorrelationId", correlationId),
			attribute.String("BucketName", bucketName),
		))

	// Create bucket with owner if user is authenticated
	user := middleware.GetUserFromContext(c)
	if user != nil {
		err = services.CreateBucketWithOwner(bucketName, user.ID)
	} else {
		err = services.CreateBucket(bucketName)
	}

	if err != nil {
		e = fmt.Errorf("error creating bucket: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, bucketName)
		return
	}

	c.Header("Location", "/"+bucketName)
	c.Status(http.StatusOK)
}

// DeleteBucket godoc
// @Summary Delete a bucket
// @Description Deletes the specified bucket. The bucket must be empty before it can be deleted
// @ID deletebucket
// @Tags buckets
// @Produce xml
// @Param bucket path string true "Name of the bucket to delete"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Content-SHA256 header string false "SHA256 hash of request payload or 'UNSIGNED-PAYLOAD' (required if S3_AUTH_ENABLED=true)"
// @Success 204 "Bucket deleted successfully"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket not found"
// @Failure 409 {object} model.S3Error "Bucket not empty"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket} [delete]
func DeleteBucket(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()

	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "DeleteBucket")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucketName := c.Param("bucket")

	// tracer
	ctx, span := tracer.Start(ctx, "DeleteBucket handler")
	defer span.End()

	log.Debugf("Deleting bucket: %s", bucketName)

	span.AddEvent("Delete Bucket",
		oteltrace.WithAttributes(
			attribute.String("CorrelationId", correlationId),
			attribute.String("BucketName", bucketName),
		))

	// Check permissions - user must have write access
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucketName, true, metaStore) {
			e = fmt.Errorf("access denied: user cannot delete bucket %s", bucketName)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, bucketName)
			return
		}
	}

	if err = services.DeleteBucket(bucketName); err != nil {
		e = fmt.Errorf("error deleting bucket: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, bucketName)
		return
	}

	c.Status(http.StatusNoContent)
}

// HeadBucket godoc
// @Summary Check if bucket exists
// @Description Checks if a bucket exists and you have permission to access it
// @ID headbucket
// @Tags buckets
// @Param bucket path string true "Name of the bucket to check"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Content-SHA256 header string false "SHA256 hash of request payload or 'UNSIGNED-PAYLOAD' (required if S3_AUTH_ENABLED=true)"
// @Success 200 "Bucket exists"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /{bucket} [head]
func HeadBucket(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "HeadBucket")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	bucketName := c.Param("bucket")

	// tracer
	ctx, span := tracer.Start(ctx, "HeadBucket handler")
	defer span.End()

	log.Debugf("Checking bucket existence: %s", bucketName)

	span.AddEvent("Head Bucket",
		oteltrace.WithAttributes(
			attribute.String("CorrelationId", correlationId),
			attribute.String("BucketName", bucketName),
		))

	if err = services.HeadBucket(bucketName); err != nil {
		e = fmt.Errorf("error checking bucket existence: %s", err)
		span.SetStatus(codes.Error, e.Error())
		span.RecordError(err)
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, bucketName)
		return
	}

	// Check if user has access to this bucket
	user := middleware.GetUserFromContext(c)
	if user != nil {
		metaStore := services.GetMetaStore()
		if metaStore != nil && !services.CanAccessBucket(user, bucketName, false, metaStore) {
			e = fmt.Errorf("access denied: user cannot access bucket %s", bucketName)
			span.SetStatus(codes.Error, e.Error())
			span.RecordError(e)
			log.Errorf("%s", e)
			response.FailureXmlResponse(c, services.ErrAccessDenied, bucketName)
			return
		}
	}

	c.Status(http.StatusOK)
}
