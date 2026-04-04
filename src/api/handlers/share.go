package handlers

import (
	"fmt"
	"net/http"
	"path/filepath"
	"s3-storage/configuration"
	"s3-storage/model"
	"strconv"
	"strings"
	"time"

	"github.com/danbordeanu/go-logger"
	"github.com/danbordeanu/go-stats/concurrency"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"s3-storage/api/response"
	"s3-storage/services"
)

// CreateShareLink godoc
// @Summary Create a public share link for an object
// @Description Generates a unique public link that allows downloading an object without authentication
// @ID createsharelink
// @Tags sharing
// @Accept json
// @Produce json
// @Param bucket path string true "Name of the bucket"
// @Param key path string true "Object key (path)"
// @Param expires_in query int false "Expiration time in seconds (0 for no expiration)" default(0)
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 200 {object} model.CreateShareLinkResponse "Share link created successfully"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Bucket or object not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /share/create/{bucket}/{key} [post]
func CreateShareLink(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "CreateShareLink")

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
	ctx, span := tracer.Start(ctx, "CreateShareLink handler")
	defer span.End()

	log.Debugf("Create shared link for bucket: %s, Key: %s", bucket, key)

	if key == "" {
		e = fmt.Errorf("object key cannot be empty")
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Error(e)
		response.FailureXmlResponse(c, services.ErrInvalidObjectKey, key)
		return
	}

	// Get expires_in parameter (default 0 = no expiration)
	expiresInStr := c.DefaultQuery("expires_in", "0")
	expiresIn, err := strconv.ParseInt(expiresInStr, 10, 64)
	if err != nil || expiresIn < 0 {
		expiresIn = 0
	}

	span.AddEvent("Create shared link", oteltrace.WithAttributes(
		attribute.String("BucketName", bucket),
		attribute.String("ObjectKey", key),
		attribute.Int64("ExpiresIn", expiresIn),
		attribute.String("CorrelationId", correlationId)))

	// Create share link
	token, err := services.CreateShareLink(bucket, key, expiresIn)
	if err != nil {
		e = fmt.Errorf("error creating share link: %s", err)
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}

	// Build share URL using configured base URL
	baseURL := configuration.AppConfig().RequestBaseUrl
	shareURL := fmt.Sprintf("%s/share/%s", baseURL, token)

	// Build response
	resp := model.CreateShareLinkResponse{
		Token:     token,
		ShareURL:  shareURL,
		Bucket:    bucket,
		Key:       key,
		ExpiresIn: expiresIn,
	}

	if expiresIn > 0 {
		// Calculate actual expiration timestamp
		resp.ExpiresAt = c.GetInt64("current_time") + expiresIn
	}

	response.SuccessShareLinkResponse(c, &resp)
}

// GetSharedObject godoc
// @Summary Download an object via public share link
// @Description Downloads an object using a public share token without authentication
// @ID getsharedobject
// @Tags sharing
// @Produce octet-stream
// @Param token path string true "Share link token"
// @Success 200 {file} binary "Object data"
// @Header 200 {string} Content-Length "Size of the object in bytes"
// @Header 200 {string} ETag "MD5 hash of the object (quoted)"
// @Header 200 {string} Last-Modified "Last modification time (RFC1123 format)"
// @Header 200 {string} Content-Type "MIME type of the object"
// @Failure 403 {object} model.S3Error "Share link expired"
// @Failure 404 {object} model.S3Error "Share link not found or object deleted"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /share/{token} [get]
func GetSharedObject(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "GetSharedObject")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)
	token := c.Param("token")

	// tracer
	ctx, span := tracer.Start(ctx, "GetSharedObject handler")
	defer span.End()

	log.Debugf("Get shared object for token: %s", token)

	if token == "" {
		e = fmt.Errorf("share link token cannot be empty")
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Error(e)
		response.FailureXmlResponse(c, services.ErrShareLinkNotFound, token)
		return
	}

	span.AddEvent("Get shared object", oteltrace.WithAttributes(
		attribute.String("ShareToken", token),
		attribute.String("CorrelationId", correlationId)))

	// Get bucket and key from share link
	bucket, key, err := services.GetShareLink(token)
	if err != nil {
		e = fmt.Errorf("error retrieving share link: %s", err)
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, token)
		return
	}

	// Get object metadata and file reader
	meta, file, err := services.GetObject(bucket, key)
	if err != nil {
		e = fmt.Errorf("error retrieving shared object: %s", err)
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, key)
		return
	}
	defer file.Close()

	// Serve content using http.ServeContent which properly supports Range requests
	c.Header("ETag", "\""+meta.ETag+"\"")
	c.Header("Content-Type", meta.ContentType)
	c.Header("Last-Modified", time.Unix(meta.LastModified, 0).UTC().Format(http.TimeFormat))

	// Extract filename from key and set Content-Disposition to preserve original filename
	name := filepath.Base(key)
	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", name))

	http.ServeContent(c.Writer, c.Request, name, time.Unix(meta.LastModified, 0), file)
}

// DeleteShareLink godoc
// @Summary Delete a share link
// @Description Revokes a public share link by deleting it
// @ID deletesharelink
// @Tags sharing
// @Param token path string true "Share link token"
// @Param Authorization header string false "AWS4-HMAC-SHA256 authorization header (required if S3_AUTH_ENABLED=true)"
// @Param X-Amz-Date header string false "Request timestamp in ISO 8601 format (required if S3_AUTH_ENABLED=true)"
// @Success 204 "Share link deleted successfully"
// @Failure 403 {object} model.S3Error "Access denied or signature mismatch"
// @Failure 404 {object} model.S3Error "Share link not found"
// @Failure 500 {object} model.S3Error "Internal server error"
// @Router /share/{token} [delete]
func DeleteShareLink(c *gin.Context) {
	concurrency.GlobalWaitGroup.Add(1)
	defer concurrency.GlobalWaitGroup.Done()
	log := logger.SugaredLogger().WithContextCorrelationId(c).With("package", "handlers", "action", "DeleteShareLink")

	var (
		e             error
		err           error
		ctx           = c.Request.Context()
		correlationId = c.MustGet("correlation_id").(string)
	)

	token := c.Param("token")

	// tracer
	ctx, span := tracer.Start(ctx, "DeleteShareLink handler")
	defer span.End()

	log.Debugf("Delete share link for token: %s", token)

	if token == "" {
		e = fmt.Errorf("share link token cannot be empty")
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Error(e)
		response.FailureXmlResponse(c, services.ErrShareLinkNotFound, token)
		return
	}

	span.AddEvent("Delete share link", oteltrace.WithAttributes(
		attribute.String("ShareToken", token),
		attribute.String("CorrelationId", correlationId)))

	// Delete share link
	err = services.DeleteShareLink(token)
	if err != nil {
		e = fmt.Errorf("error deleting share link: %s", err)
		span.RecordError(e)
		span.SetStatus(codes.Error, e.Error())
		log.Errorf("%s", e)
		response.FailureXmlResponse(c, err, token)
		return
	}

	// Return 204 No Content on success
	response.SuccessNoContentResponse(c)
}
