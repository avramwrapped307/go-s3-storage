package response

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"s3-storage/model"
	"s3-storage/services"
)

// SuccessXmlResponse sends an XML response with HTTP 200 status
func SuccessXmlResponse(c *gin.Context, data any) {
	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, data)
}

// SuccessResponse sends a success response with ETag header
func SuccessResponse(c *gin.Context, etag string) {
	c.Header("ETag", "\""+etag+"\"")
	c.Status(http.StatusOK)
}

// SuccessNoContentResponse sends a 204 No Content response with ETag header
func SuccessNoContentResponse(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// SuccessObjectResponse sends an object response with S3-compatible headers and streams the content
func SuccessObjectResponse(c *gin.Context, meta *model.ObjectMeta, reader io.Reader) {
	c.Header("Content-Length", strconv.FormatInt(meta.Size, 10))
	c.Header("ETag", "\""+meta.ETag+"\"")
	c.Header("Last-Modified", time.Unix(meta.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Content-Type", meta.ContentType)
	c.Header("Accept-Ranges", "bytes")

	fmt.Printf("DEBUG SuccessObjectResponse: Sending object with meta.Size=%d bytes\n", meta.Size)

	// Use io.LimitReader to ensure we only send exactly meta.Size bytes
	limitedReader := io.LimitReader(reader, meta.Size)
	c.Status(http.StatusOK)
	written, err := io.Copy(c.Writer, limitedReader)
	fmt.Printf("DEBUG SuccessObjectResponse: Actually sent %d bytes (error: %v)\n", written, err)
}

// SuccessHeadObjectResponse sends headers for a HEAD request without a body
func SuccessHeadObjectResponse(c *gin.Context, meta *model.ObjectMeta) {
	c.Header("Content-Length", strconv.FormatInt(meta.Size, 10))
	c.Header("ETag", "\""+meta.ETag+"\"")
	c.Header("Last-Modified", time.Unix(meta.LastModified, 0).UTC().Format(http.TimeFormat))
	c.Header("Content-Type", meta.ContentType)
	c.Status(http.StatusOK)
}

// SuccessListObjectsResponse sends an S3-compatible ListBucketResult XML response
func SuccessListObjectsResponse(c *gin.Context, result *services.ListObjectsResult) {
	// Convert services.ListObjectsResult to model.ListBucketResult
	contents := make([]model.ObjectContent, 0, len(result.Objects))
	for _, obj := range result.Objects {
		contents = append(contents, model.ObjectContent{
			Key:          obj.Key,
			LastModified: time.Unix(obj.LastModified, 0).UTC().Format(time.RFC3339),
			ETag:         "\"" + obj.ETag + "\"",
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	commonPrefixes := make([]model.CommonPrefix, 0, len(result.CommonPrefixes))
	for _, prefix := range result.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, model.CommonPrefix{
			Prefix: prefix,
		})
	}

	response := model.ListBucketResult{
		Name:           result.Bucket,
		Prefix:         result.Prefix,
		Delimiter:      result.Delimiter,
		MaxKeys:        result.MaxKeys,
		IsTruncated:    result.IsTruncated,
		KeyCount:       len(result.Objects),
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	c.Header("Content-Type", "application/xml")
	c.XML(http.StatusOK, response)
}

// SuccessShareLinkResponse sends a JSON response for share link creation
func SuccessShareLinkResponse(c *gin.Context, resp *model.CreateShareLinkResponse) {
	c.JSON(http.StatusOK, resp)
}

// FailureXmlResponse sends an S3-compatible XML error response
func FailureXmlResponse(c *gin.Context, err error, resource string) {
	code := services.S3ErrorCode(err)
	message := services.S3ErrorMessage(err)

	status := http.StatusInternalServerError
	switch code {
	case "NoSuchBucket":
		status = http.StatusNotFound
	case "BucketAlreadyOwnedByYou":
		status = http.StatusConflict
	case "BucketNotEmpty":
		status = http.StatusConflict
	case "InvalidBucketName":
		status = http.StatusBadRequest
	case "ObjectAlreadyExists":
		status = http.StatusConflict
	case "QuotaExceeded":
		status = http.StatusForbidden
	}

	s3Error := model.S3Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: c.GetHeader("X-Correlation-Id"),
	}

	c.Header("Content-Type", "application/xml")
	c.Writer.WriteHeader(status)
	xml.NewEncoder(c.Writer).Encode(s3Error)
}
