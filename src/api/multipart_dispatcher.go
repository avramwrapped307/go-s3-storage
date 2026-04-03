package api

import (
	"github.com/gin-gonic/gin"

	"s3-storage/api/handlers"
)

// multipartDispatcher is a middleware that dispatches multipart upload requests to the appropriate handlers
// Gin router doesn't distinguish routes by query parameters, so we use this middleware-based dispatcher
func multipartDispatcher() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Request.URL.Query()

		// POST /:bucket/*key?uploads - Initiate
		if c.Request.Method == "POST" && query.Has("uploads") {
			handlers.InitiateMultipartUploadHandler(c)
			c.Abort()
			return
		}

		// POST /:bucket/*key?uploadId=X - Complete
		if c.Request.Method == "POST" && query.Has("uploadId") {
			handlers.CompleteMultipartUploadHandler(c)
			c.Abort()
			return
		}

		// PUT /:bucket/*key?partNumber=N&uploadId=X - Upload part
		if c.Request.Method == "PUT" && query.Has("partNumber") && query.Has("uploadId") {
			handlers.UploadPartHandler(c)
			c.Abort()
			return
		}

		// GET /:bucket/*key?uploadId=X - List parts
		if c.Request.Method == "GET" && query.Has("uploadId") {
			handlers.ListPartsHandler(c)
			c.Abort()
			return
		}

		// DELETE /:bucket/*key?uploadId=X - Abort
		if c.Request.Method == "DELETE" && query.Has("uploadId") {
			handlers.AbortMultipartUploadHandler(c)
			c.Abort()
			return
		}

		// Not multipart, continue to normal handlers
		c.Next()
	}
}
