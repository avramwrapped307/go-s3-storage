package handlers

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"s3-storage/model"
	"s3-storage/services"
)

func setupTestRouter(t *testing.T) (*gin.Engine, string) {
	// Create temp directory for test storage
	tmpDir := t.TempDir()

	// Initialize services
	metaStore, err := services.NewMetaStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create metastore: %v", err)
	}

	services.InitBucketService(metaStore, tmpDir)

	// Setup Gin in test mode
	gin.SetMode(gin.TestMode)
	router := gin.New()

	return router, tmpDir
}

func TestInitiateMultipartUploadHandler(t *testing.T) {
	router, _ := setupTestRouter(t)

	// Setup route
	router.POST("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		InitiateMultipartUploadHandler(c)
	})

	// Create test bucket
	bucket := "test-bucket"
	services.CreateBucket(bucket)

	// Test initiate multipart upload
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/test-object.bin?uploads", bucket), nil)
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Parse XML response
	var result model.InitiateMultipartUploadResult
	err := xml.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, bucket, result.Bucket)
	assert.Equal(t, "test-object.bin", result.Key)
	assert.NotEmpty(t, result.UploadID)
	assert.Contains(t, result.UploadID, "-") // Should have timestamp-uuid format
}

func TestUploadPartHandler(t *testing.T) {
	router, _ := setupTestRouter(t)

	// Setup route
	router.PUT("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		UploadPartHandler(c)
	})

	// Create test bucket and initiate upload
	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	uploadID, err := services.InitiateMultipartUpload(
		httptest.NewRequest("POST", "/", nil).Context(),
		bucket, key, "application/octet-stream", "test-user",
	)
	assert.NoError(t, err)

	// Test upload part
	partData := []byte("This is test part data")
	req := httptest.NewRequest(
		"PUT",
		fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(partData),
	)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(partData)))
	req.ContentLength = int64(len(partData))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify ETag header is set
	etag := w.Header().Get("ETag")
	assert.NotEmpty(t, etag)
	assert.True(t, strings.HasPrefix(etag, "\""))
	assert.True(t, strings.HasSuffix(etag, "\""))
}

func TestUploadPartHandlerInvalidPartNumber(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.PUT("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		UploadPartHandler(c)
	})

	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	uploadID, _ := services.InitiateMultipartUpload(
		httptest.NewRequest("POST", "/", nil).Context(),
		bucket, key, "", "test-user",
	)

	// Test with invalid part numbers
	testCases := []string{"0", "-1", "10001", "abc"}

	for _, partNum := range testCases {
		t.Run(fmt.Sprintf("PartNumber=%s", partNum), func(t *testing.T) {
			req := httptest.NewRequest(
				"PUT",
				fmt.Sprintf("/%s/%s?partNumber=%s&uploadId=%s", bucket, key, partNum, uploadID),
				bytes.NewReader([]byte("test")),
			)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestCompleteMultipartUploadHandler(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.POST("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		CompleteMultipartUploadHandler(c)
	})

	// Create test bucket and initiate upload
	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	ctx := httptest.NewRequest("POST", "/", nil).Context()
	uploadID, _ := services.InitiateMultipartUpload(ctx, bucket, key, "text/plain", "test-user")

	// Upload 3 parts
	parts := []struct {
		partNumber int
		data       []byte
	}{
		{1, bytes.Repeat([]byte("Part 1 "), 1024*1024)}, // ~7MB
		{2, bytes.Repeat([]byte("Part 2 "), 1024*1024)}, // ~7MB
		{3, []byte("Part 3 final")},                     // small last part
	}

	var completedParts []model.CompletedPartRequest
	for _, part := range parts {
		reader := bytes.NewReader(part.data)
		etag, _, err := services.UploadPart(ctx, bucket, key, part.partNumber, uploadID, reader, int64(len(part.data)))
		assert.NoError(t, err)

		completedParts = append(completedParts, model.CompletedPartRequest{
			PartNumber: part.partNumber,
			ETag:       etag,
		})
	}

	// Build XML request
	reqBody := model.CompleteMultipartUploadRequest{
		Parts: completedParts,
	}
	xmlData, _ := xml.Marshal(reqBody)

	// Test complete multipart upload
	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(xmlData),
	)
	req.Header.Set("Content-Type", "application/xml")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Parse XML response
	var result model.CompleteMultipartUploadResult
	err := xml.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, bucket, result.Bucket)
	assert.Equal(t, key, result.Key)
	assert.NotEmpty(t, result.ETag)
	assert.Contains(t, result.ETag, "-3") // Should have multipart suffix
}

func TestCompleteMultipartUploadHandlerInvalidParts(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.POST("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		CompleteMultipartUploadHandler(c)
	})

	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	ctx := httptest.NewRequest("POST", "/", nil).Context()
	uploadID, _ := services.InitiateMultipartUpload(ctx, bucket, key, "", "test-user")

	// Upload one valid part
	partData := bytes.Repeat([]byte("Part 1 "), 1024*1024)
	etag, _, _ := services.UploadPart(ctx, bucket, key, 1, uploadID, bytes.NewReader(partData), int64(len(partData)))

	// Test with wrong ETag
	reqBody := model.CompleteMultipartUploadRequest{
		Parts: []model.CompletedPartRequest{
			{PartNumber: 1, ETag: "wrong-etag"},
		},
	}
	xmlData, _ := xml.Marshal(reqBody)

	req := httptest.NewRequest(
		"POST",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(xmlData),
	)
	req.Header.Set("Content-Type", "application/xml")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	// Test with missing part
	reqBody = model.CompleteMultipartUploadRequest{
		Parts: []model.CompletedPartRequest{
			{PartNumber: 1, ETag: etag},
			{PartNumber: 2, ETag: "some-etag"}, // Part 2 doesn't exist
		},
	}
	xmlData, _ = xml.Marshal(reqBody)

	req = httptest.NewRequest(
		"POST",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(xmlData),
	)
	req.Header.Set("Content-Type", "application/xml")

	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAbortMultipartUploadHandler(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.DELETE("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		AbortMultipartUploadHandler(c)
	})

	// Create test bucket and initiate upload
	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	ctx := httptest.NewRequest("POST", "/", nil).Context()
	uploadID, _ := services.InitiateMultipartUpload(ctx, bucket, key, "", "test-user")

	// Upload one part
	partData := bytes.Repeat([]byte("test"), 1024*1024)
	_, _, _ = services.UploadPart(ctx, bucket, key, 1, uploadID, bytes.NewReader(partData), int64(len(partData)))

	// Test abort multipart upload
	req := httptest.NewRequest(
		"DELETE",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		nil,
	)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)

	// Verify upload is removed
	metaStore := services.GetMetaStore()
	_, err := metaStore.GetMultipartUpload(uploadID)
	assert.Error(t, err)
	assert.Equal(t, services.ErrNoSuchUpload, err)
}

func TestListPartsHandler(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.GET("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		ListPartsHandler(c)
	})

	// Create test bucket and initiate upload
	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	ctx := httptest.NewRequest("POST", "/", nil).Context()
	uploadID, _ := services.InitiateMultipartUpload(ctx, bucket, key, "", "test-user")

	// Upload 3 parts
	for i := 1; i <= 3; i++ {
		partData := []byte(fmt.Sprintf("Part %d data", i))
		_, _, _ = services.UploadPart(ctx, bucket, key, i, uploadID, bytes.NewReader(partData), int64(len(partData)))
	}

	// Test list parts
	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		nil,
	)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Parse XML response
	var result model.ListPartsResult
	err := xml.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, bucket, result.Bucket)
	assert.Equal(t, key, result.Key)
	assert.Equal(t, uploadID, result.UploadID)
	assert.Len(t, result.Parts, 3)
	assert.False(t, result.IsTruncated)

	// Verify parts are in order
	for i, part := range result.Parts {
		assert.Equal(t, i+1, part.PartNumber)
	}
}

func TestListPartsHandlerPagination(t *testing.T) {
	router, _ := setupTestRouter(t)

	router.GET("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		ListPartsHandler(c)
	})

	// Create test bucket and initiate upload
	bucket := "test-bucket"
	key := "test-object.bin"
	services.CreateBucket(bucket)

	ctx := httptest.NewRequest("POST", "/", nil).Context()
	uploadID, _ := services.InitiateMultipartUpload(ctx, bucket, key, "", "test-user")

	// Upload 5 parts
	for i := 1; i <= 5; i++ {
		partData := []byte(fmt.Sprintf("Part %d data", i))
		_, _, _ = services.UploadPart(ctx, bucket, key, i, uploadID, bytes.NewReader(partData), int64(len(partData)))
	}

	// Test with max-parts=2
	req := httptest.NewRequest(
		"GET",
		fmt.Sprintf("/%s/%s?uploadId=%s&max-parts=2", bucket, key, uploadID),
		nil,
	)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result model.ListPartsResult
	err := xml.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Len(t, result.Parts, 2)
	assert.True(t, result.IsTruncated)
	assert.Equal(t, 2, result.NextPartNumberMarker)

	// Test with part-number-marker
	req = httptest.NewRequest(
		"GET",
		fmt.Sprintf("/%s/%s?uploadId=%s&part-number-marker=2", bucket, key, uploadID),
		nil,
	)

	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	err = xml.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Len(t, result.Parts, 3) // Parts 3, 4, 5
	assert.Equal(t, 3, result.Parts[0].PartNumber)
}

func TestMultipartUploadEndToEnd(t *testing.T) {
	router, _ := setupTestRouter(t)

	// Setup routes
	router.POST("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		query := c.Request.URL.Query()
		if query.Has("uploads") {
			InitiateMultipartUploadHandler(c)
		} else if query.Has("uploadId") {
			CompleteMultipartUploadHandler(c)
		}
	})

	router.PUT("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		UploadPartHandler(c)
	})

	router.GET("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		ListPartsHandler(c)
	})

	router.DELETE("/:bucket/*key", func(c *gin.Context) {
		c.Set("correlation_id", "test-correlation-id")
		AbortMultipartUploadHandler(c)
	})

	// Create test bucket
	bucket := "test-bucket"
	key := "end-to-end.bin"
	services.CreateBucket(bucket)

	// Step 1: Initiate multipart upload
	req := httptest.NewRequest("POST", fmt.Sprintf("/%s/%s?uploads", bucket, key), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var initResult model.InitiateMultipartUploadResult
	xml.Unmarshal(w.Body.Bytes(), &initResult)
	uploadID := initResult.UploadID

	// Step 2: Upload 2 parts
	part1Data := bytes.Repeat([]byte("Part 1 "), 1024*1024) // ~7MB
	part2Data := []byte("Part 2 final")

	req = httptest.NewRequest(
		"PUT",
		fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(part1Data),
	)
	req.ContentLength = int64(len(part1Data))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	etag1 := strings.Trim(w.Header().Get("ETag"), "\"")

	req = httptest.NewRequest(
		"PUT",
		fmt.Sprintf("/%s/%s?partNumber=2&uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(part2Data),
	)
	req.ContentLength = int64(len(part2Data))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	etag2 := strings.Trim(w.Header().Get("ETag"), "\"")

	// Step 3: List parts
	req = httptest.NewRequest(
		"GET",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		nil,
	)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var listResult model.ListPartsResult
	xml.Unmarshal(w.Body.Bytes(), &listResult)
	assert.Len(t, listResult.Parts, 2)

	// Step 4: Complete multipart upload
	completeReq := model.CompleteMultipartUploadRequest{
		Parts: []model.CompletedPartRequest{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		},
	}
	xmlData, _ := xml.Marshal(completeReq)

	req = httptest.NewRequest(
		"POST",
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, uploadID),
		bytes.NewReader(xmlData),
	)
	req.Header.Set("Content-Type", "application/xml")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var completeResult model.CompleteMultipartUploadResult
	xml.Unmarshal(w.Body.Bytes(), &completeResult)
	assert.NotEmpty(t, completeResult.ETag)
	assert.Contains(t, completeResult.ETag, "-2")

	// Verify object exists
	assert.True(t, services.ObjectExists(bucket, key))
}
