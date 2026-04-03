package configuration

// CorrelationIdKey Server related constants
const (
	CorrelationIdKey = "correlation_id"
)

// OTName Telemetry related constants
const (
	OTName    = "s3-storage"
	OTVersion = "1.0"
	OTSchema  = "/v1"
)

const (
	OwnerId = "s3-storage"
	//ObjectMaxUploadSize = 5 * 1024 * 1024 * 1024 // 5GB, the maximum size for a single PUT object in S3
	ObjectMaxUploadSize = 100 * 1024 * 1024 // 100MB, the maximum size allowed by Cloudflare for uploads

	// Multipart upload limits (AWS S3 compatible)
	PartMinSize = 5 * 1024 * 1024 // 5MB (except last part)
	// PartMaxSize        = 5 * 1024 * 1024 * 1024  // 5GB
	PartMaxSize        = 100 * 1024 * 1024 // 100MB, the maximum size allowed by Cloudflare for uploads
	PartMaxCount       = 10000             // Maximum parts per upload
	MultipartUploadTTL = 24 * 60 * 60      // 24 hours in seconds
)
