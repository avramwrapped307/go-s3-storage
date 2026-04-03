package services

import "errors"

// S3 error codes
var (
	ErrNoSuchBucket            = errors.New("NoSuchBucket")
	ErrBucketAlreadyOwnedByYou = errors.New("BucketAlreadyOwnedByYou")
	ErrBucketNotEmpty          = errors.New("BucketNotEmpty")
	ErrInvalidBucketName       = errors.New("InvalidBucketName")
	ErrAccessDenied            = errors.New("AccessDenied")
	ErrInvalidAccessKeyId      = errors.New("InvalidAccessKeyId")
	ErrSignatureDoesNotMatch   = errors.New("SignatureDoesNotMatch")
	ErrRequestTimeTooSkewed    = errors.New("RequestTimeTooSkewed")
	ErrMissingSecurityHeader   = errors.New("MissingSecurityHeader")
	ErrNoSuchKey               = errors.New("NoSuchKey")
	ErrInvalidObjectKey        = errors.New("InvalidObjectKey")
	ErrMissingContentSHA256    = errors.New("MissingContentSHA256")
	ErrObjectAlreadyExists     = errors.New("ObjectAlreadyExists")
	ErrEntityTooLarge          = errors.New("EntityTooLarge")
	ErrInternalError           = errors.New("InternalError")
	ErrShareLinkNotFound       = errors.New("ShareLinkNotFound")
	ErrShareLinkExpired        = errors.New("ShareLinkExpired")
	ErrQuotaExceeded           = errors.New("QuotaExceeded")
	ErrNoSuchUpload            = errors.New("NoSuchUpload")
	ErrInvalidPart             = errors.New("InvalidPart")
	ErrInvalidPartOrder        = errors.New("InvalidPartOrder")
	ErrEntityTooSmall          = errors.New("EntityTooSmall")
	ErrInvalidPartNumber       = errors.New("InvalidPartNumber")
)

// S3ErrorCode returns the S3 error code string for a given error
func S3ErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrNoSuchBucket):
		return "NoSuchBucket"
	case errors.Is(err, ErrBucketAlreadyOwnedByYou):
		return "BucketAlreadyOwnedByYou"
	case errors.Is(err, ErrBucketNotEmpty):
		return "BucketNotEmpty"
	case errors.Is(err, ErrInvalidBucketName):
		return "InvalidBucketName"
	case errors.Is(err, ErrAccessDenied):
		return "AccessDenied"
	case errors.Is(err, ErrInvalidAccessKeyId):
		return "InvalidAccessKeyId"
	case errors.Is(err, ErrSignatureDoesNotMatch):
		return "SignatureDoesNotMatch"
	case errors.Is(err, ErrRequestTimeTooSkewed):
		return "RequestTimeTooSkewed"
	case errors.Is(err, ErrMissingSecurityHeader):
		return "MissingSecurityHeader"
	case errors.Is(err, ErrNoSuchKey):
		return "NoSuchKey"
	case errors.Is(err, ErrInvalidObjectKey):
		return "InvalidObjectKey"
	case errors.Is(err, ErrMissingContentSHA256):
		return "MissingContentSHA256"
	case errors.Is(err, ErrObjectAlreadyExists):
		return "ObjectAlreadyExists"
	case errors.Is(err, ErrEntityTooLarge):
		return "EntityTooLarge"
	case errors.Is(err, ErrInternalError):
		return "InternalError"
	case errors.Is(err, ErrShareLinkNotFound):
		return "NoSuchKey"
	case errors.Is(err, ErrShareLinkExpired):
		return "AccessDenied"
	case errors.Is(err, ErrQuotaExceeded):
		return "QuotaExceeded"
	case errors.Is(err, ErrNoSuchUpload):
		return "NoSuchUpload"
	case errors.Is(err, ErrInvalidPart):
		return "InvalidPart"
	case errors.Is(err, ErrInvalidPartOrder):
		return "InvalidPartOrder"
	case errors.Is(err, ErrEntityTooSmall):
		return "EntityTooSmall"
	case errors.Is(err, ErrInvalidPartNumber):
		return "InvalidPartNumber"
	default:
		return "InternalError"
	}
}

// S3ErrorMessage returns the S3 error message for a given error
func S3ErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrNoSuchBucket):
		return "The specified bucket does not exist."
	case errors.Is(err, ErrBucketAlreadyOwnedByYou):
		return "Your previous request to create the named bucket succeeded and you already own it."
	case errors.Is(err, ErrBucketNotEmpty):
		return "The bucket you tried to delete is not empty."
	case errors.Is(err, ErrInvalidBucketName):
		return "The specified bucket is not valid."
	case errors.Is(err, ErrAccessDenied):
		return "Access Denied."
	case errors.Is(err, ErrInvalidAccessKeyId):
		return "The AWS Access Key Id you provided does not exist in our records."
	case errors.Is(err, ErrSignatureDoesNotMatch):
		return "The request signature we calculated does not match the signature you provided."
	case errors.Is(err, ErrRequestTimeTooSkewed):
		return "The difference between the request time and the server's time is too large."
	case errors.Is(err, ErrMissingSecurityHeader):
		return "Your request was missing a required header."
	case errors.Is(err, ErrNoSuchKey):
		return "The specified key does not exist."
	case errors.Is(err, ErrInvalidObjectKey):
		return "The specified object key is not valid."
	case errors.Is(err, ErrMissingContentSHA256):
		return "Missing required header: X-Amz-Content-SHA256."
	case errors.Is(err, ErrEntityTooLarge):
		return "Your proposed upload exceeds the maximum allowed object size."
	case errors.Is(err, ErrInternalError):
		return "We encountered an internal error. Please try again."
	case errors.Is(err, ErrShareLinkNotFound):
		return "The specified share link does not exist."
	case errors.Is(err, ErrShareLinkExpired):
		return "The share link has expired."
	case errors.Is(err, ErrQuotaExceeded):
		return "Your storage quota has been exceeded."
	case errors.Is(err, ErrNoSuchUpload):
		return "The specified multipart upload does not exist."
	case errors.Is(err, ErrInvalidPart):
		return "One or more of the specified parts could not be found."
	case errors.Is(err, ErrInvalidPartOrder):
		return "The list of parts was not in ascending order."
	case errors.Is(err, ErrEntityTooSmall):
		return "Your proposed upload is smaller than the minimum allowed object size."
	case errors.Is(err, ErrInvalidPartNumber):
		return "The part number must be an integer between 1 and 10000, inclusive."
	default:
		return "We encountered an internal error. Please try again."
	}
}

// S3ErrorHTTPStatus returns the HTTP status code for a given error
// This is used to set the appropriate status code in the S3 error response
// based on the type of error that occurred
// For example, if the error is ErrNoSuchBucket, it will return 404 Not Found
// Parameters:
// - err: the error to get the HTTP status code for
// Returns:
// - int: the HTTP status code corresponding to the error
func S3ErrorHTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrNoSuchBucket):
		return 404
	case errors.Is(err, ErrBucketAlreadyOwnedByYou):
		return 409
	case errors.Is(err, ErrBucketNotEmpty):
		return 409
	case errors.Is(err, ErrInvalidBucketName):
		return 400
	case errors.Is(err, ErrAccessDenied):
		return 403
	case errors.Is(err, ErrInvalidAccessKeyId):
		return 403
	case errors.Is(err, ErrSignatureDoesNotMatch):
		return 403
	case errors.Is(err, ErrRequestTimeTooSkewed):
		return 403
	case errors.Is(err, ErrMissingSecurityHeader):
		return 400
	case errors.Is(err, ErrNoSuchKey):
		return 404
	case errors.Is(err, ErrInvalidObjectKey):
		return 400
	case errors.Is(err, ErrMissingContentSHA256):
		return 400
	case errors.Is(err, ErrQuotaExceeded):
		return 403
	case errors.Is(err, ErrNoSuchUpload):
		return 404
	case errors.Is(err, ErrInvalidPart), errors.Is(err, ErrInvalidPartOrder),
		errors.Is(err, ErrEntityTooSmall), errors.Is(err, ErrInvalidPartNumber):
		return 400
	default:
		return 500
	}
}
