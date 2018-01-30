package s3cp

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func removeMe() bool {
	return true
}

const (
	// DefaultCopyPartSize declares the default size of chunks to get copied.
	// It is currently set dumbly to 500MB. So that the maximum object size
	// (5TB) will work without exceeding the maximum part count (10,000).
	DefaultCopyPartSize = 1024 * 1024 * 500

	// DefaultCopyConcurrency sets the number of parts to request copying at
	// once.
	DefaultCopyConcurrency = 64

	// DefaultCopyTimeout is the max time we expect the copy operation to
	// take.  For a lambda < 5 minutes is best, but for a large copy it could
	// take hours.  DefaultCopyTimeout = 260 * time.Second
	DefaultCopyTimeout = 18 * time.Hour

	// MinCopyPartSize is the minimum allowed part size when doing multipart
	// copies.
	MinCopyPartSize = 1024 * 1024 * 25

	// MaxUploadParts is the maximum number of part allowed in a multipart
	// upload.
	// TODO(ro) 2018-01-30 Remove using s3manager constants.
	MaxUploadParts = 10000
)

// API contains the s3 API methods we use in this package for testing.
type API interface {
	CopyObject(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	// CopyObjectWithContext(aws.Context, *s3.CopyObjectInput, ...request.Option) (*s3.CopyObjectOutput, error)
	// DeleteObject(*s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	// DeleteObjectWithContext(aws.Context, *s3.DeleteObjectInput, ...request.Option) (*s3.DeleteObjectOutput, error)
	// HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	// HeadObjectWithContext(aws.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)
	// AbortMultipartUpload(*s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	// AbortMultipartUploadWithContext(aws.Context, *s3.AbortMultipartUploadInput, ...request.Option) (*s3.AbortMultipartUploadOutput, error)
	// CreateMultipartUpload(*s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	// CreateMultipartUploadWithContext(aws.Context, *s3.CreateMultipartUploadInput, ...request.Option) (*s3.CreateMultipartUploadOutput, error)
	// CompleteMultipartUpload(*s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	// CompleteMultipartUploadWithContext(aws.Context, *s3.CompleteMultipartUploadInput, ...request.Option) (*s3.CompleteMultipartUploadOutput, error)
}

// CopyInput is a parameter container for Copier.Copy.
type CopyInput struct {
	// The key for the copy destination.
	Key *string

	// The bucket for the copy destination.
	Bucket *string

	// CopySource is the name of the source bucket and key name of the source
	// object, separated by a slash (/). Must be URL-encoded.
	CopySource *string

	// If we should delete the source object on successful copy.
	Delete bool

	// The region of the destination bucket.
	Region *string

	// The region of the source bucket. If nil the SourceRegion is considered
	// to be the same as the destination bucket's region.
	SourceRegion *string

	// The size of the source object. If provided we use this to calculate the
	// parts copy source ranges. Otherwise we head the source object to get
	// the size.
	Size int64
}

// NewCopier creates a new Copier instance to copy opbjects concurrently from
// one s3 location to another.
func NewCopier(api API, opts ...func(*Copier)) *Copier {
	c := &Copier{
		PartSize:    DefaultCopyPartSize,
		Timeout:     DefaultCopyTimeout,
		Concurrency: DefaultCopyConcurrency,
		S3:          api,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// WithCopierRequestOptions appends to the Copier's API requst options.
func WithCopierRequestOptions(opts ...request.Option) func(*Copier) {
	return func(c *Copier) {
		c.RequestOptions = append(c.RequestOptions, opts...)
	}
}

// Copier holds the configuration details for copying from an s3 object to
// another s3 location.
type Copier struct {
	// The chunk size for parts.
	PartSize int64

	// How long to run before we quit waiting.
	Timeout time.Duration

	// How many parts to copy at once.
	Concurrency int

	// Setting This Value To True Will Cause The Sdk To Avoid Calling
	// Abortmultipartupload On A Failure, Leaving All Successfully Uploaded
	// Parts On S3 For Manual Recovery.
	//
	// Note That Storing Parts Of An Incomplete Multipart Upload Counts
	// Towards space usage on S3 and will add additional costs if not cleaned
	// up.
	LeavePartsOnError bool

	// The s3 client ot use when copying.
	S3 API

	// // SrcS3 is the source if set, it is a second region. Needed to delete.
	// SrcS3 s3iface.S3API

	// RequestOptions to be passed to the individual calls.
	RequestOptions []request.Option
}

// Copy copies the source to the destination.
func (c Copier) Copy(i CopyInput) error {
	return nil
}
