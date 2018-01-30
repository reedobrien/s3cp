package s3cp

import (
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func removeMe() bool {
	return true
}

// DefaultCopyPartSize declares the default size of chunks to get copied. It
// is currently set dumbly to 500MB. So that the maximum object size (5TB)
// will work without exceeding the maximum part count (10,000).
const DefaultCopyPartSize = 1024 * 1024 * 500

// DefaultCopyConcurrency sets the number of parts to request copying at once.
const DefaultCopyConcurrency = 64

// DefaultCopyTimeout is the max time we expect the copy operation to take.
// For a lambda < 5 minutes is best, but for a large copy it could take hours.
// const DefaultCopyTimeout = 260 * time.Second
const DefaultCopyTimeout = 18 * time.Hour

// API contains the s3 API methods we use in this package for testing.
type API interface {
	CopyObject(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
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

	// The s3 client ot use when copying.
	S3 API

	// // SrcS3 is the source if set, it is a second region. Needed to delete.
	// SrcS3 s3iface.S3API

	// // RequestOptions to be passed to the individual calls.
	// RequestOptions []request.Option
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
