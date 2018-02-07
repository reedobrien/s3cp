package dummy

import (
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// NewS3API mocks the S3 API parts we use.
func NewS3API(region string, opts ...func(*S3API)) *S3API {
	if region == "" {
		region = "default-region"
	}

	d := &S3API{region: aws.String(region)}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

// S3API is the API struct.
type S3API struct {
	region *string

	Cmp      *s3.CreateMultipartUploadOutput
	CmpCalls int64
	CmpErr   error
	CooErr   error
	Coo      *s3.CopyObjectOutput
	Hoo      *s3.HeadObjectOutput
	HooErr   error
	Doo      *s3.DeleteObjectOutput
	DooCalls int64
	DooErr   error
}

// CopyObjectWithContext is a mock method.
func (d *S3API) CopyObjectWithContext(_ aws.Context, in *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error) {
	if d.CooErr != nil {
		return nil, d.CooErr
	}
	return d.Coo, nil
}

// CreateMultipartUploadWithContext is a mock method.
func (d *S3API) CreateMultipartUploadWithContext(_ aws.Context, in *s3.CreateMultipartUploadInput, ops ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	_ = atomic.AddInt64(&d.CmpCalls, 1)
	if d.CmpErr != nil {
		return nil, d.CmpErr
	}
	return d.Cmp, nil
}

// HeadObjectWithContext is a mock method.
func (d *S3API) HeadObjectWithContext(ctx aws.Context, in *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	if d.HooErr != nil {
		return nil, d.HooErr
	}
	return d.Hoo, nil
}

// DeleteObjectWithContext is a mock method.
func (d *S3API) DeleteObjectWithContext(ctx aws.Context, in *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	_ = atomic.AddInt64(&d.DooCalls, 1)
	if d.DooErr != nil {
		return nil, d.DooErr
	}
	return d.Doo, nil
}

// Region is a mock method.
func (d *S3API) Region() string {
	if d.region == nil {
		return ""
	}
	return *d.region
}
