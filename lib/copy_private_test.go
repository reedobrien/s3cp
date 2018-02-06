package s3cp

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/reedobrien/checkers"
)

func TestSetGetErr(t *testing.T) {
	tut := copier{}

	checkers.OK(t, tut.getErr())

	tut.setErr(errors.New("uh-oh"))

	checkers.Equals(t, tut.getErr().Error(), "uh-oh")
}

func TestGetContentLengthProvided(t *testing.T) {
	tut := copier{in: CopyInput{
		Size: 10,
	}}

	tut.getContentLength()

	checkers.OK(t, tut.getErr())
	checkers.Assert(t, tut.contentLength != nil, "got nil, expected value for contentLength")
	checkers.Equals(t, *tut.contentLength, int64(10))
}

func TestGetContentLengthLookup(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
	})
	tut := copier{
		in: CopyInput{
			COI: s3.CopyObjectInput{
				CopySource: aws.String("bucket/key")},
		},
		cfg: Copier{SrcS3: api},
	}

	tut.getContentLength()

	checkers.OK(t, tut.getErr())
	checkers.Assert(t, tut.contentLength != nil, "got nil, expected value for contentLength")
	checkers.Equals(t, *tut.contentLength, int64(6))
}

func TestGetContentLengthLookupError(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.HooErr = errors.New("boom")
	})
	tut := copier{
		in: CopyInput{
			COI: s3.CopyObjectInput{
				CopySource: aws.String("bucket/key")},
		},
		cfg: Copier{SrcS3: api},
	}

	tut.getContentLength()

	checkers.Assert(t, tut.contentLength == nil, "got nil, expected value for contentLength")
	checkers.Equals(t, tut.getErr().Error(), "error getting object info: boom")
}

func TestObjectInfoNoCopySource(t *testing.T) {
	// TODO(ro) 2018-02-01 Can this happen?
	api := newDummyAPI("", func(d *dummyAPI) {
		d.HooErr = errors.New("boom")
	})
	tut := copier{
		in:  CopyInput{},
		cfg: Copier{SrcS3: api},
	}

	tut.getContentLength()

	checkers.Assert(t, tut.contentLength == nil, "got nil, expected value for contentLength")
	checkers.Equals(t, tut.getErr().Error(), "got nil *string as CopySource")
}

func newDummyAPI(region string, opts ...func(*dummyAPI)) *dummyAPI {
	if region == "" {
		region = "default-region"
	}

	d := &dummyAPI{region: aws.String(region)}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

type dummyAPI struct {
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

func (d *dummyAPI) CopyObjectWithContext(_ aws.Context, in *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error) {
	if d.CooErr != nil {
		return nil, d.CooErr
	}
	return d.Coo, nil
}

func (d *dummyAPI) CreateMultipartUploadWithContext(_ aws.Context, in *s3.CreateMultipartUploadInput, ops ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	_ = atomic.AddInt64(&d.CmpCalls, 1)
	if d.CmpErr != nil {
		return nil, d.CmpErr
	}
	return d.Cmp, nil
}

func (d *dummyAPI) HeadObjectWithContext(ctx aws.Context, in *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	if d.HooErr != nil {
		return nil, d.HooErr
	}
	return d.Hoo, nil
}

func (d *dummyAPI) DeleteObjectWithContext(ctx aws.Context, in *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	_ = atomic.AddInt64(&d.DooCalls, 1)
	if d.DooErr != nil {
		return nil, d.DooErr
	}
	return d.Doo, nil
}

func (d *dummyAPI) Region() string {
	if d.region == nil {
		return ""
	}
	return *d.region
}
