package s3cp_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/reedobrien/checkers"
	s3cp "github.com/reedobrien/s3cp/lib"
)

func TestNewDefaults(t *testing.T) {
	api := newDummyAPI(nil, nil)
	got := s3cp.NewCopier(api)

	checkers.Equals(t, got.PartSize, int64(s3cp.DefaultCopyPartSize))
	checkers.Equals(t, got.Concurrency, s3cp.DefaultCopyConcurrency)
	checkers.Equals(t, got.Timeout, s3cp.DefaultCopyTimeout)
}

func TestNewWithOptions(t *testing.T) {
	api := newDummyAPI(nil, nil)
	got := s3cp.NewCopier(api,
		func(c *s3cp.Copier) { c.PartSize = 100 },
		func(c *s3cp.Copier) { c.Concurrency = 8 },
		func(c *s3cp.Copier) { c.Timeout = time.Second },
	)

	checkers.Equals(t, got.PartSize, int64(100))
	checkers.Equals(t, got.Concurrency, 8)
	checkers.Equals(t, got.Timeout, time.Second)
}

func TestWithCopierRequestOptions(t *testing.T) {
	api := newDummyAPI(nil, nil)
	tut := s3cp.NewCopier(api)

	s3cp.WithCopierRequestOptions(
		func(r *request.Request) { r.RetryCount = 99 },
		func(r *request.Request) { r.DisableFollowRedirects = true },
	)(tut)

	checkers.Equals(t, len(tut.RequestOptions), 2)
}

func newDummyAPI(coo *s3.CopyObjectOutput, err error) *dummyAPI {
	return &dummyAPI{err: err, coo: coo}
}

type dummyAPI struct {
	err error
	coo *s3.CopyObjectOutput
}

func (d *dummyAPI) CopyObject(in *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.coo, nil
}
