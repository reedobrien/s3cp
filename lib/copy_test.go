package s3cp_test

import (
	"bytes"
	"errors"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/reedobrien/checkers"
	s3cp "github.com/reedobrien/s3cp/lib"
)

func TestNewDefaults(t *testing.T) {
	api := newDummyAPI("")
	got := s3cp.NewCopier(api)

	checkers.Equals(t, got.PartSize, int64(s3cp.DefaultCopyPartSize))
	checkers.Equals(t, got.Concurrency, s3cp.DefaultCopyConcurrency)
	checkers.Equals(t, got.Timeout, s3cp.DefaultCopyTimeout)
	checkers.Equals(t, got.LeavePartsOnError, false)
}

func TestNewWithOptions(t *testing.T) {
	api := newDummyAPI("")
	got := s3cp.NewCopier(api,
		func(c *s3cp.Copier) { c.PartSize = 100 },
		func(c *s3cp.Copier) { c.Concurrency = 8 },
		func(c *s3cp.Copier) { c.Timeout = time.Second },
		func(c *s3cp.Copier) { c.LeavePartsOnError = true },
		func(c *s3cp.Copier) { c.MustSvcForRegion = func(s *string) s3cp.API { return newDummyAPI(*s) } },
	)

	checkers.Equals(t, got.PartSize, int64(100))
	checkers.Equals(t, got.Concurrency, 8)
	checkers.Equals(t, got.Timeout, time.Second)
	checkers.Equals(t, got.LeavePartsOnError, true)
}

func TestWithCopierRequestOptions(t *testing.T) {
	api := newDummyAPI("a-region")
	tut := s3cp.NewCopier(api)

	s3cp.WithCopierRequestOptions(
		func(r *request.Request) { r.RetryCount = 99 },
		func(r *request.Request) { r.DisableFollowRedirects = true },
	)(tut)

	checkers.Equals(t, len(tut.RequestOptions), 2)
}

func TestCopyGetObjectHeadObjectError(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
	},
	)

	in := s3cp.CopyInput{
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
		},
		SourceRegion: aws.String("another-region"),
	}

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.MustSvcForRegion = func(s *string) s3cp.API {
				return newDummyAPI(*s, func(d *dummyAPI) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
					d.HooErr = errors.New("boom")
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.Equals(t, err.Error(), "error getting object info: boom")
}

func TestCopyDeleteCalled(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		Delete: true,
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
		},
	}
	api2 := newDummyAPI("api2-region", func(d *dummyAPI) {
		d.Doo = &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(false),
		}
		d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
	})

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.SrcS3 = api2
			c.MustSvcForRegion = func(_ *string) s3cp.API {
				return api2
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.OK(t, err)
	time.Sleep(time.Second)
	checkers.Equals(t, api2.DooCalls, int64(1))
}

func TestCopyDeleteError(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		Delete: true,
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
		},
	}
	api2 := newDummyAPI("api2-region", func(d *dummyAPI) {
		d.DooErr = errors.New("delete boom")
		d.Doo = &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(false),
		}
		d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
	})

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.SrcS3 = api2
			c.MustSvcForRegion = func(_ *string) s3cp.API {
				return api2
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.OK(t, err)
	checkers.Equals(t, api2.DooCalls, int64(1))
	checkers.Assert(t,
		strings.HasSuffix(
			buf.String(),
			"failed to delete \"bucket/key\": \"delete boom\"\n"),
		"missing expected log message")
}

func TestCopyDeleteMissingSourceError(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		Size:   68,
		Delete: true,
	}
	api2 := newDummyAPI("api2-region", func(d *dummyAPI) {
		d.DooErr = errors.New("delete boom")
		d.Doo = &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(false),
		}
		d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
	})

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.SrcS3 = api2
			c.MustSvcForRegion = func(_ *string) s3cp.API {
				return api2
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.OK(t, err)
	checkers.Equals(t, api2.DooCalls, int64(0))
}

func TestSinglePartCopy(t *testing.T) {
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
		},
	}

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.MustSvcForRegion = func(s *string) s3cp.API {
				return newDummyAPI(*s, func(d *dummyAPI) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.OK(t, err)
}

func TestSinglePartCopyAWSErr(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
		d.CooErr = awserr.New("boomCode", "boomMsg", errors.New("boom"))
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
			Key:        aws.String("akey"),
			Bucket:     aws.String("abucket"),
		},
	}

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.MustSvcForRegion = func(s *string) s3cp.API {
				return newDummyAPI(*s, func(d *dummyAPI) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.Equals(t, err.Error(), "boomCode: boomMsg\ncaused by: boom")
	checkers.Assert(t, strings.HasSuffix(buf.String(), "failed to copy \"bucket/key\" to \"abucket/akey\": boomCode: boomMsg\ncaused by: boom\n"), "missing expected log message")
}

func TestSinglePartCopyError(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()
	api := newDummyAPI("", func(d *dummyAPI) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
		d.CooErr = errors.New("boom")
	},
	)

	in := s3cp.CopyInput{SourceRegion: aws.String("another-region"),
		COI: s3.CopyObjectInput{
			CopySource: aws.String("bucket/key"),
			Key:        aws.String("akey"),
			Bucket:     aws.String("abucket"),
		},
	}

	tut := s3cp.NewCopier(api,
		func(c *s3cp.Copier) {
			c.MustSvcForRegion = func(s *string) s3cp.API {
				return newDummyAPI(*s, func(d *dummyAPI) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.Equals(t, err.Error(), "boom")
	checkers.Assert(t, strings.HasSuffix(buf.String(), "failed to copy \"bucket/key\" to \"abucket/akey\": boom\n"), "missing expected log message")
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
