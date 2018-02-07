package s3cp_test

import (
	"bytes"
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/reedobrien/checkers"
	s3cp "github.com/reedobrien/s3cp/lib"
	"github.com/reedobrien/s3cp/lib/dummy"
)

func TestNewDefaults(t *testing.T) {
	api := dummy.NewS3API("")
	got := s3cp.NewCopier(api)

	checkers.Equals(t, got.PartSize, int64(s3cp.DefaultCopyPartSize))
	checkers.Equals(t, got.Concurrency, s3cp.DefaultCopyConcurrency)
	checkers.Equals(t, got.Timeout, s3cp.DefaultCopyTimeout)
	checkers.Equals(t, got.LeavePartsOnError, false)
}

func TestNewWithOptions(t *testing.T) {
	api := dummy.NewS3API("")
	got := s3cp.NewCopier(api,
		func(c *s3cp.Copier) { c.PartSize = 100 },
		func(c *s3cp.Copier) { c.Concurrency = 8 },
		func(c *s3cp.Copier) { c.Timeout = time.Second },
		func(c *s3cp.Copier) { c.LeavePartsOnError = true },
		func(c *s3cp.Copier) { c.MustSvcForRegion = func(s *string) s3cp.API { return dummy.NewS3API(*s) } },
	)

	checkers.Equals(t, got.PartSize, int64(100))
	checkers.Equals(t, got.Concurrency, 8)
	checkers.Equals(t, got.Timeout, time.Second)
	checkers.Equals(t, got.LeavePartsOnError, true)
}

func TestWithCopierRequestOptions(t *testing.T) {
	api := dummy.NewS3API("a-region")
	tut := s3cp.NewCopier(api)

	s3cp.WithCopierRequestOptions(
		func(r *request.Request) { r.RetryCount = 99 },
		func(r *request.Request) { r.DisableFollowRedirects = true },
	)(tut)

	checkers.Equals(t, len(tut.RequestOptions), 2)
}

func TestCopyGetObjectHeadObjectError(t *testing.T) {
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
	api2 := dummy.NewS3API("api2-region", func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
	api2 := dummy.NewS3API("api2-region", func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
	api2 := dummy.NewS3API("api2-region", func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(6))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.Equals(t, err.Error(), "boom")
	checkers.Assert(t, strings.HasSuffix(buf.String(), "failed to copy \"bucket/key\" to \"abucket/akey\": boom\n"), "missing expected log message")
}

func TestStartMultipartError(t *testing.T) {
	api := dummy.NewS3API("", func(d *dummy.S3API) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
		d.CmpErr = awserr.New("mpboomCode", "mpMsqBoom", errors.New("boomboom"))
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(s3cp.DefaultCopyPartSize * 2))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })

	checkers.Equals(t, api.CmpCalls, int64(1))
	checkers.Equals(t, err.Error(), "mpboomCode: mpMsqBoom\ncaused by: boomboom")
}

func TestMultipartCopy(t *testing.T) {
	api := dummy.NewS3API("", func(d *dummy.S3API) {
		d.Coo = &s3.CopyObjectOutput{
			CopyObjectResult: &s3.CopyObjectResult{
				ETag:         aws.String("etag"),
				LastModified: aws.Time(time.Date(2005, 7, 1, 9, 30, 00, 00, time.UTC)),
			},
		}
		d.Cmp = &s3.CreateMultipartUploadOutput{
			UploadId: aws.String("an-id"),
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
				return dummy.NewS3API(*s, func(d *dummy.S3API) {
					d.Hoo = &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(s3cp.DefaultCopyPartSize * 2))}
				})
			}
		},
	)

	err := tut.Copy(in, func(c *s3cp.Copier) { c.Concurrency = 1 })
	checkers.OK(t, err)
}
