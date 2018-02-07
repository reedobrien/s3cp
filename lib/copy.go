package s3cp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

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
	CopyObjectWithContext(aws.Context, *s3.CopyObjectInput, ...request.Option) (*s3.CopyObjectOutput, error)
	DeleteObjectWithContext(aws.Context, *s3.DeleteObjectInput, ...request.Option) (*s3.DeleteObjectOutput, error)
	HeadObjectWithContext(aws.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)
	// AbortMultipartUploadWithContext(aws.Context, *s3.AbortMultipartUploadInput, ...request.Option) (*s3.AbortMultipartUploadOutput, error)
	CreateMultipartUploadWithContext(aws.Context, *s3.CreateMultipartUploadInput, ...request.Option) (*s3.CreateMultipartUploadOutput, error)
	// CompleteMultipartUploadWithContext(aws.Context, *s3.CompleteMultipartUploadInput, ...request.Option) (*s3.CompleteMultipartUploadOutput, error)
}

// CopyInput is a parameter container for Copier.Copy.
type CopyInput struct {
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

	// COI is an embedded s3.CopyObjectInput struct.
	COI s3.CopyObjectInput
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

	// MustSvcForRegion returns a new API for the provided region.
	MustSvcForRegion func(*string) API

	// The s3 client ot use when copying.
	S3 API

	// SrcS3 is the source if set, it is a second region. Needed to delete.
	SrcS3 API

	// RequestOptions to be passed to the individual calls.
	RequestOptions []request.Option
}

// Copy copies the source to the destination.
func (c Copier) Copy(i CopyInput, opts ...func(*Copier)) error {
	return c.CopyWithContext(context.Background(), i, opts...)
}

// CopyWithContext performs Copy with the given context.Context.
func (c Copier) CopyWithContext(ctx aws.Context, input CopyInput, opts ...func(*Copier)) error {
	ctx, cancel := context.WithCancel(ctx)
	c.SrcS3 = c.S3
	if input.SourceRegion != nil && *input.SourceRegion != "" {
		c.SrcS3 = c.MustSvcForRegion(input.SourceRegion)
	}

	impl := copier{in: input, cfg: c, ctx: ctx, cancel: cancel}

	for _, opt := range opts {
		opt(&impl.cfg)
	}

	impl.cfg.RequestOptions = append(impl.cfg.RequestOptions, request.WithAppendUserAgent("s3manager"))

	if s, ok := c.S3.(maxRetrier); ok {
		impl.maxRetries = s.MaxRetries()
	}

	return impl.copy()
}

// copier is the struct for the internal implementation of copy.
type copier struct {
	sync.Mutex
	err error

	cfg    Copier
	cancel context.CancelFunc

	maxRetries int
	ctx        aws.Context

	contentLength     *int64
	MultipartUploadID *string
	in                CopyInput
	parts             []*s3.CompletedPart
	results           chan copyPartResult
	work              chan multipartCopyInput
	wg                sync.WaitGroup
}

func (c *copier) copy() error {
	c.getContentLength()
	if err := c.getErr(); err != nil {
		return err
	}

	// If there's a request to delete the source copy, do it on exit if there
	// was no error copying.
	if c.in.Delete {
		defer func() {
			if c.err != nil {
				return
			}
			c.deleteObject()
		}()
	}

	if *c.contentLength < c.cfg.PartSize {
		// It is smaller than part size so just copy.
		return c.singlePartCopyObject()
	}

	err := c.startMultipart()
	if err != nil {
		return err
	}

	c.primeMultipart()

	go c.produceParts()

	return nil
}

func (c *copier) deleteObject() {
	if c.in.COI.CopySource == nil {
		c.setErr(errors.New("delete requested but copy source is nil"))
		return
	}
	source := strings.SplitN(*c.in.COI.CopySource, "/", 2)
	_, err := c.cfg.SrcS3.DeleteObjectWithContext(c.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(source[0]),
		Key:    aws.String(source[1]),
	})
	if err != nil {
		log.Printf("failed to delete %q: %q", *c.in.COI.CopySource, err)
	}
}

func (c *copier) getContentLength() {
	if c.in.Size > 0 {
		c.contentLength = aws.Int64(c.in.Size)
		return
	}

	info, err := c.objectInfo(c.in.COI.CopySource)
	if err != nil {
		c.setErr(err)
		return
	}
	c.contentLength = info.ContentLength
}

func (c *copier) objectInfo(cs *string) (*s3.HeadObjectOutput, error) {
	if cs == nil {
		return nil, errors.New("got nil *string as CopySource")
	}
	source := strings.SplitN(*cs, "/", 2)
	info, err := c.cfg.SrcS3.HeadObjectWithContext(c.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(source[0]),
		Key:    aws.String(source[1]),
	})
	if err != nil {
		return nil, fmt.Errorf("error getting object info: %s", err)
	}
	return info, nil
}

func (c *copier) primeMultipart() {
	partCount := int(math.Ceil(float64(*c.contentLength) / float64(c.cfg.PartSize)))
	c.parts = make([]*s3.CompletedPart, partCount)
	c.results = make(chan copyPartResult, c.cfg.Concurrency)
	c.work = make(chan multipartCopyInput, c.cfg.Concurrency)
}

func (c *copier) produceParts() {
	var partNum int64
	size := *c.contentLength

	for size >= 0 {
		offset := c.cfg.PartSize * partNum
		endByte := offset + c.cfg.PartSize - 1
		if endByte >= *c.contentLength {
			endByte = *c.contentLength - 1
		}
		mci := multipartCopyInput{
			PartNumber:      partNum + 1,
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, endByte)),
			UploadID:        c.MultipartUploadID,
		}
		c.wg.Add(1)
		c.work <- mci
		partNum++
		size -= c.cfg.PartSize
		if size <= 0 {
			break
		}
	}
	close(c.work)
}

func (c *copier) singlePartCopyObject() error {
	_, err := c.cfg.S3.CopyObjectWithContext(c.ctx, &c.in.COI)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			log.Printf("failed to copy %q to %q: %s", *c.in.COI.CopySource, *c.in.COI.Bucket+"/"+*c.in.COI.Key, aerr)
		} else {
			log.Printf("failed to copy %q to %q: %s", *c.in.COI.CopySource, *c.in.COI.Bucket+"/"+*c.in.COI.Key, err)
		}
		return err
	}

	return nil
}

func (c *copier) startMultipart() error {
	cmui := &s3.CreateMultipartUploadInput{
		Bucket: c.in.COI.Bucket,
		Key:    c.in.COI.Key,
	}
	resp, err := c.cfg.S3.CreateMultipartUploadWithContext(c.ctx, cmui)
	if err != nil {
		// TODO(ro) 2018-02-06 parse for awserr?
		c.setErr(err)
		return err
	}

	c.MultipartUploadID = resp.UploadId
	return nil
}

func (c *copier) getErr() error {
	c.Lock()
	defer c.Unlock()

	return c.err
}

func (c *copier) setErr(e error) {
	c.Lock()
	defer c.Unlock()

	c.err = e
}

// maxRetrier provices an interface to MaRetries. This was copied from aws sdk.
// TODO(ro) 2018-01-30 Remove if part of the s3manager package.
type maxRetrier interface {
	MaxRetries() int
}
