package s3cp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/reedobrien/checkers"
	"github.com/reedobrien/s3cp/lib/dummy"
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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
	api := dummy.NewS3API("", func(d *dummy.S3API) {
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

func TestMultipartCopyInput(t *testing.T) {
	coi := newCOI()
	mci := multipartCopyInput{
		PartNumber:      99,
		CopySourceRange: aws.String("1000-1999"),
		UploadID:        aws.String("AN-ID"),
	}
	upci := mci.FromCopyPartInput(coi)

	checkers.Equals(t, *upci.PartNumber, int64(99))
	checkers.Equals(t, *upci.CopySourceRange, "1000-1999")
	checkers.Equals(t, *upci.UploadId, "AN-ID")

	checkers.Equals(t, *upci.Bucket, "bucket")
	checkers.Equals(t, *upci.CopySource, "anotherbucket/foo/bar")

	checkers.Assert(t,
		upci.CopySourceIfMatch == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.CopySourceIfMatch))
	checkers.Assert(t,
		upci.CopySourceIfModifiedSince == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.CopySourceIfModifiedSince))
	checkers.Equals(t, *upci.CopySourceIfNoneMatch, "lalkfkjdsa")
	checkers.Assert(t,
		upci.CopySourceIfUnmodifiedSince == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.CopySourceIfUnmodifiedSince))
	checkers.Equals(t, *upci.CopySourceSSECustomerAlgorithm, "AES256")
	checkers.Assert(t,
		upci.CopySourceSSECustomerKey == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.CopySourceSSECustomerKey))
	checkers.Assert(t,
		upci.CopySourceSSECustomerKeyMD5 == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.CopySourceSSECustomerKeyMD5))
	checkers.Assert(t,
		upci.RequestPayer == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.RequestPayer))

	checkers.Equals(t, *upci.Key, "foo/bar")

	checkers.Equals(t, *upci.SSECustomerAlgorithm, "AES256")
	checkers.Assert(t,
		upci.SSECustomerKey == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.SSECustomerKey))

	checkers.Assert(t,
		upci.SSECustomerKeyMD5 == nil,
		fmt.Sprintf("got value %#v, wanted nil", upci.SSECustomerKeyMD5))

}

// newCOI creates a new s3.CopyObjectInput partially populated, for testing
// multipartCopyInput.FromCopyPartInput.
func newCOI() *s3.CopyObjectInput {
	return &s3.CopyObjectInput{
		ACL:                aws.String("public"),
		Bucket:             aws.String("bucket"),
		CacheControl:       aws.String("no-cache"),
		ContentDisposition: aws.String(`Content-Disposition: attachment; filename="fname.ext`),
		ContentEncoding:    aws.String("gzip"),
		ContentLanguage:    aws.String("en-US"),
		ContentType:        aws.String("application/pdf"),
		CopySource:         aws.String("anotherbucket/foo/bar"),
		// CopySourceIfMatch *string
		// CopySourceIfModifiedSince *time.Time
		CopySourceIfNoneMatch: aws.String("lalkfkjdsa"),
		// CopySourceIfUnmodifiedSince *time.Time
		CopySourceSSECustomerAlgorithm: aws.String("AES256"),
		// CopySourceSSECustomerKey *string
		// CopySourceSSECustomerKeyMD5 *string
		Key:               aws.String("foo/bar"),
		Metadata:          map[string]*string{"spam": aws.String("eggs")},
		MetadataDirective: aws.String("REPLACE"),
		// RequestPayer *string
		SSECustomerAlgorithm: aws.String("AES256"),
		// SSECustomerKey *string
		// SSECustomerKeyMD5 *string
		ServerSideEncryption: aws.String("AES256"),
	}
}
