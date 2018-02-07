package s3cp

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type copyPartResult struct {
	Part int64
	*s3.CopyPartResult
}

type multipartCopyInput struct {
	PartNumber      int64
	CopySourceRange *string
	UploadID        *string
}

func (m multipartCopyInput) FromCopyPartInput(c *s3.CopyObjectInput) *s3.UploadPartCopyInput {
	return &s3.UploadPartCopyInput{
		PartNumber:      aws.Int64(m.PartNumber),
		CopySourceRange: m.CopySourceRange,
		UploadId:        m.UploadID,

		Bucket: c.Bucket,

		CopySource:                     c.CopySource,
		CopySourceIfMatch:              c.CopySourceIfMatch,
		CopySourceIfModifiedSince:      c.CopySourceIfModifiedSince,
		CopySourceIfNoneMatch:          c.CopySourceIfNoneMatch,
		CopySourceIfUnmodifiedSince:    c.CopySourceIfUnmodifiedSince,
		CopySourceSSECustomerAlgorithm: c.CopySourceSSECustomerAlgorithm,
		CopySourceSSECustomerKey:       c.CopySourceSSECustomerKey,
		CopySourceSSECustomerKeyMD5:    c.CopySourceSSECustomerKeyMD5,

		Key: c.Key,

		RequestPayer: c.RequestPayer,

		SSECustomerAlgorithm: c.SSECustomerAlgorithm,
		SSECustomerKey:       c.SSECustomerKey,
		SSECustomerKeyMD5:    c.SSECustomerKeyMD5,
	}
}
