package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	s3cp "github.com/reedobrien/s3cp/lib"
)

var (
	contentType = flag.String("contentType", "application/octet-stream", "The content type of object being copied.")
	dest        = flag.String("dest", "", "The destination bucket and key.")
	move        = flag.Bool("move", false, "Set to true to delete the file after copy.")
	region      = flag.String("region", os.Getenv("AWS_DEFAULT_REGION"), "The region of the destination bucket.")
	sha1        = flag.String("sha1", "", "The sha1 hash of the object.")
	size        = flag.Int64("size", -1, "The size of the object being copied.")
	source      = flag.String("source", "", "The source bucket and key. E.g. bucket/key/one")
	srcRegion   = flag.String("srcRegion", "", "The source bucket region, if different from the destination region.")
)

func main() {
	var (
		err      error
		metadata map[string]*string
	)

	flag.Parse()

	// srcElems := strings.SplitN(*source, "/", 2)
	destElems := strings.SplitN(*dest, "/", 2)

	if *sha1 != "" {
		metadata = make(map[string]*string)
		metadata["sha1"] = sha1
	}

	coi := s3.CopyObjectInput{
		Bucket:      &destElems[0],
		ContentType: contentType,
		CopySource:  source,
		Key:         &destElems[1],
	}

	if metadata != nil {
		coi.Metadata = metadata
		coi.MetadataDirective = aws.String("REPLACE")
	}

	in := s3cp.CopyInput{
		Delete:       *move,
		Size:         *size,
		Region:       region,
		SourceRegion: srcRegion,
		COI:          coi,
	}

	sess := session.Must(session.NewSession(&aws.Config{Region: in.Region}))

	copier := s3cp.NewCopier(s3.New(sess),
		func(c *s3cp.Copier) { c.PartSize = s3cp.MinCopyPartSize },
	)

	err = copier.Copy(in)
	if err != nil {
		log.Fatal(err)
	}
}
