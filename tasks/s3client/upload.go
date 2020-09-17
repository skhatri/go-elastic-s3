package s3client

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/skhatri/elastics3/model"
	"log"
	"os"
	"strings"
	"time"
)

func UploadToS3(fileName string, cfg model.ElasticS3Config) {
	if !cfg.Tasks.Upload {
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatal("can not open file for upload")
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.ApSoutheast1RegionID),
	}))
	svc := s3.New(sess)
	ctx := context.Background()
	key := strings.Replace(cfg.S3.Key, "$date", time.Now().Format("2006-01-02"), -1)
	md, err := svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3.Bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
	} else {
		if md.ETag != nil {
			fmt.Printf("file=s3://%s/%s, etag=%s\n", cfg.S3.Bucket, key, *md.ETag)
		} else {
			fmt.Printf("issue uploading. no etag set")
		}
	}
}

