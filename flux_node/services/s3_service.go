package services

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	S3_BUCKET_NAME string
	s3Client *s3.Client
)

func InitS3Client(){
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil{
		log.Fatalf("unable to load AWS SDK config, %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)

	S3_BUCKET_NAME = os.Getenv("S3_BUCKET_NAME")
	if S3_BUCKET_NAME == "" {
		log.Fatal("Missing required environment variable: S3_BUCKET_NAME")
	}
}

func UploadFileToS3(topic, filePath string) error {
	if s3Client == nil{
		InitS3Client()
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for S3 upload: %v", err)
	}
	defer file.Close()

	key:= fmt.Sprintf("%s/%s/%s",
		topic,
		time.Now().Format("2006-01-02"),
		filepath.Base(filePath),
	)

	_,err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(S3_BUCKET_NAME),
		Key: aws.String(key),
		Body: file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %v", err)
	}

	log.Printf("S3 upload complete: s3://%s/%s", S3_BUCKET_NAME, key)
	return nil
}
