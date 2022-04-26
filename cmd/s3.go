package cmd

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

func buildS3Cmd(parentCmd *cobra.Command) {
	var ()
	var cmd = &cobra.Command{
		Use:   "s3",
		Short: "s3API test",
	}

	parentCmd.AddCommand(cmd)

	buildCOSCheckCmd(cmd)
	buildCOSMultipartUploadCheckCmd(cmd)
	buildCOSRateCmd(cmd)
	buildCOSClearCmd(cmd)
	buildCOSCheck2Cmd(cmd)

	buildVersionsCmd(cmd)
}

type S3API interface {
	//Bucket OP
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput) (*s3.CreateBucketOutput, error)
	DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)

	//Object OP
	PutObject(ctx context.Context, params *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)

	//MultipartUpload OP
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)

	//versions OP
	GetBucketVersioning(ctx context.Context, params *s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error)
	PutBucketVersioning(ctx context.Context, params *s3.PutBucketVersioningInput) (*s3.PutBucketVersioningOutput, error)
	ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error)
}

type S3User struct {
	Client      *s3.Client
	TokenBucket *TokenBucket
}

func CreateS3User(endpoints string, region string, accessKey string, secretKey string, sessionToken string, rateLimit int) *S3User {
	client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)
	tokenBucket := CreateTokenBucket(rateLimit)
	return &S3User{Client: client, TokenBucket: tokenBucket}
}

func (u *S3User) CreateBucket(ctx context.Context, params *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	u.TokenBucket.Get()
	return u.Client.CreateBucket(ctx, params)
}

func (u *S3User) DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	u.TokenBucket.Get()
	return u.Client.DeleteBucket(ctx, params)
}

func (u *S3User) PutObject(ctx context.Context, params *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	u.TokenBucket.Get()
	return u.Client.PutObject(ctx, params)
}

func (u *S3User) GetObject(ctx context.Context, params *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	u.TokenBucket.Get()
	return u.Client.GetObject(ctx, params)
}

func (u *S3User) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	u.TokenBucket.Get()
	return u.Client.DeleteObject(ctx, params)
}

func (u *S3User) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	u.TokenBucket.Get()
	return u.Client.CreateMultipartUpload(ctx, params)
}

func (u *S3User) UploadPart(ctx context.Context, params *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	u.TokenBucket.Get()
	return u.Client.UploadPart(ctx, params)
}

func (u *S3User) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	u.TokenBucket.Get()
	return u.Client.CompleteMultipartUpload(ctx, params)
}

func (u *S3User) GetBucketVersioning(ctx context.Context, params *s3.GetBucketVersioningInput) (*s3.GetBucketVersioningOutput, error) {
	u.TokenBucket.Get()
	return u.Client.GetBucketVersioning(ctx, params)
}

func (u *S3User) ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error) {
	u.TokenBucket.Get()
	return u.Client.ListObjectVersions(ctx, params)
}

func (u *S3User) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	u.TokenBucket.Get()
	return u.Client.ListObjectsV2(ctx, params)
}

func (u *S3User) PutBucketVersioning(ctx context.Context, params *s3.PutBucketVersioningInput) (*s3.PutBucketVersioningOutput, error) {
	u.TokenBucket.Get()
	return u.Client.PutBucketVersioning(ctx, params)
}
