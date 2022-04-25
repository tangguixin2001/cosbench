package cmd

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
	"log"
	"sync"
)

func buildCOSClearCmd(parentCmd *cobra.Command) {

	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		bucketName   string
	)

	var cmd = &cobra.Command{
		Use:   "clear",
		Short: "clear data",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Runing...")

			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

			//获取桶的版本控制状态
			getBucketVersioningOutput, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				log.Fatal(err)
			}

			if getBucketVersioningOutput.Status == "Enabled" {
				listObjectVersionsOutput, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}

				var wg sync.WaitGroup

				if len(listObjectVersionsOutput.Versions) > 0 {
					for _, object := range listObjectVersionsOutput.Versions {
						wg.Add(1)
						go func(object types.ObjectVersion) {
							defer wg.Done()
							_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
								Bucket:    aws.String(bucketName),
								Key:       object.Key,
								VersionId: object.VersionId,
							})
							if err != nil {
								log.Fatal(err)
							}
						}(object)
					}
				}
				if len(listObjectVersionsOutput.DeleteMarkers) > 0 {
					for _, object := range listObjectVersionsOutput.DeleteMarkers {
						wg.Add(1)
						go func(object types.DeleteMarkerEntry) {
							defer wg.Done()
							_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
								Bucket:    aws.String(bucketName),
								Key:       object.Key,
								VersionId: object.VersionId,
							})
							if err != nil {
								log.Fatal(err)
							}
						}(object)
					}
				}

				for listObjectVersionsOutput.IsTruncated {
					listObjectVersionsOutput, err = client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
						Bucket:          aws.String(bucketName),
						KeyMarker:       listObjectVersionsOutput.KeyMarker,
						VersionIdMarker: listObjectVersionsOutput.VersionIdMarker,
					})
					if err != nil {
						log.Fatal(err)
					}
					if len(listObjectVersionsOutput.Versions) > 0 {
						for _, object := range listObjectVersionsOutput.Versions {
							wg.Add(1)
							go func(object types.ObjectVersion) {
								defer wg.Done()
								_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
									Bucket:    aws.String(bucketName),
									Key:       object.Key,
									VersionId: object.VersionId,
								})
								if err != nil {
									log.Fatal(err)
								}
							}(object)
						}
					}
					if len(listObjectVersionsOutput.DeleteMarkers) > 0 {
						for _, object := range listObjectVersionsOutput.DeleteMarkers {
							wg.Add(1)
							go func(object types.DeleteMarkerEntry) {
								defer wg.Done()
								_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
									Bucket:    aws.String(bucketName),
									Key:       object.Key,
									VersionId: object.VersionId,
								})
								if err != nil {
									log.Fatal(err)
								}
							}(object)
						}
					}
				}

				wg.Wait()

				_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}

				log.Println("complete")
			} else if getBucketVersioningOutput.Status == "Suspended" {
				listObjectsV2Output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}

				var wg sync.WaitGroup

				if listObjectsV2Output.KeyCount > 0 {
					for _, object := range listObjectsV2Output.Contents {
						wg.Add(1)
						go func(object types.Object) {
							defer wg.Done()
							_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
								Bucket: aws.String(bucketName),
								Key:    object.Key,
							})
							if err != nil {
								log.Fatal(err)
							}
						}(object)
					}
				}
				for listObjectsV2Output.IsTruncated {
					listObjectsV2Output, err = client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
						Bucket:            aws.String(bucketName),
						ContinuationToken: listObjectsV2Output.NextContinuationToken,
					})
					if err != nil {
						log.Fatal(err)
					}
					if listObjectsV2Output.KeyCount > 0 {
						for _, object := range listObjectsV2Output.Contents {
							wg.Add(1)
							go func(object types.Object) {
								defer wg.Done()
								_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
									Bucket: aws.String(bucketName),
									Key:    object.Key,
								})
								if err != nil {
									log.Fatal(err)
								}

							}(object)
						}
					}
				}

				wg.Wait()

				_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}

				log.Println("complete")
			} else {
				log.Println(bucketName, " GetBucketVersioningStatus error")
			}

		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "clear other bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}
