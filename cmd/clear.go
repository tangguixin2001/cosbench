package cmd

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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

			customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					PartitionID:       "aws",
					URL:               endpoints,
					SigningRegion:     region,
					HostnameImmutable: true,
				}, nil
			})

			// Load the Shared AWS Configuration (~/.aws/config)
			cfg, err := config.LoadDefaultConfig(
				context.TODO(),
				config.WithRegion(region),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)),
				config.WithEndpointResolverWithOptions(customResolver),
			)
			if err != nil {
				log.Fatal(err)
			}

			// Create an Amazon S3 service client
			client := s3.NewFromConfig(cfg)

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
