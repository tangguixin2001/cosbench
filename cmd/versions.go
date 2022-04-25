package cmd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
	"log"
	"sync"
)

func buildVersionsCmd(parentCmd *cobra.Command) {
	var ()
	var cmd = &cobra.Command{
		Use:   "versions",
		Short: "versions bucket test",
	}

	parentCmd.AddCommand(cmd)

	buildCOSVersionsCheckCmd(cmd)
}

func buildCOSVersionsCheckCmd(parentCmd *cobra.Command) {
	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		objNum       uint64
		objVersions  uint64
		objMaxSize   uint64
		objMinSize   uint64
		success      uint64
		bucketName   string
		notCreate    bool
	)

	var cmd = &cobra.Command{
		Use:   "check",
		Short: "check uploaded data in versions bucket",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Runing...")

			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

			//是否创建桶
			if !notCreate {
				_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}
				defer func() {
					_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
						Bucket: aws.String(bucketName),
					})
					if err != nil {
						log.Println(bucketName + " DeleteBucket fail:" + err.Error())
					}
				}()
				//开启版本控制
				_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
					Bucket:                  aws.String(bucketName),
					VersioningConfiguration: &types.VersioningConfiguration{Status: "Enabled"},
				})
				if err != nil {
					log.Fatal(err)
				}
			}

			var wg sync.WaitGroup
			var lock sync.Mutex

			for i := uint64(0); i < objNum; i++ {
				wg.Add(1)
				go func(i uint64) {
					defer wg.Done()
					objKey, _ := GenerateObject(i, 0, 0)
					for i = uint64(0); i < objVersions; i++ {
						wg.Add(1)
						go func(objKey string) {
							defer wg.Done()
							_, objData := GenerateObject(0, objMaxSize, objMinSize)
							//上传对象
							putObjectOutput, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
								Bucket: aws.String(bucketName),
								Key:    aws.String(objKey),
								Body:   bytes.NewReader(objData),
							})
							if err != nil {
								log.Println(objKey + " PutObject fail:" + err.Error())
								return
							}
							defer func() {
								//删除对象指定版本
								_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
									Bucket:    aws.String(bucketName),
									Key:       aws.String(objKey),
									VersionId: putObjectOutput.VersionId,
								})
								if err != nil {
									log.Println(objKey+" DeleteObject"+" versionId ", putObjectOutput.VersionId, " fail:"+err.Error())
								}
							}()

							//获取对象指定版本
							getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
								Bucket:    aws.String(bucketName),
								Key:       aws.String(objKey),
								VersionId: putObjectOutput.VersionId,
							})
							if err != nil {
								log.Println(objKey+" GetObject"+" versionId ", putObjectOutput.VersionId, " fail:"+err.Error())
								return
							}

							buff, err := ObjectBodyToBytes(getObjectOutput)
							if err != nil {
								log.Println(objKey+" GetObject"+" versionId ", putObjectOutput.VersionId, " read data fail:"+err.Error())
								return
							}

							//校验对象指定版本
							if !bytes.Equal(buff, objData) {
								return
							}
							lock.Lock()
							success++
							lock.Unlock()
						}(objKey)
					}
				}(i)
			}
			wg.Wait()
			log.Println("check results:")
			log.Printf("sum: %v success: %v\n", objNum*objVersions, success)
		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 100, "upload object num")
	cmd.Flags().Uint64VarP(&objVersions, "max_object_versions", "", 100, "upload object versions")
	cmd.Flags().Uint64VarP(&objMaxSize, "max_object_size", "", 1*1024*1024, "upload object max size")
	cmd.Flags().Uint64VarP(&objMinSize, "min_object_size", "", 0, "upload object min size")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}
