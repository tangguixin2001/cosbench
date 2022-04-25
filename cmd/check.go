package cmd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"io"
	"log"
	"math/rand"
	"strconv"
	"sync"
)

func buildCOSCheckCmd(parentCmd *cobra.Command) {
	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		objNum       uint64
		objMaxSize   uint64
		objMinSize   uint64
		success      uint64
		bucketName   string
		notCreate    bool
	)

	var cmd = &cobra.Command{
		Use:   "check",
		Short: "check uploaded data",
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
			}

			var wg sync.WaitGroup
			var lock sync.Mutex

			for i := uint64(0); i < objNum; i++ {
				wg.Add(1)
				go func(i uint64) {
					defer wg.Done()
					objKey, objData := GenerateObject(i, objMaxSize, objMinSize)

					//上传对象
					_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
						Body:   bytes.NewReader(objData),
					})
					if err != nil {
						log.Println(objKey + " PutObject fail:" + err.Error())
						return
					}
					defer func() {
						//删除上传对象
						_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objKey),
						})
						if err != nil {
							log.Println(objKey + " DeleteObject fail:" + err.Error())
						}
					}()

					//获取对象
					getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
					})
					if err != nil {
						log.Println(objKey + " GetObject fail:" + err.Error())
						return
					}

					buff, err := ObjectBodyToBytes(getObjectOutput)
					if err != nil {
						log.Println(objKey + " GetObject read data fail:" + err.Error())
						return
					}

					//校验对象
					if !bytes.Equal(buff, objData) {
						return
					}
					lock.Lock()
					success++
					lock.Unlock()
				}(i)
			}
			wg.Wait()
			log.Println("check results:")
			log.Printf("sum: %v success: %v\n", objNum, success)
		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 100, "upload object num")
	cmd.Flags().Uint64VarP(&objMaxSize, "max_object_size", "", 100*1024*1024, "upload object max size")
	cmd.Flags().Uint64VarP(&objMinSize, "min_object_size", "", 0, "upload object min size")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}

func buildCOSMultipartUploadCheckCmd(parentCmd *cobra.Command) {
	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		objNum       uint64
		objMaxSize   uint64
		objMinSize   uint64
		partSize     uint64
		success      uint64
		fail         uint64
		bucketName   string
		notCreate    bool
	)

	var cmd = &cobra.Command{
		Use:   "multicheck",
		Short: "check multipart uploaded data",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Runing...")

			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

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
			}

			for i := uint64(0); i < objNum; i++ {
				func() {
					objKey, objData := GenerateObject(i, objMaxSize, objMinSize)

					//上传对象
					//创建多块上传
					createMultipartUploadOutput, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
					})
					if err != nil {
						fail++
						log.Println(objKey + " CreateMultipartUpload fail:" + err.Error())
						return
					}
					uploadId := createMultipartUploadOutput.UploadId

					//多块上传
					var wg sync.WaitGroup
					offset := uint64(0)
					length := partSize
					for partId := uint64(0); partId < uint64(len(objData))/partSize+1; partId++ {
						if offset+length > uint64(len(objData)) {
							length = uint64(len(objData)) - offset
						}
						wg.Add(1)
						go func(partId uint64, data []byte) {
							defer wg.Done()
							_, err = client.UploadPart(context.TODO(), &s3.UploadPartInput{
								Bucket:     aws.String(bucketName),
								Key:        aws.String(objKey),
								UploadId:   uploadId,
								PartNumber: int32(partId),
								Body:       bytes.NewReader(data),
							})
							if err != nil {
								log.Println(objKey+" UploadPart ", partId, " fail:"+err.Error())
							}
						}(partId, objData[offset:offset+length])
						offset += length
					}

					//完成多块上传
					wg.Wait()
					_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
						Bucket:   aws.String(bucketName),
						Key:      aws.String(objKey),
						UploadId: uploadId,
					})
					if err != nil {
						fail++
						log.Println(objKey + " CompleteMultipartUpload fail:" + err.Error())
						return
					}
					defer func() {
						//删除上传对象
						_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objKey),
						})
						if err != nil {
							log.Println(objKey + " DeleteObject fail:" + err.Error())
						}
					}()

					//获取对象
					getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
					})
					if err != nil {
						fail++
						log.Println(objKey + " GetObject fail:" + err.Error())
						return
					}
					buff, err := ObjectBodyToBytes(getObjectOutput)
					if err != nil {
						log.Println(objKey + " GetObject read data fail:" + err.Error())
						return
					}

					//校验对象
					if !bytes.Equal(buff, objData) {
						fail++
						return
					}
					success++
				}()
			}

			log.Println("check results:")
			log.Printf("sum: %v success: %v fail: %v", objNum, success, fail)
		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 100, "upload object num")
	cmd.Flags().Uint64VarP(&objMaxSize, "max_object_size", "", 100*1024*1024, "upload object max size")
	cmd.Flags().Uint64VarP(&objMinSize, "min_object_size", "", 0, "upload object min size")
	cmd.Flags().Uint64VarP(&partSize, "partSize", "", 25*1024*1024, "mutipartupload object partSize")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}

func buildCOSCheck2Cmd(parentCmd *cobra.Command) {
	var (
		endpoints     string
		region        string
		accessKey     string
		secretKey     string
		sessionToken  string
		objNum        uint64
		currencyValue uint64
		objMaxSize    uint64
		objMinSize    uint64
		bucketName    string
		notCreate     bool
	)

	var cmd = &cobra.Command{
		Use:   "check2",
		Short: "check concurrency upload same object key",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Runing...")

			// Create an Amazon S3 service client
			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

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
			}

			isPass := true

			var wgMain sync.WaitGroup

			for i := uint64(0); i < objNum; i++ {
				wgMain.Add(1)
				go func(i uint64) {
					defer wgMain.Done()
					var wg sync.WaitGroup
					var lock sync.Mutex
					//保存上传成功对象数据,以确定最后是否存在与对象数据保持一致的原始数据
					var dataArr [][]byte
					objKey, _ := GenerateObject(i, 0, 0)

					//并发PutObject上传同名对象
					for i := uint64(0); i < currencyValue; i++ {
						wg.Add(1)
						go func(objKey string) {
							defer wg.Done()

							_, objData := GenerateObject(0, objMaxSize, objMinSize)
							//上传对象
							_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
								Bucket: aws.String(bucketName),
								Key:    aws.String(objKey),
								Body:   bytes.NewReader(objData),
							})
							if err != nil {
								return
							}
							lock.Lock()
							dataArr = append(dataArr, objData)
							lock.Unlock()
						}(objKey)
					}
					wg.Wait()
					if len(dataArr) == 0 {
						log.Println(objKey + " PutObject fail")
						return
					}

					defer func() {
						//删除上传对象
						_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objKey),
						})
						if err != nil {
							log.Println(objKey + " DeleteObject fail:" + err.Error())
						}
					}()

					//GetObject获取数据与PutObject上传成功数据块匹配个数
					isSuccess := false
					getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
					})
					if err != nil {
						log.Println(objKey + " GetObject fail:" + err.Error())
						return
					}
					buff, err := ObjectBodyToBytes(getObjectOutput)
					if err != nil {
						log.Println(objKey + " GetObject read data fail:" + err.Error())
						return
					}
					for _, data := range dataArr {
						if bytes.Equal(data, buff) {
							isSuccess = true
							break
						}
					}
					if !isSuccess {
						isPass = false
					}
					log.Println(objKey+" data check:", isSuccess)
				}(i)
			}
			wgMain.Wait()
			log.Println("Pass:", isPass)

		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 100, "upload object num")
	cmd.Flags().Uint64VarP(&currencyValue, "concurrency_value", "", 100, "same object key concurrency value")
	cmd.Flags().Uint64VarP(&objMaxSize, "max_object_size", "", 1*1024*1024, "upload object max size")
	cmd.Flags().Uint64VarP(&objMinSize, "min_object_size", "", 0, "upload object min size")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}

func CreateS3Client(endpoints string, region string, accessKey string, secretKey string, sessionToken string) *s3.Client {
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
	return s3.NewFromConfig(cfg)
}

func ObjectBodyToBytes(object *s3.GetObjectOutput) ([]byte, error) {
	length := 0
	buff := make([]byte, object.ContentLength)
	for {
		n, err := object.Body.Read(buff[length:])
		if n > 0 {
			length += n
		} else if err == io.EOF || err != nil {
			object.Body.Close()
			if err != io.EOF {
				return buff, err
			}
			break
		}
	}
	return buff, nil
}

func GenerateObject(num uint64, maxSize uint64, minSize uint64) (key string, data []byte) {
	const (
		OBJECTNAME = "cosbench-object"
	)

	key = OBJECTNAME + strconv.FormatUint(num, 10)
	data = make([]byte, rand.Int63n(int64(maxSize-minSize+1))+int64(minSize))
	rand.Read(data)
	return key, data
}
