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
		objSize      uint64
		success      uint64
		fail         uint64
		bucketName   string
		notCreate    bool
	)

	var cmd = &cobra.Command{
		Use:   "check",
		Short: "check uploaded data",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Runing...")

			// Create an Amazon S3 service client
			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

			//如果为默认测试桶则创建,否则对已有特定桶进行测试
			if !notCreate {
				_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					log.Fatal(err)
				}
				defer func() {
					client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
						Bucket: aws.String(bucketName),
					})
				}()
			}

			for i := uint64(0); i < objNum; i++ {
				func() {
					objKey, objData := GenerateObject(i, objSize)

					//上传对象
					_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
						Body:   bytes.NewReader(objData),
					})
					if err != nil {
						fail++
						log.Println(objKey + " PutObject fail:" + err.Error())
						return
					}
					defer func() {
						//删除上传对象
						client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objKey),
						})
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
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 10, "upload object num")
	cmd.Flags().Uint64VarP(&objSize, "max_object_size", "", 1*1024*1024*1024, "upload object max size")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}

func buildCOSMultipartUploadCheckCmd(parentCmd *cobra.Command) {

	const (
		OBJECTNAME = "cosbench-object"
	)

	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		objNum       uint64
		objSize      uint64
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
					client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
						Bucket: aws.String(bucketName),
					})
				}()
			}

			for i := uint64(0); i < objNum; i++ {
				func() {
					objectKey := OBJECTNAME + strconv.FormatUint(i, 10)
					objData := make([]byte, uint64(rand.Int63n(int64(objSize-partSize)))+partSize)
					rand.Read(objData)

					//上传对象
					//创建多块上传
					createMultipartUploadOutput, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
					})
					if err != nil {
						fail++
						log.Println(objectKey + " CreateMultipartUpload fail:" + err.Error())
						return
					}
					uploadId := createMultipartUploadOutput.UploadId

					//多块上传
					offset := uint64(0)
					length := partSize
					for partId := uint64(0); partId < uint64(len(objData))/partSize+1; partId++ {
						if offset+length > uint64(len(objData)) {
							length = uint64(len(objData)) - offset
						}
						reader := bytes.NewReader(objData[offset : offset+length])
						offset += length

						_, err = client.UploadPart(context.TODO(), &s3.UploadPartInput{
							Bucket:     aws.String(bucketName),
							Key:        aws.String(objectKey),
							UploadId:   uploadId,
							PartNumber: int32(partId),
							Body:       reader,
						})
						if err != nil {
							log.Println(objectKey+" UploadPart ", partId, " fail:"+err.Error())
						}
					}

					//完成多块上传
					_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
						Bucket:   aws.String(bucketName),
						Key:      aws.String(objectKey),
						UploadId: uploadId,
					})
					if err != nil {
						fail++
						log.Println(objectKey + " CompleteMultipartUpload fail:" + err.Error())
						return
					}
					defer func() {
						//删除上传对象
						client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objectKey),
						})
					}()

					//获取对象
					getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
					})
					if err != nil {
						fail++
						log.Println(objectKey + " GetObject fail:" + err.Error())
						return
					}
					buff, err := ObjectBodyToBytes(getObjectOutput)
					if err != nil {
						log.Println(objectKey + " GetObject read data fail:" + err.Error())
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
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 10, "upload object num")
	cmd.Flags().Uint64VarP(&objSize, "max_object_size", "", 4*1024*1024*1024, "upload object max size")
	cmd.Flags().Uint64VarP(&partSize, "partSize", "", 1*1024*1024*1024, "mutipartupload object partSize")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "not_create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}

func buildCOSCheck2Cmd(parentCmd *cobra.Command) {

	const (
		OBJECTNAME = "cosbench-object"
	)

	var (
		endpoints     string
		region        string
		accessKey     string
		secretKey     string
		sessionToken  string
		objNum        uint64
		currencyValue uint64
		objSize       uint64
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
					client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
						Bucket: aws.String(bucketName),
					})
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
					var dataArr [][]byte
					objectKey := OBJECTNAME + strconv.FormatUint(i, 10)
					//并发PutObject上传是否存在成功
					isSuccess := false
					for i := uint64(0); i < currencyValue; i++ {
						wg.Add(1)
						go func(objectKey string, i uint64) {
							defer wg.Done()
							objData := make([]byte, rand.Int63n(int64(objSize)))
							rand.Read(objData)
							reader := bytes.NewReader(objData)

							//上传对象
							_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
								Bucket: aws.String(bucketName),
								Key:    aws.String(objectKey),
								Body:   reader,
							})
							if err != nil {
								return
							}
							lock.Lock()
							dataArr = append(dataArr, objData)
							lock.Unlock()
							isSuccess = true
						}(objectKey, i)
					}
					wg.Wait()
					if !isSuccess {
						log.Println(objectKey + " PutObject fail")
						return
					}
					defer func() {
						//删除上传对象
						client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucketName),
							Key:    aws.String(objectKey),
						})
					}()

					//GetObject获取数据与PutObject上传成功数据块匹配个数
					success := uint64(0)
					getObjectOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
					})
					if err != nil {
						log.Println(objectKey + " GetObject fail:" + err.Error())
						return
					}
					buff, err := ObjectBodyToBytes(getObjectOutput)
					if err != nil {
						log.Println(objectKey + " GetObject read data fail:" + err.Error())
						return
					}
					for _, data := range dataArr {
						if bytes.Equal(data, buff) {
							success++
						}
					}
					if success == 0 {
						isPass = false
					}
					log.Println(objectKey+" data check equals:", success)
				}(i)
			}
			wgMain.Wait()
			if isPass {
				log.Println("check results:pass")
			} else {
				log.Println("check results:not pass")
			}

		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 10, "upload object num")
	cmd.Flags().Uint64VarP(&currencyValue, "concurrency_value", "", 100, "same object key concurrency value")
	cmd.Flags().Uint64VarP(&objSize, "max_object_size", "", 1*1024*1024, "upload object max size")
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

func GenerateObject(num uint64, size uint64) (key string, data []byte) {
	const (
		OBJECTNAME = "cosbench-object"
	)

	key = OBJECTNAME + strconv.FormatUint(num, 10)
	data = make([]byte, rand.Int63n(int64(size)))
	rand.Read(data)
	return key, data
}
