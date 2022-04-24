package cmd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func buildCOSRateCmd(parentCmd *cobra.Command) {

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
		isOutputErr  bool
		bucketName   string
		notCreate    bool
	)

	var cmd = &cobra.Command{
		Use:   "rate",
		Short: "test I/O rate",
		Long: `Op-count:操作总数
Success:操作成功数
Byte-count:传输数据总数(bytes)
Avg-ResTime:平均响应时间(ms)
Throughput:每秒操作数
Bandwidth:平均每秒传输数据量(bytes/s)`,
		Run: func(cmd *cobra.Command, args []string) {
			// Create an Amazon S3 service client
			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

			log.Println("Runing...Don't interrupt the program,if interrupted you must to manually delete cosbench-bucket")

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

			var (
				uploadedObjSet  []string
				uploadedDataSum uint64
				responseTime    uint64 //平均响应时间
			)

			var wg sync.WaitGroup
			var lock sync.Mutex
			timeArr := make([]uint64, objNum)

			log.Println("Write...")
			for i := uint64(0); i < objNum; i++ {
				wg.Add(1)
				go func(i uint64) {
					defer wg.Done()
					objectKey := OBJECTNAME + strconv.FormatUint(i, 10)
					objData := make([]byte, rand.Int63n(int64(objSize)))
					uploadedDataSum += uint64(len(objData))
					rand.Read(objData)
					reader := bytes.NewReader(objData)

					//上传对象
					t1 := time.Now()
					_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
						Body:   reader,
					})
					elapsed := time.Since(t1).Milliseconds()
					timeArr[i] = uint64(elapsed)
					if err != nil {
						if isOutputErr {
							log.Println(objectKey + " PutObject fail:" + err.Error())
						}
						return
					}
					lock.Lock()
					uploadedObjSet = append(uploadedObjSet, objectKey)
					lock.Unlock()
				}(i)
			}
			wg.Wait()
			for _, elapsed := range timeArr {
				responseTime += elapsed
			}
			if responseTime/1000 == 0 {
				log.Println("Op-count:", objNum, " Success:", len(uploadedObjSet), " Byte-count:", uploadedDataSum, " Avg-ResTime:", responseTime/objNum, "  Throughput:", objNum, " Bandwidth:", uploadedDataSum)

			} else {
				log.Println("Op-count:", objNum, " Success:", len(uploadedObjSet), " Byte-count:", uploadedDataSum, " Avg-ResTime:", responseTime/objNum, "  Throughput:", objNum/(responseTime/1000), " Bandwidth:", uploadedDataSum/(responseTime/1000))
			}

			log.Println("Read...")
			var (
				success    uint64
				getDataSum uint64
			)
			responseTime = 0
			timeArr = make([]uint64, len(uploadedObjSet))
			for i, objectKey := range uploadedObjSet {
				wg.Add(1)
				go func(index int, objectKey string) {
					defer wg.Done()
					//获取对象
					t2 := time.Now()
					getObjOutput, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
					})
					elapsed := time.Since(t2).Milliseconds()
					timeArr[index] = uint64(elapsed)
					if err != nil {
						if isOutputErr {
							log.Println(objectKey + " GetObject fail:" + err.Error())
						}
						return
					}
					lock.Lock()
					success++
					getDataSum += uint64(getObjOutput.ContentLength)
					lock.Unlock()

				}(i, objectKey)
			}
			wg.Wait()
			for _, elapsed := range timeArr {
				responseTime += elapsed
			}
			if responseTime/1000 == 0 {
				log.Println("Op-count:", len(uploadedObjSet), " Success:", success, " Byte-count:", getDataSum, " Avg-ResTime:", responseTime/uint64(len(uploadedObjSet)), "  Throughput:", uint64(len(uploadedObjSet)), " Bandwidth:", getDataSum)
			} else {
				log.Println("Op-count:", len(uploadedObjSet), " Success:", success, " Byte-count:", getDataSum, " Avg-ResTime:", responseTime/uint64(len(uploadedObjSet)), "  Throughput:", uint64(len(uploadedObjSet))/(responseTime/1000), " Bandwidth:", getDataSum/(responseTime/1000))
			}

			log.Println("Delete...")
			responseTime = 0
			success = 0
			timeArr = make([]uint64, len(uploadedObjSet))
			//删除上传对象
			for i, objectKey := range uploadedObjSet {
				wg.Add(1)
				go func(index int, objectKey string) {
					defer wg.Done()
					t3 := time.Now()
					_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objectKey),
					})
					elapsed := time.Since(t3).Milliseconds()
					timeArr[index] = uint64(elapsed)
					if err != nil {
						if isOutputErr {
							log.Println(objectKey + " DeleteObject fail:" + err.Error())
						}
						return
					}
					lock.Lock()
					success++
					lock.Unlock()
				}(i, objectKey)
			}

			wg.Wait()
			for _, elapsed := range timeArr {
				responseTime += elapsed
			}
			if responseTime/1000 == 0 {
				log.Println("Op-count:", len(uploadedObjSet), " Success:", success, " Avg-ResTime:", responseTime/uint64(len(uploadedObjSet)), "  Throughput:", uint64(len(uploadedObjSet)))
			} else {
				log.Println("Op-count:", len(uploadedObjSet), " Success:", success, " Avg-ResTime:", responseTime/uint64(len(uploadedObjSet)), "  Throughput:", uint64(len(uploadedObjSet))/(responseTime/1000))
			}

		},
	}
	cmd.Flags().StringVarP(&endpoints, "endpoints", "p", "", "specify the endpoint")
	cmd.Flags().StringVarP(&region, "region", "", "us-east-1", "specify the Region")
	cmd.Flags().StringVarP(&accessKey, "access_key", "a", "", "specify the access_key")
	cmd.Flags().StringVarP(&secretKey, "secret_key", "s", "", "specify the secret_key")
	cmd.Flags().StringVarP(&sessionToken, "session_token", "", "", "specify the session_token")
	cmd.Flags().Uint64VarP(&objNum, "max_object_num", "", 10000, "upload object num")
	cmd.Flags().Uint64VarP(&objSize, "max_object_size", "", 1*1024*1024, "upload object max size")
	cmd.Flags().BoolVarP(&isOutputErr, "err", "e", false, "output err")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}
