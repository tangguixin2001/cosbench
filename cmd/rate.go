package cmd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"log"
	"sync"
	"time"
)

func buildCOSRateCmd(parentCmd *cobra.Command) {
	var (
		endpoints    string
		region       string
		accessKey    string
		secretKey    string
		sessionToken string
		objNum       uint64
		objMaxSize   uint64
		objMinSize   uint64
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
			client := CreateS3Client(endpoints, region, accessKey, secretKey, sessionToken)

			log.Println("Runing...Don't interrupt the program,if interrupted you must to manually delete bucket")

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

			var (
				uploadedObjSet  []string
				uploadedDataSum uint64
				getDataSum      uint64
				elapsedSum      uint64
			)
			var wg sync.WaitGroup
			var lock sync.Mutex
			timeArr := make([]uint64, objNum)

			log.Println("Write...")
			var (
				opCount    uint64  //总操作数
				success    int     //成功操作数
				byteCount  uint64  //总传输数据量
				avgResTime uint64  //平均响应时间
				throughput float64 //平均每秒操作数
				bandwidth  uint64  //平均每秒传输数据量
			)
			for i := uint64(0); i < objNum; i++ {
				wg.Add(1)
				go func(i uint64) {
					defer wg.Done()
					objKey, objData := GenerateObject(i, objMaxSize, objMinSize)
					uploadedDataSum += uint64(len(objData))

					//上传对象
					t1 := time.Now()
					_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
						Bucket: aws.String(bucketName),
						Key:    aws.String(objKey),
						Body:   bytes.NewReader(objData),
					})
					elapsed := time.Since(t1).Milliseconds()
					timeArr[i] = uint64(elapsed)
					if err != nil {
						if isOutputErr {
							log.Println(objKey + " PutObject fail:" + err.Error())
						}
						return
					}
					lock.Lock()
					uploadedObjSet = append(uploadedObjSet, objKey)
					lock.Unlock()
				}(i)
			}
			wg.Wait()
			for _, elapsed := range timeArr {
				elapsedSum += elapsed
			}
			opCount = objNum
			success = len(uploadedObjSet)
			byteCount = uploadedDataSum
			avgResTime = elapsedSum / opCount
			throughput = float64(opCount) * 1000 / float64(elapsedSum)
			bandwidth = byteCount * 1000 / elapsedSum
			log.Printf("Op-count:%v Success:%v Byte-count:%v Avg-ResTime:%v Throughput:%.2f Bandwidth:%v\n", opCount, success, byteCount, avgResTime, throughput, bandwidth)

			log.Println("Read...")
			elapsedSum = 0
			timeArr = make([]uint64, len(uploadedObjSet))
			opCount = 0
			success = 0
			byteCount = 0
			avgResTime = 0
			throughput = 0
			bandwidth = 0
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
				elapsedSum += elapsed
			}
			opCount = uint64(len(uploadedObjSet))
			byteCount = getDataSum
			avgResTime = elapsedSum / opCount
			throughput = float64(opCount) * 1000 / float64(elapsedSum)
			bandwidth = byteCount * 1000 / elapsedSum
			log.Printf("Op-count:%v Success:%v Byte-count:%v Avg-ResTime:%v Throughput:%.2f Bandwidth:%v\n", opCount, success, byteCount, avgResTime, throughput, bandwidth)

			log.Println("Delete...")
			elapsedSum = 0
			timeArr = make([]uint64, len(uploadedObjSet))
			opCount = 0
			success = 0
			avgResTime = 0
			throughput = 0
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
				elapsedSum += elapsed
			}
			opCount = uint64(len(uploadedObjSet))
			avgResTime = elapsedSum / opCount
			throughput = float64(opCount) * 1000 / float64(elapsedSum)
			log.Printf("Op-count:%v Success:%v Avg-ResTime:%v Throughput:%.2f\n", opCount, success, avgResTime, throughput)

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
	cmd.Flags().BoolVarP(&isOutputErr, "err", "e", false, "output err")
	cmd.Flags().StringVarP(&bucketName, "bucket", "b", "cosbench-bucket", "specify the bucket")
	cmd.Flags().BoolVarP(&notCreate, "create", "", false, "not create bucket")
	cmd.MarkFlagRequired("access_key")
	cmd.MarkFlagRequired("secret_key")
	cmd.MarkFlagRequired("endpoints")

	parentCmd.AddCommand(cmd)
}
