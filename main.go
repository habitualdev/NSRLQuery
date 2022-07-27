package main

import (
	"NSRLQuery/nsrl"
	"NSRLQuery/utilities"
	"bufio"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"github.com/roaldi/bloom"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Variable Block

var bucketList []bucketEntry

var Workers int

var jobsChan = make(chan string, 1)

var targetFile = "rds_modernm/rds_modernm/NSRLFile.txt"

// Type Block

type NSRLDataPoint struct {
	SHA1         string
	MD5          string
	CRC32        string
	FileName     string
	FileSize     string
	ProductCode  string
	OpSystemCode string
	SpecialCode  string
}

type bucketEntry struct {
	Bucket     *bloom.BloomFilter
	BucketName string
	Hash       string
	M          uint
	K          uint
	KeyName    string
}

// Functions Block

func makeBucket(numLines int, dataBuffer []string, iteration int, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	fDump, _ := os.OpenFile("buckets/"+strconv.Itoa(iteration)+".bkt", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	defer fDump.Close()
	for _, dataBufferLine := range dataBuffer {
		fDump.Write([]byte(dataBufferLine + "\n"))
	}
	sha1Pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "127.0.0.1:6379") },
	}
	md5Pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "127.0.0.1:6379") },
	}
	namePool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "127.0.0.1:6379") },
	}

	sha1Conn := sha1Pool.Get()
	md5Conn := md5Pool.Get()

	shaBitsetName := strconv.Itoa(iteration) + "_sha256_key"
	md5BitsetName := strconv.Itoa(iteration) + "_md5_key"
	nameBitsetName := strconv.Itoa(iteration) + "_name_key"

	m, k := bloom.EstimateParameters(uint(numLines), .01)

	sha1BitSet := bloom.NewRedisBitSet(shaBitsetName, m, sha1Conn)
	sha1Bloom := bloom.New(m, k, sha1BitSet)
	md5BitSet := bloom.NewRedisBitSet(md5BitsetName, m, md5Conn)
	md5Bloom := bloom.New(m, k, md5BitSet)
	nameBitSet := bloom.NewRedisBitSet(nameBitsetName, m, namePool.Get())
	nameBloom := bloom.New(m, k, nameBitSet)

	for _, lineBuffer := range dataBuffer {
		delimitedText := strings.Split(strings.ReplaceAll(lineBuffer, "\"", ""), ",")
		if len(delimitedText) == 1 {
			continue
		}
		tempDatapoint := NSRLDataPoint{
			SHA1:         delimitedText[0],
			MD5:          delimitedText[1],
			CRC32:        delimitedText[2],
			FileName:     delimitedText[3],
			FileSize:     delimitedText[4],
			ProductCode:  delimitedText[5],
			OpSystemCode: delimitedText[6],
			SpecialCode:  delimitedText[7],
		}

		sha1Bloom.Add([]byte(tempDatapoint.SHA1))
		md5Bloom.Add([]byte(tempDatapoint.MD5))
		nameBloom.Add([]byte(tempDatapoint.FileName))
	}

	sha1Entry := bucketEntry{
		Bucket:     sha1Bloom,
		BucketName: strconv.Itoa(iteration),
		Hash:       "sha1",
		M:          m,
		K:          k,
		KeyName:    shaBitsetName,
	}

	md5Entry := bucketEntry{
		Bucket:     md5Bloom,
		BucketName: strconv.Itoa(iteration),
		Hash:       "md5",
		M:          m,
		K:          k,
		KeyName:    md5BitsetName,
	}

	nameEntry := bucketEntry{
		Bucket:     nameBloom,
		BucketName: strconv.Itoa(iteration),
		Hash:       "name",
		M:          m,
		K:          k,
		KeyName:    nameBitsetName,
	}

	bucketList = append(bucketList, sha1Entry, md5Entry, nameEntry)
	printLine := <-jobsChan
	fmt.Print("\r")
	fmt.Print(printLine)
}

func retrieveData(bucket bucketEntry, searchHash string) string {
	tempOpen, _ := os.Open("buckets/" + (bucket.BucketName) + ".bkt")
	tempScanner := bufio.NewScanner(tempOpen)
	for tempScanner.Scan() {
		hashMatch, _ := regexp.Match(`.*`+searchHash+`.*`, []byte(tempScanner.Text()))
		if hashMatch {
			return tempScanner.Text()
		}
	}
	return ""
}

func main() {
	bucketList = []bucketEntry{}
	nsrl.GetNSrl()
	utilities.PrepFilesytem()
	FlushRedis()
	LoadRedis()
	startTime := time.Now()

	bucketInfo, _ := os.ReadDir("buckets")

	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "The Ferret Says Hello!",
		})
	})
	router.GET("/stats/buckets", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"buckets": len(bucketInfo),
		})
	})
	router.GET("/stats/uptime", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"uptime": time.Since(startTime),
		})
	})
	router.GET("/query", func(c *gin.Context) {
		hash := c.Query("hash")
		if hash == "" {
			c.JSON(200, gin.H{
				"message": "No hash provided",
			})
			return
		}
		value := Search(bucketList, hash)
		if value == "" {
			c.JSON(200, gin.H{
				"message": "No match found",
			})
			return
		} else {
			c.JSON(200, gin.H{
				"message": value,
			})
			return
		}
	})
	router.Run()
}
func Search(BucketList []bucketEntry, searchHash string) string {
	for _, bucket := range BucketList {
		if checkState, err := bucket.Bucket.Exists([]byte(searchHash)); checkState && err == nil {
			data := retrieveData(bucket, searchHash)
			return data
		} else if err != nil {
			println("Error: " + err.Error())
		}
	}
	return ""
}
