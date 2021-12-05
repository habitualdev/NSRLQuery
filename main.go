package main

import (
	"archive/zip"
	"bufio"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"github.com/roaldi/bloom"
	"sync"
	"time"
)

// Variable Block

var bucketList []bucketEntry

var jobsChan = make(chan string, 1)

var targetFile = "rds_modernm/NSRLFile.txt"

// Type Block

type NSRLDataPoint struct {
	SHA1 string
	MD5 string
	CRC32 string
	FileName string
	FileSize string
	ProductCode string
	OpSystemCode string
	SpecialCode string
}

type bucketEntry struct {
	Bucket *bloom.BloomFilter
	BucketName string
	Hash string
}

// Functions Block

func makeBucket(numLines int, dataBuffer []string, iteration int, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	fDump, _ := os.OpenFile("buckets/"+strconv.Itoa(iteration)+".bkt",os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	defer fDump.Close()
	for _,dataBufferLine := range dataBuffer{
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

	sha1Conn := sha1Pool.Get()
	md5Conn := md5Pool.Get()

	shaBitsetName := strconv.Itoa(iteration) + "_sha256_key"
	md5BitsetName := strconv.Itoa(iteration) + "_md5_key"


	m, k := bloom.EstimateParameters(uint(numLines), .01)

	sha1BitSet := bloom.NewRedisBitSet(shaBitsetName, m, sha1Conn)
	sha256Bloom := bloom.New(m, k, sha1BitSet)

	md5BitSet := bloom.NewRedisBitSet(md5BitsetName, m, md5Conn)
	md5Bloom := bloom.New(m, k, md5BitSet)

	for _, lineBuffer := range dataBuffer {
		delimitedText := strings.Split(strings.ReplaceAll(lineBuffer, "\"", ""), ",")
		if len(delimitedText) == 1{continue}
		tempDatapoint := NSRLDataPoint{
			SHA1:       delimitedText[0],
			MD5:          delimitedText[1],
			CRC32:        delimitedText[2],
			FileName:     delimitedText[3],
			FileSize:     delimitedText[4],
			ProductCode:  delimitedText[5],
			OpSystemCode: delimitedText[6],
			SpecialCode:  delimitedText[7],
		}

		sha256Bloom.Add([]byte(tempDatapoint.SHA1))

		md5Bloom.Add([]byte(tempDatapoint.MD5))
	}

	sha1Entry := bucketEntry{
		Bucket:     sha256Bloom,
		BucketName: strconv.Itoa(iteration),
		Hash: "sha256",
	}

	md5Entry := bucketEntry{
		Bucket:     md5Bloom,
		BucketName: strconv.Itoa(iteration),
		Hash: "md5",
	}
	bucketList = append(bucketList, sha1Entry,md5Entry)
	printLine := <- jobsChan
	fmt.Print("\r")
	fmt.Print(printLine)
}

func getNSrl(){
	linkUrl := "https://s3.amazonaws.com/rds.nsrl.nist.gov/RDS/current/rds_modernm.zip"
	_, err := os.Stat("rds_modernm/NSRLFile.txt")
	if err != nil{
		println("NSRL hash list not found, downloading...")
		resp, err := http.Get(linkUrl)
		if err != nil{
			println(err.Error())
			os.Exit(1)
		}
		defer resp.Body.Close()
		 out, _ := os.Create("rds_modernm.zip")
		 defer out.Close()
		 io.Copy(out, resp.Body)
	}
	unzip()
	println("Loading data into REDIS Bloom Filters...")
}

func unzip(){
	dst := "rds_modernm"
	archive, err := zip.OpenReader("rds_modernm.zip")
	if err != nil {
		panic(err)
	}
	defer archive.Close()

	for _, f := range archive.File {
		filePath := filepath.Join(dst, f.Name)
		fmt.Println("unzipping file ", filePath)

		if !strings.HasPrefix(filePath, filepath.Clean(dst)+string(os.PathSeparator)) {
			fmt.Println("invalid file path")
			return
		}
		if f.FileInfo().IsDir() {
			fmt.Println("creating directory...")
			os.MkdirAll(filePath, os.ModePerm)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
			panic(err)
		}

		dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			panic(err)
		}

		fileInArchive, err := f.Open()
		if err != nil {
			panic(err)
		}

		if _, err := io.Copy(dstFile, fileInArchive); err != nil {
			panic(err)
		}

		dstFile.Close()
		fileInArchive.Close()
	}
}

func prepFilesytem(){

	if _, err := os.Stat("buckets"); err != nil {
		os.Mkdir("buckets", 0700)
	}

}

func retrieveData(bucket bucketEntry, searchHash string){
	tempOpen, _ := os.Open("buckets/"+(bucket.BucketName)+".bkt")
	tempScanner := bufio.NewScanner(tempOpen)
	println(bucket.BucketName)
	println(bucket.Hash)
	for tempScanner.Scan() {
		hashMatch, _ := regexp.Match(`.*`+searchHash+`.*`, []byte(tempScanner.Text()))

		if hashMatch {
			knownHash, _ := os.OpenFile("found.list", os.O_CREATE|os.O_APPEND|os.O_RDWR,0644)
			knownHash.Write([]byte(tempScanner.Text() + "\n"))
			knownHash.Close()
		}
	}

}

func loadRedis(){

	var wg sync.WaitGroup
	//Begin loading NSRL data into Redis

	file, err := os.Open(targetFile)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	iteration := 0
	numLines := 10000

	for scanner.Scan() {
		bucketCount := 0
		var bucketData []string
		for bucketCount < numLines+1 {
			bucketData = append(bucketData, scanner.Text())
			scanner.Scan()
			bucketCount = bucketCount + 1
		}
		jobsChan <- "Current NSRL Items Loaded: " + strconv.Itoa(numLines * iteration)
		go makeBucket(numLines, bucketData, iteration, &wg)
		iteration = iteration + 1
	}
	wg.Wait()

	ioutil.WriteFile("NSRL.lock",[]byte(time.Now().Format(time.RFC822Z)), 0644)
}

func flushRedis(){
	redisConn, _ := redis.Dial("tcp","127.0.0.1:6379")
	redisReply, _ := redisConn.Do("FLUSHALL")
	print(redisReply)
	fmt.Println("Redis flush: " + fmt.Sprint(redisReply))
}

// Main Block

func main() {

	// Check/Retrieve the hashlist
	// TODO: Add UI watcher for the download
	getNSrl()

	// For now just creates the bucket folder
	// TODO: Maybe add more cleanup/sanitization?
	prepFilesytem()

	// Check for lock file, and initiate REDIS load depending on lock file / user interaction
	_, err := os.Stat("NSRL.lock")
	if err != nil{
		flushRedis()
		loadRedis()
		} else{
			timeDate,_ := ioutil.ReadFile("NSRL.lock")
			fmt.Println("NSRL may have already been loaded into redis on " + string(timeDate))
			fmt.Println("Do you wish to reload data?\nY/N")
			var input string
			fmt.Scanln(&input)
			if (input == "Y") || (input == "y"){
			flushRedis()
			loadRedis()
			}
	}

	// Start comparing the sample list with the redis bloom filters, then retrieving results
	// Known hashes go to known.list with data, unknown go to unknown.list as just the hash
	// TODO: Add more UI representation of the bucket/bloom filter searches

	testFile, _ := os.Open("test.list")
	testScanner := bufio.NewScanner(testFile)
	testLineNumber := 1
	for testScanner.Scan(){
	searchHash := testScanner.Text()
	for _, bucket := range bucketList {
		if checkState, _ := bucket.Bucket.Exists([]byte(searchHash)); checkState {
			retrieveData(bucket, searchHash)
		}else{
			unknownHash, _ := os.OpenFile("not_found.list", os.O_CREATE|os.O_APPEND|os.O_RDWR,0644)
			unknownHash.Write([]byte(searchHash + "\n"))
			unknownHash.Close()
		}

		if err := testScanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Print("\r")
	fmt.Print("                                                  ")
	fmt.Print("\r")
	fmt.Print("Hashes searched: " + strconv.Itoa(testLineNumber))
	testLineNumber = testLineNumber + 1
}
}
