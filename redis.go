package main

import (
	"bufio"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func LoadRedis() {

	var wg sync.WaitGroup
	//Begin loading NSRL data into Redis

	file, err := os.Open(targetFile)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	iteration := 0
	numLines := 20000

	for scanner.Scan() {
		bucketCount := 0
		var bucketData []string
		for bucketCount < numLines+1 {
			bucketData = append(bucketData, scanner.Text())
			scanner.Scan()
			bucketCount = bucketCount + 1
		}
		jobsChan <- "Current NSRL Items Loaded: " + strconv.Itoa(numLines*iteration)
		go makeBucket(numLines, bucketData, iteration, &wg)
		iteration = iteration + 1
	}
	wg.Wait()
	os.WriteFile("NSRL.lock", []byte(time.Now().Format(time.RFC822Z)), 0644)
}

func FlushRedis() {
	redisConn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}
	redisReply, _ := redisConn.Do("FLUSHALL")
	print(redisReply)
	fmt.Println("Redis flush: " + fmt.Sprint(redisReply))
}
