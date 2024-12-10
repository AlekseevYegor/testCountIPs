package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

const (
	IPv4Max       = 4294967296   // 2^32 - max count unique ip4 addresses
	BitmapSize    = IPv4Max / 8  // size of bit slice
	ShardCount    = 256          // 256
	ChunkSize     = 10240 * 1024 // buffer size for reading file
	MaxGoroutines = 10
)

type bitmap struct {
	shards []shard
}
type shard struct {
	mx    sync.Mutex
	slice []byte
}

func main() {
	var start = time.Now()
	var bm = newBitmap()

	filePath := "ip_addresses" //

	reader, err := mmap.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer reader.Close()
	fileSize := reader.Len()
	fmt.Printf("File size: %d bytes\n", fileSize)

	var (
		buffer         = make([]byte, ChunkSize)
		processedBytes int
		leftover       []byte
		wg             sync.WaitGroup
		sem            = make(chan struct{}, MaxGoroutines)
	)

	for offset := 0; offset < fileSize; offset += ChunkSize {
		readSize := ChunkSize
		if offset+ChunkSize > fileSize {
			readSize = fileSize - offset
		}

		_, err := reader.ReadAt(buffer[:readSize], int64(offset))
		if err != nil && err != io.EOF {
			log.Fatalf("Error reading file with mmap: %v", err)
		}

		chunk := append(leftover, buffer[:readSize]...)

		rows := bytes.Split(chunk, []byte("\n"))

		leftover = rows[len(rows)-1]

		wg.Add(1)
		sem <- struct{}{}
		go processChunk(sem, &wg, bm, rows[:len(rows)-1])

		processedBytes += readSize
		fmt.Printf("Processed: %.2f%%\n", float64(processedBytes)/float64(fileSize)*100)
	}

	wg.Add(1)
	sem <- struct{}{}
	go processChunk(sem, &wg, bm, [][]byte{leftover})

	wg.Wait()

	uniqueCount := bm.countBits()
	println("Number of unique IP addresses: ", uniqueCount)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Printf("Alloc: %v MB\n", memStats.Alloc/1024/1024)
	fmt.Printf("TotalAlloc: %v MB\n", memStats.TotalAlloc/1024/1024)
	fmt.Printf("Sys: %v MB\n", memStats.Sys/1024/1024)
	fmt.Printf("HeapAlloc: %v MB\n", memStats.HeapAlloc/1024/1024)
	fmt.Printf("HeapSys: %v MB\n", memStats.HeapSys/1024/1024)

	println("Processing time: ", time.Now().Sub(start).String())
}

func processChunk(sem chan struct{}, wg *sync.WaitGroup, bm *bitmap, rows [][]byte) {
	defer wg.Done()
	defer func() { <-sem }()

	for _, row := range rows {
		row := strings.TrimSpace(string(row))
		if row == "" {
			continue
		}

		ip, err := ipToUint32(row)
		if err != nil {
			println("Invalid IP address: ", row)
			continue
		}

		bm.setBit(ip)
	}

}

func ipToUint32(ip string) (uint32, error) {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return 0, fmt.Errorf("invalid IP address: %s", ip)
	}
	parsedIP = parsedIP.To4()
	if parsedIP == nil {
		return 0, fmt.Errorf("not an IPv4 address: %s", ip)
	}
	return uint32(parsedIP[0])<<24 | uint32(parsedIP[1])<<16 | uint32(parsedIP[2])<<8 | uint32(parsedIP[3]), nil
}

func (b *bitmap) setBit(index uint32) {
	shardIndex := index % ShardCount
	localShard := &b.shards[shardIndex]

	byteIndex := (index / 8) % (BitmapSize / ShardCount)
	bitOffset := index % 8

	localShard.mx.Lock()
	defer localShard.mx.Unlock()
	localShard.slice[byteIndex] |= 1 << bitOffset
}

func (b *bitmap) countBits() int {
	count := 0
	for _, shd := range b.shards {
		for _, byteVal := range shd.slice {
			count += bitsInByte(byteVal)
		}
	}
	return count
}

func bitsInByte(b byte) int {
	count := 0
	for b > 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}

func newBitmap() *bitmap {
	shards := make([]shard, ShardCount)
	for i := range shards {
		shards[i].slice = make([]byte, BitmapSize/ShardCount)
	}
	return &bitmap{shards: shards}
}
