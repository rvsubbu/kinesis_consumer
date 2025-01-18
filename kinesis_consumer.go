package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz/lzma"
)

const (
	// common
	region = "us-east-1"
	// region = "us-west-2"
	streamName = "my.kinesis.stream" // Update with your Kinesis stream name
	shardID = "shardId-000000000000" // Replace with your actual shard ID
	shardIteratorType = types.ShardIteratorTypeTrimHorizon
	// shardIteratorType = types.ShardIteratorTypeLatest
)

var count int64

func basicTest() {
	var testStr = `
Four score and seven years ago our fathers brought forth on this continent, a new nation, conceived in Liberty, and dedicated to the proposition that all men are created equal.

Now we are engaged in a great civil war, testing whether that nation, or any nation so conceived and so dedicated, can long endure. We are met on a great battle-field of that war. We have come to dedicate a portion of that field, as a final resting place for those who here gave their lives that that nation might live. It is altogether fitting and proper that we should do this.

But, in a larger sense, we can not dedicate—we can not consecrate—we can not hallow—this ground. The brave men, living and dead, who struggled here, have consecrated it, far above our poor power to add or detract. The world will little note, nor long remember what we say here, but it can never forget what they did here. It is for us the living, rather, to be dedicated here to the unfinished work which they who fought here have thus far so nobly advanced. It is rather for us to be here dedicated to the great task remaining before us—that from these honored dead we take increased devotion to that cause for which they gave the last full measure of devotion—that we here highly resolve that these dead shall not have died in vain—that this nation, under God, shall have a new birth of freedom—and that government of the people, by the people, for the people, shall not perish from the earth.

—Abraham Lincoln
`
	zstdEnc, err := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		fmt.Println("zstd encoder could not be created, basic test could not be run, error", err)
		return
	}
	zstdDec, _ := zstd.NewReader(nil)
	var compressedData, decompressedData []byte
	compressedData = zstdEnc.EncodeAll([]byte(testStr), nil)
	decompressedData, _ = zstdDec.DecodeAll(compressedData, nil)
	fmt.Println("testStr == decompressedData", testStr == string(decompressedData))
	fmt.Println("decompressedData", string(decompressedData))
	fmt.Println("compressedData magic byte", compressedData[0], compressedData[1], compressedData[2], compressedData[3])
}

func lz4Decompress(compressedData []byte) (decompressedData []byte, err error) {
	decompressedData = make([]byte, len(compressedData)*10)
	decompressedSize, err := lz4.UncompressBlock(compressedData, decompressedData)
	if err != nil {
		// fmt.Printf("Decompression error: %v\n", err)
		return
	}
	return decompressedData[:decompressedSize], err
}

func gzipDecompress(compressedData []byte) (decompressedData []byte, err error) {
	b := bytes.NewBuffer(compressedData)

	var r io.Reader
	r, err = gzip.NewReader(b)
	if err != nil {
		return
	}

	var resB bytes.Buffer
	_, err = resB.ReadFrom(r)
	if err != nil {
		return
	}

	decompressedData = resB.Bytes()
	return
}

func lzmaDecompress(compressedData []byte) ([]byte, error) {
	reader := bytes.NewReader(compressedData)
	lzmaReader, err := lzma.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create LZMA reader: %w", err)
	}
	// defer lzmaReader.Close()

	// Create a buffer to store the decompressed data
	var decompressedData bytes.Buffer

	// Decompress the data
	_, err = io.Copy(&decompressedData, lzmaReader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress lzma data: %w", err)
	}

	return decompressedData.Bytes(), nil
}

func zstdDecompress(compressedData []byte) ([]byte, error) {
	zstdDec, _ := zstd.NewReader(nil)

	// This is a hack, just traverse the byte stream until we find the zstd magic number sequence
	var start int
	for i, b := range compressedData {
		if i < 3 {
			continue
		}
		if compressedData[i-3] == 0x28 {
			if compressedData[i-2] == 0xB5 {
				if compressedData[i-1] == 0x2F {
					if b == 0xFD {
						start = i-3
						fmt.Println("\tzstd found at", i)
						break
					}
				}
			}
		}
	}
	return zstdDec.DecodeAll(compressedData[start:len(compressedData)-16], nil)
}

// Check if data is likely Zstd-compressed by checking for the magic bytes.
func isZstdCompressed(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

func processKinesisRecords(client *kinesis.Client) {
	// Get a shard iterator
	shardIteratorResp, err := client.GetShardIterator(context.TODO(), &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: shardIteratorType,
	})
	if err != nil {
		panic(fmt.Sprintf("Unable to get shard iterator: %v", err))
	}

	shardIterator := shardIteratorResp.ShardIterator

	// Fetch records from the stream
	for {
		// Get records from the Kinesis stream
		resp, err := client.GetRecords(context.TODO(), &kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit: aws.Int32(100),
		})
		if err != nil {
			panic(fmt.Sprintf("Failed to fetch records from Kinesis: %v", err))
		}

		// Process each record
		for _, record := range resp.Records {
			atomic.AddInt64(&count, 1)
			fmt.Println("message #", atomic.LoadInt64(&count))
			fmt.Printf("\tcompressed message len %d\n", len(record.Data))
			// fmt.Println("\tzstd compression", isZstdCompressed(record.Data))

			var err error
			var decompressedData []byte
			if decompressedData, err = zstdDecompress(record.Data); err != nil {
				fmt.Printf("\tzstd decompression didn't work, err=%+v, assuming no compression\n", err)
				// This is a hack, just traverse the byte stream until we hit a starting brace "{" char
				var start int
				for i, b := range record.Data {
					if b == '{' {
						start = i
						break
					}
				}
				decompressedData = record.Data[start:len(record.Data)-16]
				fmt.Println("\tno compression")
				err = nil
			}
			fmt.Println("\tDecompressed message", string(decompressedData))
		}

		// Update the shard iterator for the next call
		shardIterator = resp.NextShardIterator
	}
}

func main() {
	basicTest()

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}

	// Create a Kinesis client
	client := kinesis.NewFromConfig(cfg)

	// Start processing records from Kinesis
	processKinesisRecords(client)
}
