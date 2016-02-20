/* Golang program to test simple disk performance
Scenario:
1 create file with preallocated space
4 write to random places
read from random places
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	DevUrandom    = "/dev/urandom"
	FillBlockSize = 1024 * 1024
)

type DiskTester interface {
	FillWithZeros(string) (time.Duration, error)
	WriteAt(name string, offset int64, buffer []byte) (time.Duration, error)
	ReadFrom(name string, offset int64, buffer []byte) (time.Duration, error)
}

type DiskTest struct {
	FilePath       string
	BlockSize      int64
	FileSizeBlocks int64
	FileSize       int64
	Writer         bool
	writePatter    []byte
}

func NewDiskTest(fp string, fs, bs int64, writer bool) (*DiskTest, error) {
	devUrand, err := os.Open(DevUrandom)
	defer devUrand.Close()
	if err != nil {
		return nil, err
	}
	wP := make([]byte, bs)
	_, err = devUrand.Read(wP)
	if err != nil {
		return nil, err
	}
	return &DiskTest{
		FilePath:       fp,
		FileSize:       fs,
		BlockSize:      bs,
		FileSizeBlocks: fs / bs,
		Writer:         writer,
		writePatter:    wP,
	}, err
}

func (d *DiskTest) Init() error {
	f, err := os.Create(d.FilePath)
	if err != nil {
		return err
	}
	err = f.Truncate(int64(d.FileSize))
	return err
}

func (d *DiskTest) FillWithZeros() (time.Duration, error) {
	buffer := make([]byte, d.BlockSize)
	return d.fill(buffer)
}

func (d *DiskTest) FillWithRandom() (time.Duration, error) {
	return d.fill(d.writePatter)
}

func (d *DiskTest) fill(buffer []byte) (time.Duration, error) {
	blockSize := d.BlockSize
	if d.FileSizeBlocks < FillBlockSize {
		factor := int64(FillBlockSize) / d.BlockSize
		rest := FillBlockSize - factor*d.BlockSize
		var newBuffer []byte
		for i := 0; i < int(factor)-1; i++ {
			newBuffer = append(newBuffer, buffer...)
		}
		if rest > 0 {
			newBuffer = append(newBuffer, buffer[:rest]...)
		}
		blockSize = FillBlockSize
		buffer = newBuffer
	}
	numIters := d.FileSize / blockSize
	start := time.Now()
	f, err := os.OpenFile(d.FilePath, os.O_WRONLY, 0666)
	defer f.Close()
	if err != nil {
		return time.Since(start), err
	}
	for i := 0; i < int(numIters); i++ {
		_, err := f.Write(buffer)
		if err != nil {
			return time.Since(start), err
		}
		_, err = f.Seek(blockSize, 1)
		if err != nil {
			return time.Since(start), err
		}
	}
	f.Seek(0, 0)
	err = f.Sync()
	return time.Since(start), err
}

func testOp(name string, offset int64, buffer []byte, write bool) (time.Duration, error) {
	start := time.Now()
	f, err := os.OpenFile(name, os.O_RDWR|syscall.O_DIRECT, 0666)
	defer f.Close()
	if err != nil {
		return time.Since(start), err
	}
	if write {
		_, err = f.WriteAt(buffer, offset)
	} else {
		_, err = f.ReadAt(buffer, offset)
	}
	if err != nil {
		return time.Since(start), err
	}
	if write {
		err = f.Sync()
	}
	return time.Since(start), err
}

type TestResults struct {
	SingleResults     []time.Duration
	TotalTime         time.Duration
	AverageTime       time.Duration
	MaxTime           time.Duration
	MinTime           time.Duration
	StDev             time.Duration
	AverageThroughput float64
	Writer            bool
}

func (t *TestResults) ComputeTestResults(blockSize int64) {
	if len(t.SingleResults) == 0 {
		return
	}
	t.MinTime = time.Duration(math.MaxInt64)
	t.MaxTime = time.Duration(0)
	for _, d := range t.SingleResults {
		t.TotalTime += d
		if t.MinTime > d {
			t.MinTime = d
		}
		if t.MaxTime < d {
			t.MaxTime = d
		}
	}
	t.AverageTime = t.TotalTime / time.Duration(len(t.SingleResults))
	deviations := make([]time.Duration, len(t.SingleResults))
	var sumDeviations uint64
	for i, d := range t.SingleResults {
		dev := d - t.AverageTime
		deviations[i] = dev * dev
		sumDeviations += uint64(deviations[i])
	}
	t.StDev = time.Duration(int64(math.Sqrt(float64(sumDeviations) / float64(len(t.SingleResults)))))
	if blockSize > 0 {
		totalData := float64(blockSize * int64(len(t.SingleResults)))
		totalSeconds := float64(t.TotalTime.Nanoseconds()) / 1000000000.0
		t.AverageThroughput = totalData / totalSeconds / (1024.0 * 1024.0)
	}
}

func (t *TestResults) AddResults(n *TestResults) {
	t.SingleResults = append(t.SingleResults, n.SingleResults...)
}

func (d *DiskTest) Run(results chan TestResults, numIter uint, seq int) {
	//log.Printf("Running test: fn: %v, fs: %v, bs: %v", d.FilePath, d.FileSize, d.BlockSize)
	r := rand.New(rand.NewSource(int64(seq)))
	offsets := make([]int64, numIter)
	var res TestResults
	for i := 0; i < int(numIter); i++ {
		offsets[i] = r.Int63n(d.FileSizeBlocks) * d.BlockSize
		//log.Printf("Worker: %v, iteration: %v, offset: %v\n", seq, i, offsets[i])
	}
	if d.Writer {
		for _, o := range offsets {
			sRes, err := testOp(d.FilePath, o, d.writePatter, true)
			if err != nil {
				log.Fatalf("Error writing to %v at offset %v worker %v, %v", d.FilePath, o, seq, err)
			}
			res.SingleResults = append(res.SingleResults, sRes)
		}
	} else {
		for _, o := range offsets {
			sRes, err := testOp(d.FilePath, o, d.writePatter, false)
			if err != nil {
				log.Fatalf("Error reading from %v at offset %v worker %v, %v", d.FilePath, o, seq, err)
			}
			res.SingleResults = append(res.SingleResults, sRes)
		}
	}
	res.Writer = d.Writer
	res.ComputeTestResults(d.BlockSize)
	results <- res
}

var (
	FileName   = flag.String("file_name", "/var/tmp/test.bin", "File name and path for test")
	FileSize   = flag.Int64("file_size", 1024, "File size in MB")
	BlockSize  = flag.Int64("block_size", 4096, "Block size in bytes")
	NumWriters = flag.Int("num_writers", 4, "Number of writers")
	NumReaders = flag.Int("num_readers", 4, "Number of readers")
	NumIters   = flag.Uint("num_iterations", 1000, "Number of iterations")
)

func wait(wg *sync.WaitGroup, results chan TestResults) {
	defer close(results)
	wg.Wait()
}

func main() {
	flag.Parse()
	*FileSize = 1024 * 1024 * *FileSize
	d, err := NewDiskTest(*FileName, *FileSize, *BlockSize, true)
	if err != nil {
		log.Fatalf("Error creating tester %v", err)
	}
	d.Init()
	res, err := d.FillWithRandom()
	tMs := int64(res.Nanoseconds() / 1000000)
	transfer := float64(*FileSize) / float64(tMs) / 1000
	fmt.Printf("Wrote %v bytes in %v miliseconds, throughput %v MB/s, err : %v\n", *FileSize, tMs, transfer, err)
	results := make(chan TestResults)
	var wg sync.WaitGroup
	for i := 0; i < *NumWriters; i++ {
		writer, err := NewDiskTest(*FileName, *FileSize, *BlockSize, true)
		if err != nil {
			log.Fatalf("Error creating writer: %v\n", err)
		}
		wg.Add(1)
		go func(results chan TestResults, NumIters uint, i int) {
			writer.Run(results, NumIters, i)
			wg.Done()
		}(results, *NumIters, i)
	}
	for i := 0; i < *NumReaders; i++ {
		reader, err := NewDiskTest(*FileName, *FileSize, *BlockSize, false)
		if err != nil {
			log.Fatalf("Error creating reader: %v\n", err)
		}
		wg.Add(1)
		go func(results chan TestResults, NumIters uint, i int) {
			reader.Run(results, NumIters, i+10000)
			wg.Done()
		}(results, *NumIters, i+10000)
	}
	log.Println("All threads started")
	var globalWriteResults TestResults
	var globalReadResults TestResults
	go wait(&wg, results)
	for t := range results {
		fmt.Printf("Writer: %v Total: %v Average: %v StDev: %v NrOfProbes: %v Throughput: %vMB/s \n", t.Writer, t.TotalTime, t.AverageTime, t.StDev, len(t.SingleResults), t.AverageThroughput)
		if t.Writer {
			globalWriteResults.AddResults(&t)
		} else {
			globalReadResults.AddResults(&t)
		}
	}
	fmt.Println("Global results")
	fmt.Println("Write:")
	globalWriteResults.ComputeTestResults(*BlockSize)
	fmt.Printf("\tTotal time: %v, NrOfProbes: %v, Average: %v, StDev: %v\n",
		globalWriteResults.TotalTime,
		len(globalWriteResults.SingleResults),
		globalWriteResults.AverageTime,
		globalWriteResults.StDev,
	)
	fmt.Println("Read:")
	globalReadResults.ComputeTestResults(*BlockSize)
	fmt.Printf("\tTotal time: %v, NrOfProbes: %v, Average: %v, StDev: %v\n",
		globalReadResults.TotalTime,
		len(globalReadResults.SingleResults),
		globalReadResults.AverageTime,
		globalReadResults.StDev,
	)

}
