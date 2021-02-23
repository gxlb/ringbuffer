package main

import (
	"fmt"
	"ringbuffer"
	"runtime"
	"sync"
	"time"
)

var (
	bufferSize  = 10
	maxId       = uint64(bufferSize * 100)
	workerCount = 5
	wg          sync.WaitGroup
	rb          = ringbuffer.NewRingBuffer(bufferSize)
)

func main() {
	start := time.Now()
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	fmt.Printf("%s start, cpus=%d workerCount=%d bufferSize=%d maxId=%d\n", start, cpus, workerCount, bufferSize, maxId)
	rb.Debug(true)
	wg.Add(workerCount * 2)
	for i := 0; i < workerCount; i++ {
		go writer(i + 1)
		go reader(i + 1)
	}
	wg.Wait()
	end := time.Now()
	cost := end.Sub(start)
	expect := (time.Duration(maxId) + time.Duration(workerCount)) * 2 * time.Millisecond
	fmt.Printf("%s~%s cpus=%d cost %s/%s\n", start, end, cpus, cost, expect)
}

func delay(id int) {
	x := 0
	for i := 0; i < id; i++ {
		for j := 0; j < id; j++ {
			for k := 0; k < id; k++ {
				x += (i + 2) * (j + 2) * (k + 2)
			}
		}
	}
}

func reader(wid int) {
	for {
		id := rb.ReserveR()
		fmt.Printf("reader %d hold %d\n", wid, id)
		delay(wid)
		rb.CommitR(id)
		fmt.Printf("reader %d commit %d\n", wid, id)
		if id > maxId {
			break
		}
	}
	wg.Done()
}

func writer(wid int) {
	for {
		id := rb.ReserveW()
		fmt.Printf("writer %d hold %d\n", wid, id)
		delay(wid)
		rb.CommitW(id)
		fmt.Printf("writer %d commit %d\n", wid, id)
		if id > maxId {
			break
		}
	}
	wg.Done()
}
