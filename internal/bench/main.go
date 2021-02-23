package main

import (
	"fmt"
	"ringbuffer"
	"runtime"
	"sync"
	"time"
)

var (
	bufferSize  = 1000
	maxId       = uint64(bufferSize * 1000)
	workerCount = 10
	wg          sync.WaitGroup
	rb          = ringbuffer.NewRingBuffer(bufferSize)
	debug       = true
)

func main() {
	start := time.Now()
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	fmt.Printf("%s start, cpus=%d workerCount=%d bufferSize=%d maxId=%d\n", start, cpus, workerCount, bufferSize, maxId)
	rb.Debug(debug)
	wg.Add(workerCount * 2)
	for i := 0; i < workerCount; i++ {
		go writer(i + 1)
		go reader(i + 1)
	}
	wg.Wait()
	end := time.Now()
	cost := end.Sub(start)
	expect := (time.Duration(maxId) + time.Duration(workerCount)) * 2 * time.Millisecond / 10
	fmt.Printf("%s~%s cpus=%d cost %s/%s\n", start, end, cpus, cost, expect)
}

func delay(id int) {
	x := 0
	for i := 0; i < id; i++ {
		for j := 0; j < id; j++ {
			for k := 0; k < id; k++ {
				x *= (i + 2) * (j + 2) * (k + 2)
			}
		}
	}
}

func reader(wid int) {
	for {
		if debug {
			fmt.Printf("reader wid=%d try hold\n", wid)
		}
		id := rb.ReserveR(wid)
		if debug {
			fmt.Printf("reader wid=%d hold %d\n", wid, id)
		}
		delay(wid)
		if debug {
			fmt.Printf("reader wid=%d try commit %d\n", wid, id)
		}
		rb.CommitR(wid, id)
		if debug {
			fmt.Printf("reader wid=%d commit %d\n", wid, id)
		}
		if id > maxId {
			break
		}
	}
	wg.Done()
}

func writer(wid int) {
	for {
		if debug {
			fmt.Printf("writer wid=%d try hold\n", wid)
		}
		id := rb.ReserveW(wid)
		if debug {
			fmt.Printf("writer wid=%d hold %d\n", wid, id)
		}

		delay(wid)
		if debug {
			fmt.Printf("writer wid=%d try commit %d\n", wid, id)
		}
		rb.CommitW(wid, id)
		if debug {
			fmt.Printf("writer wid=%d commit %d\n", wid, id)
		}
		if id > maxId {
			break
		}
	}
	wg.Done()
}
