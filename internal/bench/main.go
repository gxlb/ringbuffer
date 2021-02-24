package main

import (
	"fmt"
	"ringbuffer"
	"runtime"
	"sync"
	"time"
)

var (
	bufferSize  = 100 - 1
	maxId       = uint64(bufferSize * 1000)
	workerCount = 5
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
	fmt.Printf("%s~%s cpus=%d workerCount=%d bufferSize=%d maxId=%d cost %s/%s\n", start, end, cpus, workerCount, bufferSize, maxId, cost, expect)
}

func delay(id int) {
	return
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
			//fmt.Printf("reader wid=%d try hold\n", wid)
		}
		id := rb.ReserveRead(wid)
		if debug {
			//fmt.Printf("reader wid=%d hold %d\n", wid, id)
		}
		delay(wid)
		if debug {
			//fmt.Printf("reader wid=%d try commit %d\n", wid, id)
		}
		rb.CommitRead(wid, id)
		if debug {
			//fmt.Printf("reader wid=%d commit %d\n", wid, id)
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
			//fmt.Printf("writer wid=%d try hold\n", wid)
		}
		id := rb.ReserveWrite(wid)
		if debug {
			//fmt.Printf("writer wid=%d hold %d\n", wid, id)
		}

		delay(wid)
		if debug {
			//fmt.Printf("writer wid=%d try commit %d\n", wid, id)
		}
		rb.CommitWrite(wid, id)
		if debug {
			//fmt.Printf("writer wid=%d commit %d\n", wid, id)
		}
		if id > maxId {
			break
		}
	}
	wg.Done()
}
