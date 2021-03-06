package ringbuffer

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func NewRingBuffer(size int) *RingBuffer {
	p := &RingBuffer{}
	p.init(size)
	return p
}

//BufferId is the id of a buffer
type BufferId uint64

// RingBuffer is goroutine-safe cycle buffer.
// It is designed as busy share buffer with lots of readers and writers.
// RingBuffer must runs under parallelism mode(runtime.GOMAXPROCS >= 4).
// It enables enable real-parallel R/W on busy shared buffers.
// see:
//   http://ifeve.com/ringbuffer
//   http://mechanitis.blogspot.com/2011/06/dissecting-disruptor-whats-so-special.html
type RingBuffer struct {
	debug      bool
	totalWait  int64
	size       int        // buffer size, readonly
	waitReadR  *sync.Cond // waitlist that are wating read
	waitWriteR *sync.Cond // waitlist that are wating write
	waitReadC  *sync.Cond // waitlist that are wating read commit
	waitWriteC *sync.Cond // waitlist that are wating write commit
	rReserve   uint64     // Read reserve, mutable
	rCommit    uint64     // Read commit, mutable
	wReserve   uint64     // Write reserve, mutable
	wCommit    uint64     // Write commit, mutable
}

func (rb *RingBuffer) Debug(enable bool) {
	rb.debug = enable
}

func (rb *RingBuffer) log(name string) func() {
	start := time.Now()
	//fmt.Printf("%s %s start\n", start, name)
	deferFun := func() {
		end := time.Now()
		cost := end.Sub(start)
		totalCost := atomic.AddInt64(&rb.totalWait, int64(cost))
		fmt.Printf("%s %s end, cost=%s totalCost=%s\n", start, name, cost, time.Duration(totalCost))
	}
	return deferFun
}

// Init ringbuffer with size.
// It is not goroutine-safe.
// RingBuffer must runs under parallelism mode(runtime.GOMAXPROCS >= 4).
func (rb *RingBuffer) init(size int) error {
	const need_cpus = 4
	if runtime.GOMAXPROCS(0) < need_cpus {
		return fmt.Errorf("RingBuffer: requires parallelism(runtime.GOMAXPROCS >= %d)", need_cpus)
	}
	if size <= 0 {
		return fmt.Errorf("RingBuffer: invalid size %d", size)
	}
	rb.size = size
	rb.waitReadR = sync.NewCond(new(sync.Mutex))
	rb.waitWriteR = sync.NewCond(new(sync.Mutex))
	rb.waitReadC = sync.NewCond(new(sync.Mutex))
	rb.waitWriteC = sync.NewCond(new(sync.Mutex))
	return nil
}

// Size return size of ringbuffer
func (rb *RingBuffer) Size() int {
	return rb.size
}

// BufferIndex returns logic index of buffer by id
func (rb *RingBuffer) BufferIndex(id uint64) int {
	return int(id % uint64(rb.size))
}

func (rb *RingBuffer) Show() string {
	return fmt.Sprintf("%s rR=%d rC=%d wR=%d wC=%d",
		time.Now().Format("2006-01-02T15:04:05.999999999"),
		atomic.LoadUint64(&rb.rReserve),
		atomic.LoadUint64(&rb.rCommit),
		atomic.LoadUint64(&rb.wReserve),
		atomic.LoadUint64(&rb.wCommit),
	)
}

// ReserveWrite returns next avable id for write.
// It will wait if ringbuffer is full.
// It is goroutine-safe.
func (rb *RingBuffer) ReserveWrite(wid int) (id uint64) {
	id = atomic.AddUint64(&rb.wReserve, 1) - 1

	if rb.debug {
		fn := rb.log("ReserveWrite")
		defer fn()
	}

	try := 0
	for {
		try++
		if rb.debug {
			fmt.Printf("ReserveWrite try=%d wid=%d %s\n", try, wid, rb.Show())
		}

		dataStart := atomic.LoadUint64(&rb.rCommit)
		maxW := dataStart + uint64(rb.size)
		if id < maxW { //no conflict, reserve ok
			break
		}

		//buffer full, wait as writer in order to awake by another reader
		rb.waitWriteR.L.Lock()
		rb.waitWriteR.Wait()
		rb.waitWriteR.L.Unlock()
	}

	return
}

// CommitWrite commit writer event for id.
// It will wait if previous writer id havn't commit.
// It will awake on reader wait list after commit OK.
// It is goroutine-safe.
func (rb *RingBuffer) CommitWrite(wid int, id uint64) {
	newId := id + 1

	if rb.debug {
		fn := rb.log("CommitWrite")
		defer fn()
	}

	try := 0
	for {
		try++
		if rb.debug {
			fmt.Printf("CommitWrite try=%d wid=%d %s\n", try, wid, rb.Show())
		}

		if atomic.CompareAndSwapUint64(&rb.wCommit, id, newId) { //commit OK
			rb.waitReadR.Broadcast()  //wakeup reader
			rb.waitWriteC.Broadcast() //wakeup write committer
			break
		}

		//commit fail, wait as reader in order to wakeup by another writer
		rb.waitWriteC.L.Lock()
		rb.waitWriteC.Wait()
		rb.waitWriteC.L.Unlock()
	}
}

// ReserveRead returns next avable id for read.
// It will wait if ringbuffer is empty.
// It is goroutine-safe.
func (rb *RingBuffer) ReserveRead(wid int) (id uint64) {
	id = atomic.AddUint64(&rb.rReserve, 1) - 1

	if rb.debug {
		fn := rb.log("ReserveRead")
		defer fn()
	}

	try := 0
	for {
		try++
		if rb.debug {
			fmt.Printf("ReserveRead try=%d wid=%d %s\n", try, wid, rb.Show())
		}

		w := atomic.LoadUint64(&rb.wCommit)
		if id < w { //no conflict, reserve ok
			break
		}

		//buffer empty, wait as reader in order to wakeup by another writer
		rb.waitReadR.L.Lock()
		rb.waitReadR.Wait()
		rb.waitReadR.L.Unlock()
	}

	return
}

// CommitRead commit reader event for id.
// It will wait if previous reader id havn't commit.
// It will awake on writer wait list after commit OK.
// It is goroutine-safe.
func (rb *RingBuffer) CommitRead(wid int, id uint64) {
	newId := id + 1

	if rb.debug {
		fn := rb.log("CommitRead")
		defer fn()
	}

	try := 0
	for {
		try++
		if rb.debug {
			fmt.Printf("CommitRead try=%d wid=%d %s\n", try, wid, rb.Show())
		}

		if atomic.CompareAndSwapUint64(&rb.rCommit, id, newId) {
			rb.waitWriteR.Broadcast() //wakeup writer
			rb.waitReadC.Broadcast()  //wakeup read committer
			break
		}

		//commit fail, wait as writer in order to wakeup by another reader
		rb.waitReadC.L.Lock()
		rb.waitReadC.Wait()
		rb.waitReadC.L.Unlock()
	}
}
