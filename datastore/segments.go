package datastore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Segment struct {
	Out     io.ReadWriteCloser
	Offset  int64
	Index   hashIndex
	OutName string
}

const bufSize = 8192

func (seg *Segment) Recover() error {
	input, err := os.Open(seg.OutName)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			seg.Index[e.key] = seg.Offset
			seg.Offset += int64(n)
		}
	}
	return err
}

func (seg *Segment) Close() error {
	return seg.Out.Close()
}

func (seg *Segment) Get(key string) (string, error) {
	position, ok := seg.Index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(seg.OutName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (seg *Segment) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := seg.Out.Write(e.Encode())

	if err == nil {
		seg.Index[key] = seg.Offset
		seg.Offset += int64(n)
	}
	return err
}

func (seg *Segment) Overwrite(oseg *Segment) error {
	err := os.Truncate(seg.OutName, 0)
	if err != nil {
		return err
	}

	seg.Index = make(hashIndex)
	seg.Offset = 0

	for key, _ := range oseg.Index {
		val, err := oseg.Get(key)
		if err != nil {
			return err
		}

		err = seg.Put(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

type OperationData any

type OperationGetData struct {
	Key string
}

type OperationPutData struct {
	Key   string
	Value string
}

type OperationResult interface {
	IsValid() (bool, error)
}

type OperationErrorResult struct {
	Err error
	// NOTE:
	// be careful with this one
	OperationResult
}

func (oer *OperationErrorResult) IsValid() (bool, error) {
	if oer.Err == nil {
		return true, nil
	}
	return false, oer.Err
}

type OperationPutResult OperationErrorResult

type OperationGetResult struct {
	Val string
	Err error
}

func (sgr *OperationGetResult) IsValid() (bool, error) {
	if sgr.Err != nil {
		return false, sgr.Err
	}
	return true, nil
}

type Operation struct {
	Data   OperationData
	Result chan OperationResult
	Type   string
}

type queueElement struct {
	value *Operation
	next  *queueElement
}

type operationQueue struct {
	m sync.Mutex

	head, tail *queueElement

	blocked chan struct{}

	terminate bool
}

func (q *operationQueue) Push(op *Operation) {
	defer q.m.Unlock()
	q.m.Lock()

	element := &queueElement{value: op, next: nil}
	if q.head == nil {
		q.head = element
		q.tail = element
	} else {
		q.tail.next = element
		q.tail = element
	}

	if q.blocked != nil {
		close(q.blocked)
		q.blocked = nil
	}
}

func (q *operationQueue) Pull() *Operation {
	defer q.m.Unlock()
	q.m.Lock()

	if q.terminate {
		return nil
	}

	if q.head == nil {
		q.blocked = make(chan struct{})
		q.m.Unlock()

		<-q.blocked
		q.m.Lock()

		if q.terminate {
			return nil
		}
	}

	op := q.head.value
	q.head = q.head.next
	return op
}

type Handler interface {
	Handle(*Operation)
}

type Loop struct {
	queue      operationQueue
	terminated chan struct{}
	Handler    Handler
}

func (l *Loop) Start() {
	go func() {
		l.terminated = make(chan struct{})
		l.queue.terminate = false

		for {
			op := l.queue.Pull()

			if op == nil {
				break
			}

			l.Handler.Handle(op)
		}

		close(l.terminated)
	}()
}

func (l *Loop) Terminate() {
	l.queue.m.Lock()

	l.queue.terminate = true

	if l.queue.blocked != nil {
		close(l.queue.blocked)
		l.queue.blocked = nil
	}

	l.queue.m.Unlock()

	<-l.terminated
}

func (l *Loop) PostOperation(op *Operation) {
	l.queue.Push(op)
}

func (l *Loop) PostOperations(ops []*Operation) {
	for _, op := range ops {
		l.PostOperation(op)
	}
}

type CreateSegmentFn func() (*Segment, error)
type GetSegmentsFn func() []*Segment

type SegmentsHandler struct {
	loop                 *Loop
	SegmentStopWriteSize *int64
	CreateSegment        CreateSegmentFn
	GetSegments          GetSegmentsFn
	stopTriggerMerge     bool
	MergeWaitInterval    time.Duration
}

func NewSegmentsHandler(getSegFn GetSegmentsFn, createSegFn CreateSegmentFn, ssws *int64, mwi time.Duration) *SegmentsHandler {
	sh := &SegmentsHandler{
		GetSegments:          getSegFn,
		CreateSegment:        createSegFn,
		SegmentStopWriteSize: ssws,
		MergeWaitInterval:    mwi,
	}

	sh.loop = &Loop{Handler: sh}

	return sh
}

type OperationHandleFn func(OperationData) OperationResult

func (sh *SegmentsHandler) currentSegment() (seg *Segment, err error) {
	segments := sh.GetSegments()
	seg = segments[len(segments)-1]

	if seg.Offset > *sh.SegmentStopWriteSize {
		seg, err = sh.CreateSegment()
	}

	return
}

func (sh *SegmentsHandler) put(op OperationData) OperationResult {
	data := op.(*OperationPutData)

	seg, err := sh.currentSegment()

	if err != nil {
		return &OperationPutResult{Err: err}
	}

	err = seg.Put(data.Key, data.Value)

	return &OperationPutResult{Err: err}
}

func (sh *SegmentsHandler) get(op OperationData) OperationResult {
	data := op.(*OperationGetData)

	var (
		val string
		err error
	)
	key := data.Key
	segments := sh.GetSegments()
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]

		val, err = seg.Get(key)

		if err == nil {
			break
		}
	}

	return &OperationGetResult{Val: val, Err: err}
}

type Buffer struct {
	buffer bytes.Buffer
}

func (b *Buffer) Close() error                      { return nil }
func (b *Buffer) Read(p []byte) (n int, err error)  { return b.buffer.Read(p) }
func (b *Buffer) Write(p []byte) (n int, err error) { return b.buffer.Write(p) }

// TODO:
// add buffer copy of segment, and try recover to previous state
// segment.CreateBufferCopy... maybe
func (sh *SegmentsHandler) merge(op OperationData) OperationResult {
	segs := sh.GetSegments()

	for i := len(segs) - 1; i >= 1; i++ {
		segHP := segs[i]
		segLP := segs[i-1]

		buff := &Buffer{}

		segM := &Segment{
			Out:   buff,
			Index: make(hashIndex),
		}

		skip := false
		for key, _ := range segHP.Index {
			val, err := segHP.Get(key)
			if err != nil {
				skip = true
				break
			}

			err = segM.Put(key, val)
			if err != nil {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		for key, _ := range segLP.Index {
			_, contains := segM.Index[key]
			if contains {
				continue
			}

			val, err := segLP.Get(key)
			if err != nil {
				skip = true
				break
			}

			err = segM.Put(key, val)
			if err != nil {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		err := segHP.Overwrite(segM)
		if err != nil {
			println("got a serious problem")
			println(err.Error())
		}
	}

	return nil
}

func (sh *SegmentsHandler) OperationHandleFn(Type string) (fn OperationHandleFn, ok bool) {
	handleFns := map[string]OperationHandleFn{
		"Get":   sh.get,
		"Put":   sh.put,
		"Merge": sh.merge,
	}

	fn, ok = handleFns[Type]
	return
}

func (sh *SegmentsHandler) Handle(op *Operation) {
	if op == nil {
		fmt.Printf("Unexpected: %#v got op == %#v\n", sh, op)
		return
	}

	if op.Result == nil {
		fmt.Printf("Unexpected: %#v got op.Result == %#v\n", sh, op.Result)
		return
	}

	handleFn, present := sh.OperationHandleFn(op.Type)
	if !present {
		err := fmt.Errorf("Error: %#v got no OperationHandleFn for type: %#v", sh, op.Type)
		op.Result <- &OperationErrorResult{Err: err}
		return
	}

	res := handleFn(op.Data)

	op.Result <- res
	close(op.Result)
}

func (sh *SegmentsHandler) Get(key string) (value string, err error) {
	op := &Operation{
		Data:   &OperationGetData{Key: key},
		Result: make(chan OperationResult),
		Type:   "Get",
	}
	sh.loop.PostOperation(op)

	res := <-op.Result

	resA := res.(*OperationGetResult)
	return resA.Val, resA.Err
}

func (sh *SegmentsHandler) Put(key, value string) error {
	op := &Operation{
		Data:   &OperationPutData{Key: key, Value: value},
		Result: make(chan OperationResult),
		Type:   "Put",
	}
	sh.loop.PostOperation(op)

	res := <-op.Result

	resA := res.(*OperationPutResult)
	return resA.Err
}

func (sh *SegmentsHandler) Terminate() {
	sh.StopTriggerMerge()
	sh.loop.Terminate()
}

func (sh *SegmentsHandler) Start() {
	sh.loop.Start()
	sh.TriggerMerge()
}

func (sh *SegmentsHandler) TriggerMerge() {
	go func() {
		sh.stopTriggerMerge = false

		for range time.Tick(sh.MergeWaitInterval) {
			if sh.stopTriggerMerge {
				break
			}

			op := &Operation{
				Result: make(chan OperationResult),
				Type:   "Merge",
			}

			sh.loop.PostOperation(op)

			res := <-op.Result

			if res != nil {
				println("expected nil...")
			}
		}
	}()
}

func (sh *SegmentsHandler) StopTriggerMerge() {
	sh.stopTriggerMerge = true
}
