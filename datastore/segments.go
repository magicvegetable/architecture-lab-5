package datastore

import (
	"sync"
	"io"
	"fmt"
	"bufio"
	"os"
	"encoding/binary"
	"bytes"
	"time"
)

type Segment struct {
	Out io.ReadWriteCloser
	In io.ReadSeekCloser
	Offset int64
	Index hashIndex
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
			n int
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

	_, err := seg.In.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(seg.In)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (seg *Segment) Delete(key string) (string, error) {
	val, err := seg.Get(key)
	delete(seg.Index, key)
	return val, err
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

type OperationDeleteData OperationGetData

type OperationPutData struct {
	Key string
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

type OperationDeleteResult struct {
	Val string
	Err error
}

func (odr *OperationDeleteResult) IsValid() (bool, error) {
	if odr.Err != nil {
		return true, nil
	}
	return false, odr.Err
}

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
	Data OperationData
	Result chan OperationResult
	Type string
}

type queueElement struct {
	value *Operation
	next *queueElement
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

		<- q.blocked
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
	queue operationQueue
	terminated chan struct{}
	Handler Handler
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

	<- l.terminated
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
	loop *Loop
	stopTriggerMerge bool

	SegmentStopWriteSize *int64
	MergeWaitInterval time.Duration
	ForceMerge bool

	CreateSegment CreateSegmentFn
	GetSegments GetSegmentsFn
	ExcludeSegment func(*Segment) error
}

func NewSegmentsHandler(
	getSegFn GetSegmentsFn,
	createSegFn CreateSegmentFn,
	ssws *int64,
	mwi time.Duration,
	es func(*Segment) error,
) *SegmentsHandler {
	sh := &SegmentsHandler{
		GetSegments: getSegFn,
		CreateSegment: createSegFn,
		SegmentStopWriteSize: ssws,
		MergeWaitInterval: mwi,
		ExcludeSegment: es,
	}

	sh.loop = &Loop{Handler: sh}

	return sh
}

type OperationHandleFn func(OperationData) OperationResult

func (sh *SegmentsHandler) currentSegment() (seg *Segment, err error) {
	segments := sh.GetSegments()
	if len(segments) == 0 {
		return sh.CreateSegment()
	}

	seg = segments[len(segments) - 1]

	if seg.Offset >= *sh.SegmentStopWriteSize {
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

func (sh *SegmentsHandler) delete(op OperationData) OperationResult {
	data := op.(*OperationDeleteData)

	var (
		val string
		sval string
		err error
	)

	key := data.Key
	segments := sh.GetSegments()
	found := false
	for _, seg := range segments {
		sval, err = seg.Delete(key)

		if err != ErrNotFound && err != nil {
			break
		}

		if err != ErrNotFound {
			found = true
			val = sval
		}
	}

	if found && err == ErrNotFound {
		err = nil
	}

	if found {
		sh.ForceMerge = true
	}

	return &OperationDeleteResult{Val: val, Err: err}
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

type Reader struct {
	reader *bytes.Reader
}

func (r *Reader) Close() error { return nil }
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}
func (r *Reader) Read(p []byte) (n int, err error) { return r.reader.Read(p) }

type Buffer struct {
	buffer bytes.Buffer
}

func (b *Buffer) Close() error { return nil }
func (b *Buffer) GetReader() *Reader {
	arr := b.buffer.Bytes()
	_, err := b.buffer.Write(arr)

	if err != nil {
		return nil
	}

	reader := bytes.NewReader(arr)
	return &Reader{reader: reader}
}
func (b *Buffer) Read(p []byte) (n int, err error) { return b.buffer.Read(p) }
func (b *Buffer) Write(p []byte) (n int, err error) { return b.buffer.Write(p) }


// TODO:
// add buffer copy of segment, and try recover to previous state
// segment.CreateBufferCopy... maybe
func (sh *SegmentsHandler) merge(op OperationData) OperationResult {
	segs := sh.GetSegments()

	segLastI := len(segs) - 1
	if segLastI == 0 && sh.ForceMerge {
		err := segs[0].Overwrite(segs[0])

		if err != nil {
			fmt.Println("got a serious problem")
			fmt.Println(err)
		}
	}

	for i := segLastI; i >= 1; i-- {
		segHP := segs[i]
		segLP := segs[i - 1]

		buff := &Buffer{}

		segM := &Segment{
			Out: buff,
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

		segM.In = buff.GetReader()

		err := segLP.Overwrite(segM)
		if err != nil {
			fmt.Println("got a serious problem")
			fmt.Println(err)
		}

		err = sh.ExcludeSegment(segHP)
		if err != nil {
			fmt.Println("got a serious problem")
			fmt.Println(err)
		}
	}

	if sh.ForceMerge {
		sh.ForceMerge = false
	}
	
	return nil
}

func (sh *SegmentsHandler) OperationHandleFn(Type string) (fn OperationHandleFn, ok bool) {
	handleFns := map[string]OperationHandleFn{
		"Get": sh.get,
		"Put": sh.put,
		"Merge": sh.merge,
		"Delete": sh.delete,
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
		Data: &OperationGetData{Key: key},
		Result: make(chan OperationResult),
		Type: "Get",
	}
	sh.loop.PostOperation(op)

	res := <- op.Result

	resA := res.(*OperationGetResult)
	return resA.Val, resA.Err
}

func (sh *SegmentsHandler) Put(key, value string) error {
	op := &Operation{
		Data: &OperationPutData{Key: key, Value: value},
		Result: make(chan OperationResult),
		Type: "Put",
	}
	sh.loop.PostOperation(op)

	res := <- op.Result

	resA := res.(*OperationPutResult)
	return resA.Err
}

func (sh *SegmentsHandler) Delete(key string) (string, error) {
	op := &Operation{
		Data: &OperationDeleteData{Key: key},
		Result: make(chan OperationResult),
		Type: "Delete",
	}
	sh.loop.PostOperation(op)

	res := <- op.Result

	resA := res.(*OperationDeleteResult)

	return resA.Val, resA.Err
}

func (sh *SegmentsHandler) Sync() {
	op := &Operation{
		Result: make(chan OperationResult),
		Type: "Merge",
	}

	sh.loop.PostOperation(op)

	res := <- op.Result

	if res != nil {
		fmt.Println("expected nil...")
	}
}

func (sh *SegmentsHandler) Terminate() {
	sh.StopTriggerMerge()
	sh.Sync()
	sh.loop.Terminate()
}

func (sh *SegmentsHandler) Start() {
	sh.loop.Start()
	sh.TriggerMerge()
}

func (sh *SegmentsHandler) TriggerMerge() {
	sh.stopTriggerMerge = false

	go func() {
		for range time.Tick(sh.MergeWaitInterval) {
			if sh.stopTriggerMerge {
				return
			}
			sh.Sync()
		}
	}()
}

func (sh *SegmentsHandler) StopTriggerMerge() {
	sh.stopTriggerMerge = true
}

