package datastore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"bytes"
	"time"
	"slices"
)

const (
	outFileName = "current-data"
	headerSize = 4
	SegmentFilePerm = os.FileMode(0o600)
	DefaultMergeWaitInterval = time.Second
	DefaultSegmentSizeLimit = 500
)

type CustomReadWriteCloser struct {
	Buffer bytes.Buffer
}

func (crwc *CustomReadWriteCloser) Write(p []byte) (n int, err error) {
	return crwc.Buffer.Write(p)
}

func (crwc *CustomReadWriteCloser) Read(p []byte) (n int, err error) {
	return crwc.Buffer.Read(p)
}

func (crwc *CustomReadWriteCloser) Close() (err error) { return }

func FileOnLimit(name string, lim int64) (bool, error) {
	if name == "" {
		err := fmt.Errorf("no given file name")
		return false, err
	}

	stat, err := os.Stat(name)

	if err != nil {
		return false, err
	}

	return stat.Size() >= lim, nil
}

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	Segments []*Segment
	SegmentsDir string
	SegmentSizeLimit int64
	SegmentsHandler *SegmentsHandler
	MergeWaitInterval time.Duration
}

func NewDbFull(dir string, mwi time.Duration, ssl int64) (*Db, error) {
	db := &Db{
		SegmentsDir: dir,
		SegmentSizeLimit: ssl,
		MergeWaitInterval: mwi,
	}

	err := db.Recover()
	if err != nil {
		return nil, err
	}

	db.SegmentsHandler = NewSegmentsHandler(
		func() []*Segment { return db.Segments },
		db.NewSegment,
		&db.SegmentSizeLimit,
		db.MergeWaitInterval,
		db.ExcludeSegment,
	)

	db.SegmentsHandler.Start()

	return db, nil
}

func NewDb(dir string) (*Db, error) {
	return NewDbFull(dir, DefaultMergeWaitInterval, DefaultSegmentSizeLimit)
}

func (db *Db) NewSegment() (*Segment, error) {
	index := len(db.Segments)
	name := fmt.Sprintf("%v-%v", filepath.Join(db.SegmentsDir, outFileName), index)

	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	fr, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	seg := &Segment{
		Out: f,
		In: fr,
		Index: make(hashIndex),
		OutName: f.Name(),
	}

	err = seg.Recover()

	if err != io.EOF {
		return nil, err
	}

	db.Segments = append(db.Segments, seg)

	return seg, nil
}

func (db *Db) GetSegment() (*Segment, error) {
	if len(db.Segments) == 0 {
		return db.NewSegment()
	}

	last := db.Segments[len(db.Segments) - 1]

	OnLimit, err := FileOnLimit(last.OutName, db.SegmentSizeLimit)
	if err != nil {
		return nil, err
	}

	if !OnLimit {
		return last, nil
	}

	return db.NewSegment()
}

func (db *Db) IsSegmentFileName(name string) bool {
	re := regexp.MustCompile(`(?m)\b.+-\d+\b`)
	return re.MatchString(name)
}

func (db *Db) Recover() error {
	entries, err := os.ReadDir(db.SegmentsDir)

	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()

		if entry.IsDir() || !db.IsSegmentFileName(name) {
			continue
		}

		fullName := filepath.Join(db.SegmentsDir, name)

		segF, err := os.OpenFile(fullName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}

		segRF, err := os.Open(fullName)
		if err != nil {
			return err
		}

		seg := &Segment{
			Out: segF,
			In: segRF,
			Index: make(hashIndex),
			OutName: segF.Name(),
		}

		err = seg.Recover()

		// TODO: fix this, must give nil if is ok
		if err != io.EOF {
			return err
		}

		db.Segments = append(db.Segments, seg)
	}

	return nil
}

func (db *Db) Close() error {
	db.SegmentsHandler.Terminate()

	for _, seg := range db.Segments {
		err := seg.Out.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Db) Get(key string) (string, error) {
	return db.SegmentsHandler.Get(key)
}

func (db *Db) Put(key, value string) error {
	return db.SegmentsHandler.Put(key, value)
}

func (db *Db) Delete(key string) (string, error) {
	return db.SegmentsHandler.Delete(key)
}

func (db *Db) ExcludeSegment(seg *Segment) error {
	i := slices.Index(db.Segments, seg)
	if i == -1 {
		return nil
	}

	after := db.Segments[i + 1:]
	db.Segments = append(db.Segments[:i], after...)

	return os.Remove(seg.OutName)
}
