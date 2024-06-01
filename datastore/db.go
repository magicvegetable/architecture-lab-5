package datastore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	outFileName              = "current-data"
	headerSize               = 4
	SegmentFilePerm          = os.FileMode(0o600)
	DefaultMergeWaitInterval = time.Second
	DefaultSegmentSizeLimit  = 500
)

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	Segments          []*Segment
	SegmentsDir       string
	SegmentSizeLimit  int64
	SegmentsHandler   *SegmentsHandler
	MergeWaitInterval time.Duration
}

func NewDbFull(dir string, mwi time.Duration, ssl int64) (*Db, error) {
	db := &Db{
		SegmentsDir:       dir,
		SegmentSizeLimit:  ssl,
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

func (db *Db) FreeSegmentOutName() (string, error) {
	dirEntries, err := os.ReadDir(db.SegmentsDir)
	if err != nil {
		return "", err
	}

	biggestI := 0
	for _, entry := range dirEntries {
		name := entry.Name()
		if !db.IsSegmentFileName(name) {
			continue
		}

		i := strings.LastIndex(name, "-")
		num, _ := strconv.Atoi(name[i+1:])

		if biggestI <= num {
			biggestI = num + 1
		}
	}

	segOutName := fmt.Sprintf(
		"%v-%v",
		filepath.Join(db.SegmentsDir, outFileName),
		biggestI,
	)

	return segOutName, nil
}

func (db *Db) NewSegment() (*Segment, error) {
	name, err := db.FreeSegmentOutName()

	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	fr, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	seg := &Segment{
		Out:     f,
		In:      fr,
		Index:   make(hashIndex),
		OutName: f.Name(),
	}

	err = seg.Recover()

	if err != io.EOF {
		return nil, err
	}

	db.Segments = append(db.Segments, seg)

	return seg, nil
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
			Out:     segF,
			In:      segRF,
			Index:   make(hashIndex),
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

	after := db.Segments[i+1:]
	db.Segments = append(db.Segments[:i], after...)

	return os.Remove(seg.OutName)
}
