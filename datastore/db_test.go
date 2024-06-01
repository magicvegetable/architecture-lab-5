package datastore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	parallelAmount = 100
)

func putGetTest(db *Db, pair []string, t *testing.T) {
	err := db.Put(pair[0], pair[1])
	if err != nil {
		t.Errorf("Cannot put %s: %s", pair[0], err)
	}
	value, err := db.Get(pair[0])
	if err != nil {
		t.Errorf("Cannot get %s: %s", pair[0], err)
	}
	if value != pair[1] {
		t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
	}
}

func deleteTest(db *Db, pair []string, t *testing.T) {
	value, err := db.Delete(pair[0])
	if err != nil {
		t.Errorf("Cannot delete %s: %s", pair[0], err)
	}
	if value != pair[1] {
		t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
	}
}

func deleteNothingTest(db *Db, pair []string, t *testing.T) {
	_, err := db.Delete(pair[0])
	if err != ErrNotFound {
		t.Errorf("Bad error returned expected %#v, got %#v", ErrNotFound, err)
	}
}

func TestDb_Put(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	// NOTE: don't use defer db.Close()
	// or crazy things could happen

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}
	_, err = db.NewSegment()
	if err != nil {
		t.Fatal(err)
	}

	outFile, err := os.Open(filepath.Join(dir, outFileName+"-0"))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			putGetTest(db, pair, t)
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	// NOTE: be careful with printf...
	waitTime := 160 * time.Millisecond
	t.Run("merge", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		filesAmount := 100

		db, err = NewDbFull(dir, waitTime, size1)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < filesAmount-1; i++ {
			for _, pair := range pairs {
				err := db.Put(pair[0], pair[1])
				if err != nil {
					t.Errorf("Cannot put %s: %s", pair[0], err)
				}
			}
		}

		dirEntries, err := os.ReadDir(dir)

		if err != nil {
			t.Errorf("Cannot read dir %#v", dirEntries)
		}
		if len(dirEntries) != filesAmount {
			t.Errorf(
				"Unexpected amount of files before merge: got %v, expected %v",
				len(dirEntries),
				filesAmount,
			)
		}
		time.Sleep(waitTime + 80*time.Millisecond)

		dirEntries, err = os.ReadDir(dir)

		if err != nil {
			t.Errorf("Cannot read dir %#v", dirEntries)
		}
		if len(dirEntries) != 1 {
			t.Errorf(
				"Unexpected amount of files after merge: got %v, expected %v",
				len(dirEntries),
				1,
			)
		}

		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}

		size := outInfo.Size()
		if size != size1 {
			t.Errorf(
				"Unexpected size of file after merge: got %v, expected %v",
				size,
				size1,
			)
		}
	})

	t.Run("delete", func(t *testing.T) {
		for _, pair := range pairs {
			deleteTest(db, pair, t)
		}
		time.Sleep(waitTime + 80*time.Millisecond)

		outInfo, err = outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if outInfo.Size() != 0 {
			t.Errorf("expected size %v, got %v", 0, outInfo.Size())
		}
	})

	t.Run("delete nothing", func(t *testing.T) {
		for _, pair := range pairs {
			deleteNothingTest(db, pair, t)
		}
	})

	t.Run("parallel", func(t *testing.T) {
		launchWait := make(chan struct{})
		testWait := make(chan struct{})

		testsLeft := parallelAmount
		var testM sync.Mutex

		for i := 0; i < parallelAmount; i++ {
			go func() {
				pair := []string{
					fmt.Sprintf("key%v", i),
					fmt.Sprintf("value%v", i),
				}

				<-launchWait

				putGetTest(db, pair, t)
				deleteTest(db, pair, t)
				deleteNothingTest(db, pair, t)

				testM.Lock()
				testsLeft -= 1
				if testsLeft == 0 {
					close(testWait)
				}
				testM.Unlock()
			}()
		}
		close(launchWait)
		<-testWait
	})

	db.Close()
}
