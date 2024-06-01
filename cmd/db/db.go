package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"time"

	"github.com/magicvegetable/architecture-lab-5/datastore"
	"github.com/magicvegetable/architecture-lab-5/httptools"
	"github.com/magicvegetable/architecture-lab-5/signal"
)

var (
	port  = flag.Int("port", 8070, "server port")
	DbDir = flag.String("db-dir", "", "db store directory")
)

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure    = "CONF_HEALTH_FAILURE"
	TeamName             = "phantoms"
)

func main() {
	flag.Parse()
	var (
		dir string
		err error
	)

	if *DbDir == "" {
		dir, err = os.MkdirTemp("", "test-db")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
	} else {
		dir = *DbDir
	}

	db, err := datastore.NewDb(dir)
	if err != nil {
		panic(err)
	}

	err = db.Put(TeamName, time.Now().Format(time.DateOnly))
	if err != nil {
		panic(err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		key, _ := strings.CutPrefix(r.URL.Path, "/db/")

		if r.Method == "GET" {
			value, err := db.Get(key)

			if err != nil {
				rw.WriteHeader(http.StatusNotFound)
				return
			}

			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(rw).Encode(map[string]string{
				key: value,
			})

			return
		}

		if r.Method == "POST" {
			reqM := make(map[string]string)

			err := json.NewDecoder(r.Body).Decode(&reqM)
			if err != nil {
				fmt.Println(err)
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write([]byte("400 bad request"))
				return
			}

			value, ok := reqM["value"]
			if !ok {
				fmt.Println(reqM)
				rw.WriteHeader(http.StatusBadRequest)
				_, _ = rw.Write([]byte("400 bad request"))
				return
			}

			err = db.Put(key, value)
			if err != nil {
				fmt.Println(err)
				rw.WriteHeader(http.StatusInternalServerError)
				_, _ = rw.Write([]byte("500 internal server error"))
				return
			}

			rw.Header().Set("content-type", "application/json")
			rw.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(rw).Encode(map[string]string{
				key: value,
			})
			return
		}

		rw.WriteHeader(http.StatusBadRequest)
		_, _ = rw.Write([]byte("400 bad request"))
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()

	db.Close()
}
