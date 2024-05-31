package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"
	"fmt"
	"io"
	"strings"

	"github.com/magicvegetable/architecture-lab-4/httptools"
	"github.com/magicvegetable/architecture-lab-4/signal"
)

var (
	port = flag.Int("port", 8080, "server port")
	dbAddrHost = flag.String("db-addr-base", "localhost:8070", "db host addr")
)

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure = "CONF_HEALTH_FAILURE"
)

func main() {
	flag.Parse()

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

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		values, ok := r.URL.Query()["key"]
		if r.Method == "GET" && ok {
			value := strings.Join(values, "")
			urlS := fmt.Sprintf("http://%s/db/%v", *dbAddrHost, value)

			resp, err := http.DefaultClient.Get(urlS)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				_, _ = rw.Write([]byte("500 internal server error"))
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				rw.WriteHeader(resp.StatusCode)
				return
			}

			rw.Header().Set("Content-Type", resp.Header.Get("Content-Type"))

			_, err = io.Copy(rw, resp.Body)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"1", "2",
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
