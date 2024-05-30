package integration

import . "github.com/magicvegetable/architecture-lab-4/err"
import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	ReadDeadline                    = 10 * time.Millisecond
	HttpBalancerTestsAmount         = 3
	HttpBalancerChecksPerTestAmount = 3
	CopyBufferSize                  = 128
	AmountOfChangeIP                = 3
	randomCIDRTestAmount            = 10
	MaxAttemptsToConnect            = 200
	connWaitTime                    = time.Second
	MaxAttemptsToGetInterface       = 20
	MaxAttemptsToGetBalancerIP      = 20
	BalancerPort                    = 8090
)

var BaseAddress = "http://balancer:8090"

func Copy(out io.Writer, connection net.Conn) error {
	size := CopyBufferSize
	buffer := make([]byte, size)

	for {
		deadline := time.Now().Add(ReadDeadline)
		connection.SetReadDeadline(deadline)
		_, err := connection.Read(buffer)

		if err != nil {
			if err != io.EOF && time.Now().Before(deadline) {
				return FormatError(
					err,
					"%v.Read(%v)",
					connection,
					buffer,
				)
			}

			break
		}

		_, err = out.Write(buffer)

		if err != nil {
			return FormatError(
				err,
				"%v.Write(%v)",
				out,
				buffer,
			)
		}

		buffer = make([]byte, size)
	}

	return nil
}

func ReadResponse(url *url.URL, connection net.Conn) (*http.Response, error) {
	buffer := &bytes.Buffer{}

	err := Copy(buffer, connection)

	if err != nil {
		return nil, FormatError(
			err,
			"Copy(%v, %v)",
			buffer,
			connection,
		)
	}

	req, err := http.NewRequest("GET", url.String(), nil)

	if err != nil {
		return nil, FormatError(
			err,
			"http.NewRequest(\"GET\", %v, nil)",
			url.String(),
		)
	}

	resp, err := http.ReadResponse(bufio.NewReader(buffer), req)

	if err != nil {
		return nil, FormatError(
			err,
			"http.ReadResponse(%v, %v)",
			bufio.NewReader(buffer),
			req,
		)
	}

	return resp, nil
}

func GetLbfrom(url *url.URL, connection net.Conn) (string, error) {
	requestStr := "GET " + url.Path + " HTTP/1.1\r\n" + "Host: " + url.Hostname() + "\r\n\r\n"
	_, err := connection.Write([]byte(requestStr))

	if err != nil {
		return "", FormatError(
			err,
			"%v.Write([]byte(%v))",
			connection,
			requestStr,
		)
	}

	resp, err := ReadResponse(url, connection)

	if err != nil {
		return "", FormatError(
			err,
			"ReadResponse(%v, %v)",
			url,
			connection,
		)
	}

	if resp.StatusCode != 200 {
		return "", FormatError(
			fmt.Errorf("%v", requestStr),
			"http.Response.StatusCode == %v for request",
			resp.StatusCode,
		)
	}

	return resp.Header.Get("Lb-from"), nil
}

func GetBalancerIP(ipNet *net.IPNet) (net.IP, error) {
	var i int

	for range time.Tick(time.Second) {
		fmt.Println("try resolve...")
		i += 1

		addrs, err := net.LookupHost("balancer")

		if i > MaxAttemptsToGetBalancerIP {
			break
		}

		if err != nil {
			continue
		}

		for _, addr := range addrs {
			ip := net.ParseIP(addr)

			if ipNet.Contains(ip) {
				fmt.Println("resolved!")
				return ip, nil
			}
		}

	}

	err := FormatError(nil, "Not balancer ip for network %#v", ipNet)
	return nil, err
}

func GetInterface(ipNet *net.IPNet) (iface *net.Interface, err error) {
	var i int

	for range time.Tick(time.Second) {
		iface, err = InterfaceByNetwork(ipNet)

		if err != nil && i > MaxAttemptsToGetInterface {
			err = FormatError(err, "InterfaceByNetwork(%v)", ipNet)
			return
		}

		if err == nil {
			break
		}

		i += 1
	}

	return
}

func ConnectBalancer() (conn net.Conn, err error) {
	ipNet, err := GetLocalNetwork()
	if err != nil {
		err = FormatError(err, "GetLocalNetwork()")
		return
	}

	balancerIP, err := GetBalancerIP(ipNet)
	if err != nil {
		err = FormatError(err, "GetBalancerIP(%#v)", ipNet)
		return
	}

	iface, err := GetInterface(ipNet)
	if err != nil {
		err = FormatError(err, "GetInterface(%#v)", ipNet)
		return
	}

	var address string

	if balancerIP.To4() == nil {
		address = fmt.Sprintf("[%v%%%v]:%v", balancerIP.String(), iface.Name, BalancerPort)
	} else {
		address = fmt.Sprintf("%v:%v", balancerIP.String(), BalancerPort)
	}

	network := "tcp"
	for i := 0; i < MaxAttemptsToConnect; i++ {
		fmt.Println("trying connect...")
		conn, err = net.DialTimeout(network, address, connWaitTime)

		if err != nil {
			continue
		}

		fmt.Println("connected!")
		return
	}

	return
}

func balancerHttpGetTest(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	urlStr := BaseAddress + "/api/v1/some-data"
	url, err := url.Parse(urlStr)

	if err != nil {
		err = FormatError(err, "url.Parse(%v)", urlStr)
		panic(err)
	}

	var testsM sync.Mutex
	testsLeft := HttpBalancerTestsAmount

	wait := make(chan struct{})

	for i := 0; i < HttpBalancerTestsAmount; i++ {
		go func() {
			network := "tcp"
			connection, err := ConnectBalancer()

			if err != nil {
				err = FormatError(err, "Connect(%#v, %#v)", network, url)
				panic(err)
			}

			lbfrom, err := GetLbfrom(url, connection)

			if err != nil {
				err = FormatError(err, "GetLbfrom(%#v, %#v)", url, connection)
				panic(err)
			}

			addr := connection.LocalAddr().String()
			t.Run("address: "+addr, func(t *testing.T) {
				for i := 0; i < HttpBalancerChecksPerTestAmount; i++ {
					nextLbfrom, err := GetLbfrom(url, connection)

					if err != nil {
						err = FormatError(err, "GetLbfrom(%v, %v)", url, connection)
						panic(err)
					}

					assert.Equal(t, lbfrom, nextLbfrom, "answer from the same server")
				}
			})

			testsM.Lock()

			testsLeft -= 1

			if testsLeft == 0 {
				close(wait)
			}

			testsM.Unlock()

			connection.Close()
		}()
	}

	<-wait
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	client := http.Client{Timeout: 3 * time.Second}

	urlBalancer := fmt.Sprintf("%s/api/v1/some-data", BaseAddress)
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(urlBalancer)
		if err != nil {
			err = FormatError(
				err,
				"%#v.Get(%#v)",
				client,
				urlBalancer,
			)

			b.Error(err)
		}

		if resp.StatusCode != 200 {
			bodyBytes, err := io.ReadAll(resp.Request.Body)

			if err != nil {
				err = FormatError(
					err,
					"io.ReadAll(%#v)",
					resp.Request.Body,
				)
				panic(err)
			}

			err = FormatError(
				fmt.Errorf("%#v", string(bodyBytes)),
				"%#v.StatusCode != %#v for request",
				resp,
				resp.StatusCode,
			)
			b.Error(err)
		}

		resp.Body.Close()
	}
}

func localIPNetTest(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	ipNet, err := GetLocalNetwork()

	if err != nil {
		err = FormatError(err, "GetLocalNetwork()")
		panic(err)
	}

	iface, err := GetInterface(ipNet)
	if err != nil {
		if strings.Contains(err.Error(), "Not found") { // skip...
			return
		}

		err = FormatError(err, "InterfaceByNetwork(%v)", ipNet)
		panic(err)
	}

	balancerIP, err := GetBalancerIP(ipNet)

	if err != nil {
		err = FormatError(err, "GetBalancerIP()")
		panic(err)
	}

	filterIPsBase := []net.IP{balancerIP, ipNet.IP}
	for i := 0; i < AmountOfChangeIP; i++ {
		localIP, err := GetLocalIP()

		if err != nil {
			err = FormatError(err, "GetLocalIP()")
			panic(err)
		}

		filterIPs := append(filterIPsBase, localIP)

		ip, err := RandIPFilter(ipNet, filterIPs)

		if err != nil {
			err = FormatError(err, "RandIPFilter(%#v, %#v)", ipNet, filterIPs)
			panic(err)
		}

		cidr, err := IPtoCIDR(ip, ipNet.Mask)

		if err != nil {
			err = FormatError(err, "IPtoCIDR(%#v, %#v)", ip, ipNet.Mask)
			panic(err)
		}

		_, err = ChangeCIDR(cidr, iface.Name)

		if err != nil {
			err = FormatError(err, "ChangeIpAddr(%#v, %#v)", cidr, iface.Name)
			panic(err)
		}

		t.Run("CIDR: "+cidr, balancerHttpGetTest)
	}
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	cidrs := []string{
		"fc00::/64",
		"fe81::/64",
		"2085:0DAA::/112",
		"3333:0D:1234:f8f8::/64",
		"192.13.0.0/16",
		"12.13.0.0/16",
		"19.33.0.0/24",
		"56.13.0.0/20",
		"192.18.0.0/16",
	}

	for _, cidr := range cidrs {
		if t.Failed() {
			return
		}

		t.Run("cidr: "+cidr, func(t *testing.T) {
			CurrentNetwork = cidr
			err := UpdateTestNetwork(cidr)

			if err != nil {
				err = FormatError(err, "UpdateTestNetwork(%#v)", cidr)
				panic(err)
			}

			localIPNetTest(t)
		})
	}

	reservedCIDRs := []string{
		"2001:0DB8::/120",
		"127.0.0.0/8",
		"::1/128",
		"172.17.0.0/16",
		"224.0.0.0/4",
		"fe80::/64",
		"ff00::0/64",
	}

	var reservedIPNets []*net.IPNet

	for _, reservedCIDR := range reservedCIDRs {
		_, ipNet, err := net.ParseCIDR(reservedCIDR)

		if err != nil {
			err = FormatError(err, "net.ParseCIDR(%#v)", reservedCIDR)
			panic(err)
		}

		reservedIPNets = append(reservedIPNets, ipNet)
	}

	for i := 0; i < randomCIDRTestAmount; i++ {
		if t.Failed() {
			return
		}

		ipNet, err := RandIPNetFilterNoIntersectMinDiff(reservedIPNets, 4)

		if err != nil {
			err = FormatError(err, "RandIPNetFilterNoIntersect(%#v)", reservedIPNets)
			panic(err)
		}

		cidr := ipNet.String()

		t.Run("cidr: "+cidr, func(t *testing.T) {
			CurrentNetwork = cidr
			err := UpdateTestNetwork(cidr)

			if err != nil {
				err = FormatError(err, "UpdateTestNetwork(%#v)", cidr)
				panic(err)
			}

			localIPNetTest(t)
		})
	}

	err := KillTestNetworkHostMonitor()

	if err != nil {
		err = FormatError(err, "KillTestNetworkHostMonitor()")
		panic(err)
	}
}
