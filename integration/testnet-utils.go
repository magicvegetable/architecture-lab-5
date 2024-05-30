package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"slices"
	"time"
)

var (
	RunningContainers = map[string]string{
		"architecture-lab-4-balancer-1": "balancer",
		"architecture-lab-4-test-1":     "",
	}

	TestNetwork  = "architecture-lab-4_testlan"
	FifoPath     = "/fifo"
	HostFifoPath = "./fifo"
)

func NamesOfContainers() []string {
	var names []string
	for name, _ := range RunningContainers {
		names = append(names, name)
	}

	return names
}

func RunCommand(exe string, args []string) error {
	cmd := exec.Command(exe, args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func DisconnectContainers(network string, containers []string) error {
	exe := "docker"
	args := []string{"network", "disconnect", network}

	for _, container := range containers {
		err := RunCommand(exe, append(args, container, "--force"))

		if err != nil {
			return err
		}
	}

	return nil
}

func ConnectContainer(network, container, alias string) error {
	exe := "docker"
	args := []string{"network", "connect", network}

	if alias != "" {
		args = append(args, "--alias", alias)
	}

	args = append(args, container)

	err := RunCommand(exe, args)

	if err != nil {
		return err
	}

	return nil
}

func ConnectContainers(network string, containers map[string]string) error {
	for container, alias := range containers {
		err := ConnectContainer(network, container, alias)

		if err != nil {
			return err
		}
	}

	return nil
}

func RemoveNetwork(network string) error {
	exe := "docker"
	args := []string{"network", "rm", network}

	err := RunCommand(exe, args)

	if err != nil {
		return err
	}

	return nil
}

func CreateNetwork(network, cidr string) error {
	exe := "docker"
	args := []string{"network", "create"}

	_, ipNet, err := net.ParseCIDR(cidr)

	if err != nil {
		return err
	}

	if ipNet.IP.To4() == nil {
		args = append(args, "--ipv6")
	}

	ones, bits := ipNet.Mask.Size()

	if bits-ones > 1 {
		ipNet.IP[len(ipNet.IP)-1] += 1
		args = append(args, "--gateway", ipNet.IP.String())
	}

	args = append(args, "--subnet", cidr, network)

	err = RunCommand(exe, args)

	if err != nil {
		return err
	}

	return nil
}

type TestMessage struct {
	Cidr         string
	KillYourself bool
}

func UpdateTestNetwork(cidr string) error {
	fileF, err := os.OpenFile(FifoPath, os.O_WRONLY, 0o644)

	if err != nil {
		return err
	}

	defer fileF.Close()

	msg, err := json.Marshal(TestMessage{Cidr: cidr})

	if err != nil {
		return err
	}

	_, err = fileF.Write(msg)

	fmt.Println("Wrote...")

	if err != nil {
		return err
	}

	return nil
}

func KillTestNetworkHostMonitor() error {
	fileF, err := os.OpenFile(FifoPath, os.O_WRONLY, 0o644)

	if err != nil {
		return err
	}

	defer fileF.Close()

	msg, err := json.Marshal(TestMessage{KillYourself: true})

	if err != nil {
		return err
	}

	_, err = fileF.Write(msg)

	if err != nil {
		return err
	}

	return nil
}

func HostUpdateTestNetwork(cidr string) error {
	containersNames := NamesOfContainers()

	err := DisconnectContainers(TestNetwork, containersNames)

	if err != nil {
		println(err)
	}

	err = RemoveNetwork(TestNetwork)

	if err != nil {
		println(err)
	}

	err = CreateNetwork(TestNetwork, cidr)

	if err != nil {
		println(err)
	}

	err = ConnectContainers(TestNetwork, RunningContainers)

	if err != nil {
		println(err)
	}

	return nil
}

func ManageNetwork() {
	fileF, err := os.OpenFile(HostFifoPath, os.O_RDONLY, 0o644)

	fmt.Println(err)
	if err != nil {
		println(err)
	}

	defer fileF.Close()

	reader := bufio.NewReader(fileF)

	for range time.Tick(time.Second) {
		buff := make([]byte, reader.Size())

		_, err := reader.Read(buff)

		if err != nil {
			if err == io.EOF {
				fmt.Println("nope...")
				continue
			}

			println(err)
		}

		res := TestMessage{}

		buff = buff[:slices.Index(buff, 0)]
		err = json.Unmarshal(buff, &res)

		if err != nil {
			println(err)
		}

		if res.KillYourself == true {
			break
		}

		fmt.Println("Trying update to", res)

		HostUpdateTestNetwork(res.Cidr)
	}
}
