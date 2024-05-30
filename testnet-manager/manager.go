package main

import . "github.com/magicvegetable/architecture-lab-4/integration"

import "syscall"

func main() {
	err := syscall.Mkfifo(HostFifoPath, 0o664)

	if err != nil && err != syscall.EEXIST {
		panic(err)
	}

	ManageNetwork()
}
