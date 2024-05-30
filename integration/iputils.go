package integration

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"slices"
)

import . "github.com/magicvegetable/architecture-lab-4/err"

var (
	CurrentNetwork = ""
)

func GetLocalNetwork() (*net.IPNet, error) {
	_, localNet, err := net.ParseCIDR(CurrentNetwork)

	if err != nil {
		err = FormatError(
			err,
			"net.ParseCIDR(%#v)",
			CurrentNetwork,
		)
	}

	return localNet, err
}

func GetLocalIP() (net.IP, error) {
	localNet, err := GetLocalNetwork()

	if err != nil {
		err = FormatError(err, "GetLocalNetwork()")
		return nil, err
	}

	addrs, err := net.InterfaceAddrs()

	if err != nil {
		err = FormatError(err, "net.InterfaceAddrs()")
		return nil, err
	}

	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())

		if err != nil {
			err = FormatError(err, "net.ParseCIDR(%#v)", addr.String())
			return nil, err
		}

		if localNet.Contains(ip) {
			return ip, nil
		}
	}

	err = FormatError(err, "Local IP not found")
	return nil, err
}

type Logger struct {
	Buffer bytes.Buffer
}

func (l *Logger) Write(p []byte) (n int, err error) {
	os.Stderr.Write(p)
	return l.Buffer.Write(p)
}

func (l *Logger) Flush() []byte {
	content := l.Buffer.Bytes()
	fmt.Println(string(content))
	return content
}

var logger Logger

func RunLogCommand(exe string, args []string) ([]byte, error) {
	cmd := exec.Command(exe, args...)

	cmd.Stdout = &logger
	cmd.Stderr = &logger

	err := cmd.Run()
	res := logger.Flush()

	if err != nil {
		return res, FormatError(
			err,
			"exec.Command(%#v, %#v...).Output()",
			exe,
			args,
		)
	}

	return res, err
}

func DelCIDR(cidr, dev string) ([]byte, error) {
	args := []string{"addr", "del", cidr, "dev", dev}
	exe := "ip"
	return RunLogCommand(exe, args)
}

func AddCIDR(cidr, dev string) ([]byte, error) {
	args := []string{"addr", "add", cidr, "dev", dev}
	exe := "ip"
	return RunLogCommand(exe, args)
}

func DelAllCIDRFilter(dev string, reserved []string) ([]byte, error) {
	iface, err := net.InterfaceByName(dev)

	if err != nil {
		return nil, FormatError(err, "net.InterfaceByName(%#v)", dev)
	}

	addrs, err := iface.Addrs()

	if err != nil {
		return nil, FormatError(err, "%#v.Addrs()", iface)
	}

	res := []byte{}
	for _, addr := range addrs {
		cidr := addr.String()

		if slices.Contains(reserved, cidr) {
			continue
		}

		subres, err := DelCIDR(cidr, dev)

		if err != nil {
			return nil, FormatError(
				err,
				"DelCIDR(%#v, %#v)",
				cidr,
				dev,
			)
		}

		res = append(res, subres...)
	}

	return res, nil
}

func DelAllCIDR(dev string) ([]byte, error) {
	return DelAllCIDRFilter(dev, []string{})
}

func ChangeCIDR(cidr, dev string) ([]byte, error) {
	res, err := AddCIDR(cidr, dev)

	if err != nil {
		return nil, FormatError(
			err,
			"AddCIDR(%#v, %#v)",
			cidr,
			dev,
		)
	}

	_, err = DelAllCIDRFilter(dev, []string{cidr})

	if err != nil {
		return nil, FormatError(
			err,
			"DelAllCIDR(%#v)",
			dev,
		)
	}

	return res, err
}

func InterfaceByNetwork(ipNet *net.IPNet) (*net.Interface, error) {
	ifaces, err := net.Interfaces()

	if err != nil {
		err = FormatError(err, "net.Interfaces()")
		return nil, err
	}

	for _, iface := range ifaces {
		addrs, err := iface.Addrs()

		if err != nil {
			err = FormatError(err, "%#v.Addrs()", iface)
			return nil, err
		}

		for _, addr := range addrs {
			cidr := addr.String()
			ip, _, err := net.ParseCIDR(cidr)

			if err != nil {
				err = FormatError(err, "net.ParseCIDR(%#v)", cidr)
				return nil, err
			}

			if ipNet.Contains(ip) {
				return &iface, nil
			}
		}
	}

	err = FormatError(err, "Not found")

	return nil, err
}

func RandIP(ipNet *net.IPNet) (net.IP, error) {
	if ipNet == nil {
		return nil, FormatError(
			nil,
			"ipNet have to be not nil",
		)
	}

	ones, bits := ipNet.Mask.Size()

	freeBits := bits - ones

	if freeBits == 0 {
		return ipNet.IP, nil
	}

	bitsToSet := rand.Int() % freeBits

	ipLen := len(ipNet.IP)
	ip := make([]byte, ipLen)

	copy(ip, ipNet.IP)

	for i := 0; i < bitsToSet; i++ {
		bitPos := rand.Int() % freeBits

		byteIndex := ipLen - bitPos/8 - 1

		bit := byte(1 << (bitPos % 8))

		ip[byteIndex] |= bit
	}

	return ip, nil
}

const MAX_AMOUNT_OF_TRY = 1024

func IPsContainsIP(ips []net.IP, ip net.IP) bool {
	if ips == nil {
		return false
	}

	if ip == nil {
		return false
	}

	for _, ipsIP := range ips {
		if ipsIP == nil {
			continue
		}

		if ipsIP.String() == ip.String() {
			return true
		}
	}

	return false
}

func indexNextIP(i int, ip net.IP) net.IP {
	if ip[i] == 0xff {
		ip[i] = 0
		return indexNextIP(i-1, ip)
	}

	ip[i] += 1

	return ip
}

func nextIP(ip net.IP) net.IP {
	lastIndex := len(ip) - 1

	if ip[lastIndex] == 0xff {
		ip[lastIndex] = 0
		return indexNextIP(lastIndex, ip)
	}

	ip[lastIndex] += 1

	return ip
}

func MaxIP(ipNet *net.IPNet) net.IP {
	if ipNet == nil {
		return nil
	}

	maxIP := append(net.IP{}, ipNet.IP...)

	ones, bits := ipNet.Mask.Size()

	freeBits := bits - ones
	ipLenBytes := len(ipNet.IP)

	for i := 0; i < freeBits; i++ {
		bit := byte(1 << (i % 8))
		maxIP[ipLenBytes-i/8-1] |= bit
	}

	return maxIP
}

func GetFirstFreeIP(ipNet *net.IPNet, ips []net.IP) (net.IP, error) {
	if ipNet == nil {
		err := FormatError(nil, "ipNet have to be not %#v", ipNet)
		return nil, err
	}

	if ips == nil {
		return ipNet.IP, nil
	}

	maxIP := MaxIP(ipNet)

	ip := append(net.IP{}, ipNet.IP...)

	for IPsContainsIP(ips, ip) {
		if ip.String() == maxIP.String() {
			err := FormatError(nil, "ipNet %#v have no free ip", ipNet)
			return nil, err
		}

		ip = nextIP(ip)
	}

	return ip, nil
}

func RandIPFilter(ipNet *net.IPNet, ips []net.IP) (net.IP, error) {
	for i := 0; i < MAX_AMOUNT_OF_TRY; i++ {
		ip, err := RandIP(ipNet)

		if err != nil {
			err = FormatError(err, "RandIP(%#v)", ipNet)
			return nil, err
		}

		if !IPsContainsIP(ips, ip) {
			return ip, err
		}
	}

	return GetFirstFreeIP(ipNet, ips)
}

func IPtoCIDR(ip net.IP, mask net.IPMask) (string, error) {
	if ip == nil {
		return "", FormatError(nil, "ip have to be not nil")
	}

	if mask == nil {
		return "", FormatError(nil, "mask have to be not nil")
	}

	ones, _ := mask.Size()

	cidr := ip.String() + "/" + fmt.Sprintf("%v", ones)
	return cidr, nil
}

func RandCIDR(ipNet *net.IPNet) (string, error) {
	ip, err := RandIP(ipNet)

	if err != nil {
		err = FormatError(err, "RandIP(%#v)", ipNet)
		return "", err
	}

	cidr, err := IPtoCIDR(ip, ipNet.Mask)

	if err != nil {
		err = FormatError(err, "IPtoCIDR(%#v, %#v)", ip, ipNet.Mask)
		return "", err
	}

	return cidr, err
}

func RandCIDRFilter(ipNet *net.IPNet, cidrs []string) (string, error) {
	if ipNet == nil {
		err := FormatError(nil, "ipNet have to be not %#v", ipNet)
		return "", err
	}

	var ips []net.IP
	for _, cidr := range cidrs {
		ip, _, err := net.ParseCIDR(cidr)

		if err != nil {
			err = FormatError(err, "net.ParseCIDR(%#v)", cidr)
			return "", err
		}

		if ipNet.Contains(ip) {
			ips = append(ips, ip)
		}
	}

	ip, err := RandIPFilter(ipNet, ips)

	if err != nil {
		err = FormatError(err, "RandIPFilter(%#v, %#v)", ipNet, ips)
		return "", err
	}

	cidr, err := IPtoCIDR(ip, ipNet.Mask)

	if err != nil {
		err = FormatError(err, "IPtoCIDR(%#v, %#v)", ip, ipNet.Mask)
		return "", err
	}

	return cidr, err
}

func randIPNet(size int) *net.IPNet {
	maskOnes := rand.Int() % (size * 8)

	mask := make([]byte, size)

	for i := 0; i < maskOnes; i++ {
		bit := byte(1 << (7 - (i % 8)))

		byteIndex := i / 8

		mask[byteIndex] |= bit
	}

	ip := make([]byte, size)

	for i := 0; i < maskOnes; i++ {
		bitPos := rand.Int() % maskOnes

		byteIndex := bitPos / 8

		bit := byte(1 << (7 - (bitPos % 8)))

		ip[byteIndex] |= bit
	}

	return &net.IPNet{IP: ip, Mask: mask}
}

func RandIPNet() *net.IPNet {
	sizes := []int{16, 4}

	size := sizes[rand.Int()%len(sizes)]

	return randIPNet(size)
}

func RandIPNetVersion(version int) (*net.IPNet, error) {
	sizes := map[int]int{
		4: 4,
		6: 16,
	}

	size, contains := sizes[version]

	if !contains {
		err := FormatError(nil, "Not supported version %#v", version)
		return nil, err
	}

	return randIPNet(size), nil
}

func IPNetsIntersect(ipNet1 *net.IPNet, ipNet2 *net.IPNet) (bool, error) {
	if ipNet1 == nil {
		err := FormatError(nil, "ipNet1 have to be not %#v", ipNet1)
		return false, err
	}

	if ipNet2 == nil {
		err := FormatError(nil, "ipNet2 have to be not %#v", ipNet2)
		return false, err
	}

	ones1, bits1 := ipNet1.Mask.Size()

	ones2, bits2 := ipNet2.Mask.Size()

	if bits1 != bits2 {
		return false, nil
	}

	var lowestOnes int

	if ones1 > ones2 {
		lowestOnes = ones2
	} else {
		lowestOnes = ones1
	}

	for i := 0; i < lowestOnes/8; i++ {
		if ipNet1.IP[i] != ipNet2.IP[i] {
			return false, nil
		}
	}

	byteIndex := lowestOnes / 8
	clearBits := 8 - lowestOnes%8

	checkBits1 := ipNet1.IP[byteIndex]
	checkBits2 := ipNet2.IP[byteIndex]

	checkBits1 >>= clearBits
	checkBits1 <<= clearBits

	checkBits2 >>= clearBits
	checkBits2 <<= clearBits

	return checkBits1 == checkBits2, nil
}

func IPNetIntersectIPNets(ipNet *net.IPNet, ipNets []*net.IPNet) (bool, error) {
	if ipNet == nil {
		err := FormatError(nil, "ipNet have to be not %#v", ipNet)
		return false, err
	}

	if ipNets == nil {
		return false, nil
	}

	for _, subIPNet := range ipNets {
		intersect, err := IPNetsIntersect(subIPNet, ipNet)

		if err != nil {
			err = FormatError(err, "IPNetsIntersect(%#v, %#v)", subIPNet, ipNet)
			return false, err
		}

		if intersect {
			return intersect, err
		}
	}

	return false, nil
}

func getBitsSizeByVersion(version int) (int, error) {
	maskBits := map[int]int{
		4: 32,
		6: 128,
	}

	bits, contains := maskBits[version]

	if !contains {
		err := FormatError(nil, "Not supported version %#v", version)
		return 0, err
	}

	return bits, nil
}

func ipNetIncludes(ipNet *net.IPNet, subIPNet *net.IPNet) bool {
	if ipNet == nil || subIPNet == nil {
		return false
	}

	ipNetOnes, ipNetBits := ipNet.Mask.Size()

	subIPNetOnes, subIPNetBits := subIPNet.Mask.Size()

	if ipNetBits != subIPNetBits {
		return false
	}

	if subIPNetOnes < ipNetOnes {
		return false
	}

	for i := 0; i < ipNetOnes; i++ {
		byteI := i / 8
		bitI := 7 - i%8

		settedBit := byte(1 << bitI)

		ipNetBit := ipNet.IP[byteI]
		ipNetBit &= settedBit

		subIPNetBit := subIPNet.IP[byteI]
		subIPNetBit &= settedBit

		if ipNetBit != subIPNetBit {
			return false
		}
	}

	return true
}

func ipNetExclude(ipNet *net.IPNet, subIPNet *net.IPNet) []*net.IPNet {
	if ipNet == nil {
		return nil
	}

	if !ipNetIncludes(ipNet, subIPNet) {
		return []*net.IPNet{ipNet}
	}

	ipNetOnes, ipNetBits := ipNet.Mask.Size()

	subIPNetOnes, _ := subIPNet.Mask.Size()

	var leftIPNets []*net.IPNet

	for i := ipNetOnes; i < subIPNetOnes; i++ {
		byteI := i / 8
		bitI := 7 - i%8

		leftIP := make(net.IP, len(ipNet.IP))
		copy(leftIP, subIPNet.IP[:byteI+1])

		leftIPNet := &net.IPNet{
			IP:   leftIP,
			Mask: net.CIDRMask(i+1, ipNetBits),
		}

		settedBit := byte(1 << bitI)

		leftIPNetBit := leftIPNet.IP[byteI]
		leftIPNetBit &= settedBit

		if leftIPNetBit == 0 {
			leftIPNet.IP[byteI] += settedBit
		} else {
			leftIPNet.IP[byteI] -= settedBit
		}

		clearBits := bitI

		leftIPNet.IP[byteI] >>= clearBits
		leftIPNet.IP[byteI] <<= clearBits

		leftIPNets = append(leftIPNets, leftIPNet)
	}

	return leftIPNets
}

func FreeIPNets(version int, ipNets []*net.IPNet) ([]*net.IPNet, error) {
	bitsSize, err := getBitsSizeByVersion(version)

	if err != nil {
		err = FormatError(err, "getBitsSizeByVersion(%#v)", version)
		return nil, err
	}

	freeIPNets := []*net.IPNet{
		// the biggest net
		&net.IPNet{
			Mask: net.CIDRMask(0, bitsSize),
			IP:   make(net.IP, bitsSize/8),
		},
	}

	for _, ipNet := range ipNets {
		var filteredFreeIPNets []*net.IPNet

		for _, freeIPNet := range freeIPNets {
			if ipNetIncludes(ipNet, freeIPNet) {
				continue
			}

			if !ipNetIncludes(freeIPNet, ipNet) {
				filteredFreeIPNets = append(filteredFreeIPNets, freeIPNet)
				continue
			}

			subFreeIPNets := ipNetExclude(freeIPNet, ipNet)

			filteredFreeIPNets = append(filteredFreeIPNets, subFreeIPNets...)
		}

		freeIPNets = filteredFreeIPNets
	}

	return freeIPNets, nil
}

func RandIPNetFilterNoIntersectMinDiff(ipNets []*net.IPNet, diff int) (*net.IPNet, error) {
	for i := 0; i < MAX_AMOUNT_OF_TRY; i++ {
		randIPNet := RandIPNet()

		ones, bits := randIPNet.Mask.Size()

		if bits-ones < diff {
			continue
		}

		intersect, err := IPNetIntersectIPNets(randIPNet, ipNets)

		if err != nil {
			err = FormatError(err, "IPNetIntersectIPNets(%#v, %#v)", randIPNet, ipNets)
			return nil, err
		}

		if !intersect {
			return randIPNet, err
		}
	}

	version := []int{4, 6}[rand.Int()%2]

	freeIPNets, err := FreeIPNets(version, ipNets)

	if err != nil {
		err = FormatError(err, "freeIPNets(%#v, %#v)", version, ipNets)
		return nil, err
	}

	if len(freeIPNets) == 0 {
		return nil, err
	}

	var filteredFreeIPNets []*net.IPNet
	for _, freeIPNet := range freeIPNets {
		ones, bits := freeIPNet.Mask.Size()

		if bits-ones < diff {
			continue
		}

		filteredFreeIPNets = append(filteredFreeIPNets, freeIPNet)
	}

	return filteredFreeIPNets[rand.Int()%len(filteredFreeIPNets)], nil
}

func RandIPNetFilterNoIntersect(ipNets []*net.IPNet) (*net.IPNet, error) {
	return RandIPNetFilterNoIntersectMinDiff(ipNets, 0)
}

func RandIPNetVersionFilterNoIntersect(version int, ipNets []*net.IPNet) (*net.IPNet, error) {
	for i := 0; i < MAX_AMOUNT_OF_TRY; i++ {
		randIPNet, err := RandIPNetVersion(version)

		if err != nil {
			err = FormatError(err, "RandIPNetVersion(%#v)", version)
			return nil, err
		}

		intersect, err := IPNetIntersectIPNets(randIPNet, ipNets)

		if err != nil {
			err = FormatError(err, "IPNetIntersectIPNets(%#v, %#v)", randIPNet, ipNets)
			return nil, err
		}

		if !intersect {
			return randIPNet, err
		}
	}

	freeIPNets, err := FreeIPNets(version, ipNets)

	if err != nil {
		err = FormatError(err, "freeIPNets(%#v, %#v)", version, ipNets)
		return nil, err
	}

	if len(freeIPNets) == 0 {
		return nil, err
	}

	return freeIPNets[rand.Int()%len(freeIPNets)], nil
}
