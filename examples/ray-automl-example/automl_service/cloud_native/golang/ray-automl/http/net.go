package http

import (
	"errors"
	"net"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var logger = logf.Log.WithName("net-util")

func GetLocalIPAddrV4() string {
	localIp, err := LocalIPAddrV4()
	if err != nil {
		logger.Error(err, "Failed to get localIPAddrV4 address")
		return ""
	}
	return localIp
}

// LocalIPAddrV4 returns a active network interface's ipV4 address. Returns the first found one.
// Returns an error if not found.
func LocalIPAddrV4() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addressed, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addressed {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}

	return "", errors.New("can't found local active network interface")
}
