package util

import (
    "fmt"
    "net"
)

func GetLocalIp() (string, error) {
    ifaces, err := net.Interfaces()
    if err != nil {
        return "", err
    }
    for _, i := range ifaces {
        addrs, _ := i.Addrs()
        for _, addr := range addrs {
            var ip net.IP
            switch v := addr.(type) {
            case *net.IPNet:
                ip = v.IP
            case *net.IPAddr:
                ip = v.IP
            }
            if ip.String() == "127.0.0.1" {
                continue
            }
            return ip.String(), nil
        }
    }
    return "", fmt.Errorf("Failed to get ip")
}
