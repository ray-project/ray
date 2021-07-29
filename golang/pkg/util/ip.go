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
            ipnet, ok := addr.(*net.IPNet)
            if !ok {
                continue
            }
            v4 := ipnet.IP.To4()
            if v4 == nil || v4[0] == 127 { // loopback address
                continue
            }
            return v4.String(), nil
        }
    }
    return "", fmt.Errorf("Failed to get ip")
}
