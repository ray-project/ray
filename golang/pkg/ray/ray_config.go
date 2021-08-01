package ray

import "os"

type RayConfig struct {
}

func NewRayConfig() {
    configPath := os.Getenv("ray.config-file")
    if configPath == "" {

    } else {

    }
}
