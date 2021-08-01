package ray

type rayConfig struct {
    sessionDir string
    jobId      int
}

func (c *rayConfig) SetSessionDir(sessionDir string) {
    c.sessionDir = sessionDir
}

func (c *rayConfig) SetJobId(jobId int) {
    c.jobId = jobId
}

func NewRayConfig() *rayConfig {
    //configPath := os.Getenv("ray.config-file")
    //if configPath == "" {
    //
    //} else {
    //
    //}
    return &rayConfig{}
}
