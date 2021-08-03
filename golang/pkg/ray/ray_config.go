package ray

type rayConfig struct {
    address            string
    password           string
    nodeManagerPort    int32
    nodeManagerAddress string
    objectStoreSocket  string
    rayletSocket       string
    sessionDir         string
    jobId              int
}

var rayConfigVaue = rayConfig{}

func SetAddress(address string) {
    rayConfigVaue.address = address
}

func GetAddress() string {
    return rayConfigVaue.address
}

func SetPassword(password string) {
    rayConfigVaue.password = password
}

func GetPassword() string {
    return rayConfigVaue.password
}

func SetNodeManagerPort(nodeManagerPort int32) {
    rayConfigVaue.nodeManagerPort = nodeManagerPort
}

func GetNodeManagerPort() int32 {
    return rayConfigVaue.nodeManagerPort
}

func SetObjectStoreSocket(objectStoreSocket string) {
    rayConfigVaue.objectStoreSocket = objectStoreSocket
}

func GetObjectStoreSocket() string {
    return rayConfigVaue.objectStoreSocket
}

func SetRayletSocket(rayletSocket string) {
    rayConfigVaue.rayletSocket = rayletSocket
}

func GetRayletSocket() string {
    return rayConfigVaue.rayletSocket
}

func SetNodeManagerAddress(nodeManagerAddress string) {
    rayConfigVaue.nodeManagerAddress = nodeManagerAddress
}

func GetNodeManagerAddress() string {
    return rayConfigVaue.nodeManagerAddress
}

func SetSessionDir(sessionDir string) {
    rayConfigVaue.sessionDir = sessionDir
}

func GetSessionDir() string {
    return rayConfigVaue.sessionDir
}

func SetJobId(jobId int) {
    rayConfigVaue.jobId = jobId
}

func GetJobId() int {
    return rayConfigVaue.jobId
}
