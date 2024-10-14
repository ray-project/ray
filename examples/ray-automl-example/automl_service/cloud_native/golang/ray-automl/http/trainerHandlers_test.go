package http

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestRestServer_trainerCreateV1(t *testing.T) {
	dataStr := "{\n  \"image\": \"gcr.io/distroless/static:debug\",\n  \"name\": \"trainer-sample\",\n  \"namespace\": \"ray-automl-system\",\n  \"proxyName\": \"proxy-sample\",\n  \"startParams\": {\n    \"grpc-port\": \"2345\"\n  },\n  \"workers\": {\n    \"group1\": {\n      \"cpu\": \"500m\",\n      \"memory\": \"500Mi\",\n      \"disk\": \"500Mi\"\n    },\n    \"group2\": {\n      \"cpu\": \"500m\",\n      \"memory\": \"500Mi\",\n      \"disk\": \"500Mi\"\n    }\n  }\n}"

	data := []byte(dataStr)
	trainerStartReq := &TrainerStartReq{}
	_ = json.Unmarshal(data, trainerStartReq)
	t.Logf(fmt.Sprintf("%v", trainerStartReq))
}
