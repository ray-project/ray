package ray

import "testing"

func TestGetType(t *testing.T) {
    typeValue, ok := typesMap["github.com/ray-project/ray-go-worker/pkg/actor.Count"]
    if !ok {
        t.FailNow()
    }
    t.Log("type: %s", typeValue.Name())
}
