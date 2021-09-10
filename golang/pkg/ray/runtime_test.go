package ray

import "testing"

func init() {
    RegisterActorType((*TestActor)(nil))
}

type TestActor struct {
}

func (c *TestActor) Init() {

}

func TestGetActorType(t *testing.T) {
    goType := GetActorType("github.com/ray-project/ray-go-worker/pkg/ray.TestActor")
    if goType == nil {
        t.FailNow()
    }
    t.Logf("type: %s", goType.Name())
}
