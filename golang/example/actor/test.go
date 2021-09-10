package actor

import (
    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

func init() {
    ray.RegisterActorType((*Counter)(nil))
}

type Counter struct {
    value int
}

func (c *Counter) Init() {

}

func (c *Counter) Increase1() {
    c.Increase(1)
}

func (c *Counter) Increase(i int) {
    c.value += i
}

func (c *Counter) Get() int {
    util.Logger.Debugf("invoke actor method")
    return c.value
}

func (c *Counter) Hello() string {
    util.Logger.Debugf("invoke actor hello method")
    return "hello"
}
