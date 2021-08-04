package actor

import (
    "reflect"

    "github.com/ray-project/ray-go-worker/pkg/ray"
    "github.com/ray-project/ray-go-worker/pkg/util"
)

func init() {
    ray.RegisterType(reflect.TypeOf((*Count)(nil)))
}

type Count struct {
    value int
}

func (c *Count) Init() {

}

func (c *Count) Increase(i int) {
    c.value += i
}

func (c *Count) Get() int {
    util.Logger.Debugf("invoke actor method")
    return c.value
}


