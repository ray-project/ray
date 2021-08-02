package actor

type Count struct {
    value int
}

func (c *Count) Init() {

}

func (c *Count) Increase(i int) {
    c.value += i
}

func (c *Count) Get() int {
    return c.value
}
