import greenlet

def f():
    x = g2.switch()
    print("f", x)

def g():
    x = g1.switch(10, 20, a=10)

    print("g", x)

g1 = greenlet.greenlet(f)
g2 = greenlet.greenlet(g)
g1.switch()
