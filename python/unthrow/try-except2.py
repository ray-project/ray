import inspect
import unthrow

def h():
    unthrow.jmp()
def g():
    h()

def f():
    x = unthrow.setjmp()
    if x:
        print("first time")
        g()
    else:
        print("second time")

f()
