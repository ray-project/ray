from unthrow import *
def l():
    return 20

def f():
    s = 0

    raise RuntimeError()



def t():
    r = Resumer()
    print("run_once", r.run_once(f,))


t()
