import atexit
import m
print(m.global_var)
def r():
    print(m.global_var)
atexit.register(r)
