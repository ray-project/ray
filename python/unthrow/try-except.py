import unthrow
import cloudpickle
import pickle
p = 10

class ContextManager():
    def __init__(self):
        print("init")
        global p
        p = 10

    def __enter__(self):
        global p
        p = 20
        print("context updating p=", p)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        global p
        p = 15
        print("context updating p=", p)

def g():
    global p
    print("before stop p=", p, flush=True)
    a = 20
    print("stop", unthrow.stop(30))
    print("after stop p=", p, flush=True)
    raise BaseException()

def f(i):
    with ContextManager() as c:
        try:
            g()
            print("try", flush=True)
        except ValueError:
            global p
            print("except ValueError", p, flush=True)
        except Exception as e:
            print("except Exception", e, p, flush=True)
        except BaseException as e:
            print("unknown?", e, type(e), flush=True)
        else:
            print("No exception", flush=True)
        finally:
            print("finally", flush=True)

        print("f", i, "finished")
        return "ABC"

r = unthrow.Resumer()
print("run_once", r.run_once(f, 10))
print("---- resuming ---")


print("run_once", cloudpickle.loads(cloudpickle.dumps(r)).run_once(f, 10))

print("DONE")
"""
(base) yic@ip-172-31-58-40:~/upstream-ray/ray/python/unthrow (dynamic-checkpoint) $ python try-except.py
init
context updating p= 20
before stop p= 20
run_once 30
---- resuming ---
RESUME
stop None
after stop p= 20
unknown?  <class 'BaseException'>
finally
f 10 finished
context updating p= 15
run_once ABC
DONE
"""
