import unthrow

import pickle
p = 10

# class ContextManager():
#     def __init__(self):
#         print("init")
#         global p
#         p = 10

#     def __enter__(self):
#         global p
#         p = 20
#         return self

#     def __exit__(self, exc_type, exc_value, exc_traceback):
#         global p
#         p = 15

def g():
    global p
    print("before stop", p, flush=True)
    a = 20
    unthrow.stop(None)
    print("after stop", p, flush=True)
    raise BaseException()

def f(i):
    try:
        g()
        print("try", flush=True)
    except ValueError:
        global p
        print("except ValueError", p, flush=True)
    except Exception as e:
        print("except Exception", e, p, flush=True)
    except:
        print("unknown?", flush=True)
    else:
        print("No exception", flush=True)
    finally:
        print("finally", flush=True)

    print("f", i, "finished")

r = unthrow.Resumer()
r.run_once(f, 10)
print("---- resuming ---")
p = 17

# pickle.loads(pickle.dumps(r)).run_once(f, 10)


r.run_once(f, 10)

print("DONE")
