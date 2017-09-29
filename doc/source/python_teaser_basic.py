import time





def f():
    time.sleep(1)
    return 1

# Execute f serially.
done = [f() for i in range(4)]
