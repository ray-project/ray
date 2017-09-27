import time





def f():
    time.sleep(1)
    return True

start = time.time()

done = [f() for i in range(8)]

end = time.time()

print(end - start)
# 8 seconds