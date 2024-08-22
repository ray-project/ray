from queue import Queue
import threading

import time

q = Queue()
d = b"1" * 1024 * 100 # 100kb
done = []
def run():
    i = 0
    while True:
        result = q.get()
        i += 1
        if i == 100:
            break
    print("done", time.time())

t = threading.Thread(target=run)
t.start()
st = time.time()
for _ in range(100):
    q.put(d)
el = time.time() - st

print(el / 100 * 1000 * 1000, "put_and_get/us")
breakpoint()
