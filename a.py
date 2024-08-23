from queue import Queue
import threading

import time

q = Queue()
d = b"1" * 1024 * 100 # 100kb
done = []
def run(event):
    i = 0
    while True:
        # result = q.get()
        event.wait()
        i += 1
        if i == 1:
            break
    print("done", time.time())

event =threading.Event()
t = threading.Thread(target=run, args=(event,))
t.start()
a = None
a = []
st = time.time()
for _ in range(1):
    a.append(d)
    a.pop()
    # q.put_nowait(d)
el = time.time() - st
print(el * 1000 * 1000, "us")
print(el / 100 * 1000 * 1000, "put_and_get/us")
