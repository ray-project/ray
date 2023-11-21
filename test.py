import random
import threading
import time

x = {}

def set(i):
	time.sleep(random.random())
	x[i] = True

def get():
	time.sleep(random.random())
	y = x.values()


threads = []
for i in range(1000):
	threads.append(threading.Thread(target=set, args=(i,)))
	threads.append(threading.Thread(target=get))


for thread in threads:
	thread.start()

for thread in threads:
	thread.join()

print(len(x))
