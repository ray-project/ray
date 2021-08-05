import requests
import tensorflow

from my_pkg import my_func

@ray.remote
def task():
    tensorflow.do_something()

def main():
    ray.get(task.remote())

if __name__ == "__main__":
    main()
