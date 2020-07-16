import ray
import time

@ray.remote
def foo():
    a = {i: "b" for i in range(10000000)}
    return a

def main():
    ray.init()
    a = foo.remote()
    b = foo.remote()
    c = foo.remote()
    abcs = [foo.remote() for _ in range(10)]
    time.sleep(10000)

if __name__ == '__main__':
    main()

