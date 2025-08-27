import ray

@ray.remote
def hello_world():
    print("Hello, world!")

def main():
    ray.init()
    ray.get(hello_world.remote())
    ray.shutdown()

if __name__ == "__main__":
    main()
