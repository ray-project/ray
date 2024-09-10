import time
import numpy as np
import ray


class StatefulFn1:
    def __init__(self):
        self.num_reuses = 0

    def __call__(self, x):
        r = self.num_reuses
        time.sleep(1.5)
        self.num_reuses += 1
        print("StatefulFn1=======")
        return {"id": np.array([r])}


class StatefulFn2:
    def __init__(self):
        self.num_reuses = 0

    def __call__(self, x):
        r = self.num_reuses
        time.sleep(1.5)
        self.num_reuses += 1
        # mock大的输出
        r = list(range(1000))
        print("StatefulFn2=======")
        return {"id": np.array(r)}


class StatefulFn3:
    def __init__(self):
        self.num_reuses = 0

    def __call__(self, x):
        r = self.num_reuses
        # mock 计算重的操作
        for i in range(1):
            for j in range(1):
                r += i * j
        time.sleep(1.5)
        print("StatefulFn3=======")
        self.num_reuses += 1
        return {"id": np.array([r])}


class StatefulFn4:
    def __init__(self):
        self.num_reuses = 0

    def __call__(self, x):
        print("StatefulFn4=======")
        r = self.num_reuses
        time.sleep(1.5)
        self.num_reuses += 1
        return {"id": np.array([r])}


min_concurrency = 1

max_concurrency = 100

def test_case():
    ray.init(runtime_env={
        'excludes': [
            '/data00/code/emr/ray/doc/data/MNIST/raw/train-images-idx3-ubyte',
            '/data00/code/emr/ray/.git/objects/pack/pack-513e36f79e4f9a59a3943ee13004b73805b86d00.pack',
            '/data00/code/emr/ray/.git/objects/pack/pack-ac131bd2655a96611fecf0ea2511fbb73bb2657d.pack'
        ]
    })
    ts = time.time()

    n = 10
    ds = ray.data.range(n)

    actor_reuse = (ds.map(StatefulFn1, num_cpus=1, concurrency=(min_concurrency, max_concurrency)).map_batches(
        StatefulFn2, num_cpus=2,
        batch_size=1,
        concurrency=(min_concurrency,
                     max_concurrency)).map(
        StatefulFn3,
        num_cpus=1,
        concurrency=(
            min_concurrency,
            max_concurrency)).map(
        StatefulFn4,
        num_cpus=1,
        concurrency=(
            min_concurrency,
            max_concurrency)).take_all())
    print(f"cost time {time.time() - ts} seconds")

test_case()