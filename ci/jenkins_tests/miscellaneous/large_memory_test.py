import numpy as np

import ray

if __name__ == "__main__":
    ray.init(num_cpus=0)

    A = np.ones(2**31 + 1, dtype="int8")
    a = ray.put(A)
    assert np.sum(ray.get(a)) == np.sum(A)
    del A
    del a
    print("Successfully put A.")

    B = {"hello": np.zeros(2**30 + 1), "world": np.ones(2**30 + 1)}
    b = ray.put(B)
    assert np.sum(ray.get(b)["hello"]) == np.sum(B["hello"])
    assert np.sum(ray.get(b)["world"]) == np.sum(B["world"])
    del B
    del b
    print("Successfully put B.")

    C = [np.ones(2**30 + 1), 42.0 * np.ones(2**30 + 1)]
    c = ray.put(C)
    assert np.sum(ray.get(c)[0]) == np.sum(C[0])
    assert np.sum(ray.get(c)[1]) == np.sum(C[1])
    del C
    del c
    print("Successfully put C.")

    # The below code runs successfully, but when commented in, the whole test
    # takes about 10 minutes.

    # D = (2 ** 30 + 1) * ["h"]
    # d = ray.put(D)
    # assert ray.get(d) == D
    # del D
    # del d
    # print("Successfully put D.")

    # E = (2 ** 30 + 1) * ("i",)
    # e = ray.put(E)
    # assert ray.get(e) == E
    # del E
    # del e
    # print("Successfully put E.")
