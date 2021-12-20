import asyncio
import pytest

import ray
import ray.cloudpickle as pickle


def test_generator_basic():
    def f(a, b):
        for n in range(a):
            yield b + b[-1] * n

    gen = f(4, 'ay')
    pickled = pickle.dumps(gen)
    results = ["ay", "ayy", "ayyy", "ayyyy"]

    for i in range(0, 4):
        new_gen = pickle.loads(pickled)
        assert new_gen is not gen
        assert results[i] == next(new_gen)
        pickled = pickle.dumps(new_gen)


def test_generator_try_except():
    def f(a):
        try:
            n = 10
            yield None
            raise ValueError("test")
        except ValueError:
            a += n
        yield a

    gen = f(4)
    pickled = pickle.dumps(gen)

    new_gen = pickle.loads(pickled)
    assert new_gen is not gen
    assert next(new_gen) is None
    pickled = pickle.dumps(new_gen)

    new_gen = pickle.loads(pickled)
    assert new_gen is not gen
    assert next(new_gen) == 14
