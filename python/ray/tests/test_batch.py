import sys

import pytest

import ray


def test_basic_batch_actor_creation(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Counter:
        def __init__(self, start=0):
            self.val = start

        def inc(self, step=1):
            self.val += step
            return self.val

        def get(self):
            return self.val

    # Create batch of actors
    with ray.batch():
        c1 = Counter.remote(10)
        c2 = Counter.remote(20)
        c3 = Counter.remote(30)

    # Verify tasks can be scheduled and executed on each actor
    res = ray.get([c1.inc.remote(5), c2.inc.remote(10), c3.inc.remote(15)])
    assert res == [15, 30, 45]


def test_heterogeneous_actors_in_batch(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Adder:
        def add(self, a, b):
            return a + b

    @ray.remote
    class Multiplier:
        def mul(self, a, b):
            return a * b

    with ray.batch():
        adder = Adder.remote()
        multiplier = Multiplier.remote()

    res = ray.get([adder.add.remote(2, 3), multiplier.mul.remote(4, 5)])
    assert res == [5, 20]


def test_nested_batch(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Echo:
        def echo(self, msg):
            return msg

    with ray.batch():
        e1 = Echo.remote()
        with ray.batch():
            e2 = Echo.remote()
            with ray.batch():
                e3 = Echo.remote()
        e4 = Echo.remote()

    res = ray.get(
        [
            e1.echo.remote("a"),
            e2.echo.remote("b"),
            e3.echo.remote("c"),
            e4.echo.remote("d"),
        ]
    )
    assert res == ["a", "b", "c", "d"]


def test_empty_batch(shutdown_only):
    ray.init(num_cpus=2)

    with ray.batch():
        pass

    @ray.remote
    class Foo:
        def ping(self):
            return "pong"

    f = Foo.remote()
    assert ray.get(f.ping.remote()) == "pong"


def test_batch_with_exception(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    class Worker:
        def ping(self):
            return "ok"

    with pytest.raises(ValueError, match="intentional error"):
        with ray.batch():
            Worker.remote()
            raise ValueError("intentional error")

    # Ensure system is still in a healthy state for normal actor creation
    w2 = Worker.remote()
    assert ray.get(w2.ping.remote()) == "ok"


def test_large_batch(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class SimpleActor:
        def __init__(self, idx):
            self.idx = idx

        def get_idx(self):
            return self.idx

    count = 50
    with ray.batch():
        actors = [SimpleActor.remote(i) for i in range(count)]

    results = ray.get([a.get_idx.remote() for a in actors])
    assert results == list(range(count))


def test_batch_calls_inside_context(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class Worker:
        def __init__(self, val):
            self.val = val

        def get(self):
            return self.val

    with ray.batch():
        a1 = Worker.remote(100)
        ref1 = a1.get.remote()
        a2 = Worker.remote(200)
        ref2 = a2.get.remote()

    assert ray.get([ref1, ref2]) == [100, 200]


def test_batch_named_and_detached_actors(shutdown_only):
    ray.init(num_cpus=4)

    @ray.remote
    class NamedActor:
        def ping(self):
            return "pong"

    with ray.batch():
        a1 = NamedActor.options(name="actor_one").remote()
        a2 = NamedActor.options(name="actor_two", lifetime="detached").remote()

    assert ray.get(a1.ping.remote()) == "pong"
    assert ray.get(a2.ping.remote()) == "pong"

    # Verify retrieval by name
    h1 = ray.get_actor("actor_one")
    h2 = ray.get_actor("actor_two")
    assert ray.get(h1.ping.remote()) == "pong"
    assert ray.get(h2.ping.remote()) == "pong"


def test_batch_concurrent_threads(shutdown_only):
    import threading

    ray.init(num_cpus=8)

    @ray.remote(num_cpus=0.1)
    class Worker:
        def __init__(self, tid, idx):
            self.tid = tid
            self.idx = idx

        def get(self):
            return (self.tid, self.idx)

    results = {}

    def thread_worker(tid):
        with ray.batch():
            actors = [Worker.remote(tid, i) for i in range(10)]
        res = ray.get([a.get.remote() for a in actors])
        results[tid] = res

    threads = [threading.Thread(target=thread_worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for tid in range(4):
        assert results[tid] == [(tid, i) for i in range(10)]


def test_batch_placement_group(shutdown_only):
    ray.init(num_cpus=4)
    pg = ray.util.placement_group([{"CPU": 1}, {"CPU": 1}])
    ray.get(pg.ready())

    @ray.remote(num_cpus=1)
    class PGActor:
        def get_id(self, idx):
            return idx

    with ray.batch():
        a1 = PGActor.options(
            scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=0
            )
        ).remote()
        a2 = PGActor.options(
            scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=1
            )
        ).remote()

    assert ray.get([a1.get_id.remote(1), a2.get_id.remote(2)]) == [1, 2]


def test_batch_performance_comparison(shutdown_only):
    import time

    ray.init(num_cpus=4)

    @ray.remote(num_cpus=0)
    class BenchActor:
        def ping(self):
            return 1

    num_actors = 100

    start_seq = time.time()
    seq_actors = [BenchActor.remote() for _ in range(num_actors)]
    ray.get([a.ping.remote() for a in seq_actors])
    seq_duration = time.time() - start_seq

    start_batch = time.time()
    with ray.batch():
        batch_actors = [BenchActor.remote() for _ in range(num_actors)]
    ray.get([a.ping.remote() for a in batch_actors])
    batch_duration = time.time() - start_batch

    print(
        f"Sequential: {seq_duration:.3f}s, Batch: {batch_duration:.3f}s for {num_actors} actors"
    )
    assert len(batch_actors) == num_actors


def test_batch_auto_init(shutdown_only):
    # Ensure ray is shut down
    if ray.is_initialized():
        ray.shutdown()

    @ray.remote
    class InitTestActor:
        def ping(self):
            return "pong"

    # with ray.batch() should auto-init ray
    with ray.batch():
        a = InitTestActor.remote()

    assert ray.is_initialized()
    assert ray.get(a.ping.remote()) == "pong"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "run_all":
        for test_fn in [
            test_basic_batch_actor_creation,
            test_heterogeneous_actors_in_batch,
            test_nested_batch,
            test_empty_batch,
            test_batch_with_exception,
            test_large_batch,
            test_batch_calls_inside_context,
            test_batch_named_and_detached_actors,
            test_batch_concurrent_threads,
            test_batch_placement_group,
            test_batch_performance_comparison,
            test_batch_auto_init,
        ]:
            print(f"Running {test_fn.__name__}...")
            if ray.is_initialized():
                ray.shutdown()
            try:
                test_fn(None)
                print(f"  {test_fn.__name__} PASSED")
            finally:
                if ray.is_initialized():
                    ray.shutdown()
        print("All tests PASSED!")
    else:
        sys.exit(pytest.main(["-v", __file__]))
