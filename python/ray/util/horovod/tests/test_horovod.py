import pytest
import torch
import ray
from ray.util.client.ray_client_helpers import ray_start_client_server

pytest.importorskip("horovod")

try:
    from horovod.ray.runner import RayExecutor
    from horovod.common.util import gloo_built
except ImportError:
    pass  # This shouldn't be reached - the test should be skipped.


# For each test, run it once with ray.init() and again with ray client.
@pytest.fixture(params=[False, True])
def ray_start_4_cpus(request):
    if request.param:
        assert not ray.util.client.ray.is_connected()
        with ray_start_client_server(ray_init_kwargs={"num_cpus": 3}):
            assert ray.util.client.ray.is_connected()
            yield
    else:
        ray.init(num_cpus=4)
        yield
        ray.shutdown()


def _train(batch_size=32, batch_per_iter=10):
    import torch.nn.functional as F
    import torch.optim as optim
    import torch.utils.data.distributed
    import horovod.torch as hvd
    import timeit

    hvd.init()

    # Set up fixed fake data
    data = torch.randn(batch_size, 2)
    target = torch.LongTensor(batch_size).random_() % 2

    model = torch.nn.Sequential(torch.nn.Linear(2, 2))
    optimizer = optim.SGD(model.parameters(), lr=0.01)

    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(
        optimizer, named_parameters=model.named_parameters()
    )

    # Horovod: broadcast parameters & optimizer state.
    hvd.broadcast_parameters(model.state_dict(), root_rank=0)
    hvd.broadcast_optimizer_state(optimizer, root_rank=0)

    def benchmark_step():
        optimizer.zero_grad()
        output = model(data)
        loss = F.cross_entropy(output, target)
        loss.backward()
        optimizer.step()

    timeit.timeit(benchmark_step, number=batch_per_iter)
    return hvd.local_rank()


@pytest.mark.skipif(not gloo_built(), reason="Gloo is required for Ray integration")
def test_train(ray_start_4_cpus):
    def simple_fn(worker):
        local_rank = _train()
        return local_rank

    setting = RayExecutor.create_settings(timeout_s=30)
    hjob = RayExecutor(setting, num_workers=3, use_gpu=torch.cuda.is_available())
    hjob.start()
    result = hjob.execute(simple_fn)
    assert set(result) == {0, 1, 2}
    result = ray.get(hjob.run_remote(simple_fn, args=[None]))
    assert set(result) == {0, 1, 2}
    hjob.shutdown()


@pytest.mark.skipif(not gloo_built(), reason="Gloo is required for Ray integration")
def test_horovod_example(ray_start_4_cpus):
    from ray.util.horovod.horovod_example import main

    kwargs = {
        "data_dir": "./data",
        "num_epochs": 1,
    }

    main(num_workers=1, use_gpu=False, kwargs=kwargs)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
