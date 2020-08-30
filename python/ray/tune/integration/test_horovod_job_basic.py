"""Ray-Horovod Job tests.

This is currently not run on the Ray CI and is expected
to land in Horovod repo soon.
"""

import os
import ray
from ray.tune.integration._horovod_job import HorovodJob


def test_local(address=None):
    ray.init(address)
    original_resources = ray.available_resources()
    setting = HorovodJob.create_settings(
        timeout_s=30,
        ssh_identity_file=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(setting, num_hosts=1, num_slots=4, use_gpu=True)
    hjob.start()
    hostnames = hjob.execute(lambda _: ray.services.get_node_ip_address())
    assert len(set(hostnames)) == 1, hostnames
    hjob.shutdown()
    assert original_resources == ray.available_resources()
    ray.shutdown()
    print("Finished!")


def test_hvd_init(address=None, hosts=1):
    ray.init(address)
    original_resources = ray.available_resources()

    def simple_fn(worker):
        import horovod.torch as hvd
        hvd.init()
        print("hvd rank", hvd.rank())
        return hvd.rank()

    setting = HorovodJob.create_settings(
        timeout_s=30,
        ssh_identity_file=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(setting, num_hosts=hosts, num_slots=4, use_gpu=True)
    hjob.start()
    result = hjob.execute(simple_fn)
    assert len(set(result)) == hosts * 4
    hjob.shutdown()
    for i in reversed(range(10)):
        if original_resources == ray.available_resources():
            break
        else:
            print(ray.available_resources())
            import time
            time.sleep(0.5)

    if i == 0:
        raise Exception("not rreosurces not released")
    ray.shutdown()
    print("Finished!")


def _train(model_str="resnet50",
           use_gpu=None,
           batch_size=32,
           batch_per_iter=10):
    import torch.backends.cudnn as cudnn
    import torch.nn.functional as F
    import torch.optim as optim
    import torch.utils.data.distributed
    from torchvision import models
    import horovod.torch as hvd
    import timeit

    if use_gpu is None:
        use_gpu = ("CUDA_VISIBLE_DEVICES" in os.environ)
    hvd.init()
    cudnn.benchmark = True

    # Set up standard model.
    model = getattr(models, model_str)()

    if use_gpu:
        # Move model to GPU.
        model.cuda()
    else:
        batch_per_iter = 1

    optimizer = optim.SGD(model.parameters(), lr=0.01)

    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(
        optimizer, named_parameters=model.named_parameters())

    # Horovod: broadcast parameters & optimizer state.
    hvd.broadcast_parameters(model.state_dict(), root_rank=0)
    hvd.broadcast_optimizer_state(optimizer, root_rank=0)

    # Set up fixed fake data
    data = torch.randn(batch_size, 3, 224, 224)
    target = torch.LongTensor(batch_size).random_() % 1000
    if use_gpu:
        data, target = data.cuda(), target.cuda()

    def benchmark_step():
        optimizer.zero_grad()
        output = model(data)
        loss = F.cross_entropy(output, target)
        loss.backward()
        optimizer.step()

    for i in range(5):
        time = timeit.timeit(benchmark_step, number=batch_per_iter)
        img_sec = batch_size * batch_per_iter / time
        print("Iter #%d: %.1f img/sec per %s" % (i, img_sec, "GPU"
                                                 if use_gpu else "CPU"))


def test_horovod_train(address=None, hosts=1, use_gpu=None):
    ray.init(address)

    def simple_fn(worker):
        _train(use_gpu=use_gpu)
        return True

    setting = HorovodJob.create_settings(
        timeout_s=30,
        ssh_identity_file=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(setting, num_hosts=hosts, num_slots=4, use_gpu=True)
    hjob.start()
    result = hjob.execute(simple_fn)
    print("Result: ", result)
    assert all(result)
    hjob.shutdown()
    ray.shutdown()
    print("Finished!")


########## Unit Tests below ###################

@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()



def test_coordinator_registration():
    from ray.tune.integration.horovod import Coordinator, MiniSettings
    settings = MiniSettings()
    coord = Coordinator(settings)
    assert coord.world_size == 0
    assert coord.hoststring == ""
    ranks = list(range(12))

    for i, hostname in enumerate(["a", "b", "c"]):
        for r in ranks:
            if r % 3 == i:
                coord.register(hostname, world_rank=r)

    rank_to_info = coord.finalize_registration()
    assert len(rank_to_info) == len(ranks)
    assert all(info["node_world_size"] == 3 for info in rank_to_info.values())
    assert {info["node_world_rank"]
            for info in rank_to_info.values()} == {0, 1, 2}
    assert all(info["local_size"] == 4 for info in rank_to_info.values())
    assert {info["local_rank"]
            for info in rank_to_info.values()} == {0, 1, 2, 3}


def test_colocator(tmpdir, ray_start_6_cpus):
    colocator = NodeColocator.options(num_cpus=4).remote(
        num_workers=4, use_gpu=False)
    colocator.create_workers.remote(_MockHorovodTrainable, {"hi": 1}, tmpdir)
    worker_handles = ray.get(colocator.get_workers.remote())
    assert len(set(ray.get(
        [h.hostname.remote() for h in worker_handles]))) == 1

    resources = ray.available_resources()
    ip_address = ray.services.get_node_ip_address()
    assert resources.get("CPU", 0) == 2, resources
    assert resources.get(f"node:{ip_address}", 0) == 1 - 4 * 0.01


def test_colocator_gpu(tmpdir, ray_start_4_cpus_4_gpus):
    colocator = NodeColocator.options(
        num_cpus=4, num_gpus=4).remote(
            num_workers=4, use_gpu=True)
    colocator.create_workers.remote(_MockHorovodTrainable, {"hi": 1}, tmpdir)
    worker_handles = ray.get(colocator.get_workers.remote())
    assert len(set(ray.get(
        [h.hostname.remote() for h in worker_handles]))) == 1
    resources = ray.available_resources()
    ip_address = ray.services.get_node_ip_address()
    assert resources.get("CPU", 0) == 0, resources
    assert resources.get("GPU", 0) == 0, resources
    assert resources.get(f"node:{ip_address}", 0) == 1 - 4 * 0.01

    all_envs = ray.get([h.env_vars.remote() for h in worker_handles])
    assert len({ev["CUDA_VISIBLE_DEVICES"] for ev in all_envs}) == 4


def test_horovod_mixin(ray_start_2_cpus):
    assert Test().hostname() == ray.services.get_node_ip_address()
    actor = ray.remote(BaseHorovodWorker).remote()
    DUMMY_VALUE = 1123123
    actor.update_env_vars.remote({"TEST": DUMMY_VALUE})
    assert ray.get(actor.env_vars.remote())["TEST"] == str(DUMMY_VALUE)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster", action="store_true", help="Use on cluster.")
    parser.add_argument(
        "--hosts", required=False, type=int, default=1, help="number of hosts")
    parser.add_argument("--no-gpu", action="store_true")
    args = parser.parse_args()
    test_local()
    test_hvd_init(address="auto" if args.cluster else None, hosts=args.hosts)
    test_horovod_train(
        address="auto" if args.cluster else None,
        hosts=args.hosts,
        use_gpu=not args.no_gpu)
