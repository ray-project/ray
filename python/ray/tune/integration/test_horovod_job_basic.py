"""Ray-Horovod Job tests.

This is currently not run on the Ray CI and is expected
to land in Horovod repo soon. If not, then these
should be run after every commit for anything that touches
the Horovod-Ray integration.
"""

import os

import ray
from ray.tune.integration._horovod_job import HorovodJob


def test_local(address=None, num_slots=4):
    ray.init(address)
    original_resources = ray.available_resources()
    setting = HorovodJob.create_settings(
        timeout_s=30,
        ssh_identity_file=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(setting, num_hosts=1, num_slots=num_slots, use_gpu=True)
    hjob.start()
    hostnames = hjob.execute(lambda _: ray.services.get_node_ip_address())
    assert len(set(hostnames)) == 1, hostnames
    hjob.shutdown()
    assert original_resources == ray.available_resources()
    ray.shutdown()
    print("Finished!")


def test_hvd_init(address=None, num_slots=4, hosts=1):
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
    hjob = HorovodJob(
        setting, num_hosts=hosts, num_slots=num_slots, use_gpu=True)
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


def test_horovod_train(address=None, num_slots=4, hosts=1, use_gpu=None):
    ray.init(address)

    def simple_fn(worker):
        _train(use_gpu=use_gpu)
        return True

    setting = HorovodJob.create_settings(
        timeout_s=30,
        ssh_identity_file=os.path.expanduser("~/ray_bootstrap_key.pem"))
    hjob = HorovodJob(
        setting, num_hosts=hosts, num_slots=num_slots, use_gpu=True)
    hjob.start()
    result = hjob.execute(simple_fn)
    print("Result: ", result)
    assert all(result)
    hjob.shutdown()
    ray.shutdown()
    print("Finished!")


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
