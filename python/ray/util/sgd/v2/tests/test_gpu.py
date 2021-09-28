import pytest
import ray
from ray.util.sgd.v2 import Trainer
# from ray.util.sgd.v2.examples.horovod.horovod_example import train_func as \
#     horovod_torch_train_func
from ray.util.sgd.v2.examples.tensorflow_mnist_example import train_func as \
    tensorflow_mnist_train_func, mnist_dataset, build_and_compile_cnn_model, \
    SGDReportCallback
from ray.util.sgd.v2.examples.train_fashion_mnist_example import train_func \
    as fashion_mnist_train_func
from test_tune import torch_fashion_mnist, tune_tensorflow_mnist


@pytest.fixture
def ray_start_2_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()

def tf_mnist_train_func(config):
    import json
    import tensorflow as tf
    import os

    # os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

    os.environ["CUDA_VISIBLE_DEVICES"] = str(ray.util.sgd.v2.local_rank())
    print(os.environ["CUDA_VISIBLE_DEVICES"])

    from tensorflow.python.client import device_lib
    print(device_lib.list_local_devices())

    per_worker_batch_size = config.get("batch_size", 64)
    epochs = config.get("epochs", 3)
    steps_per_epoch = config.get("steps_per_epoch", 70)

    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_and_compile_cnn_model(config)

    history = multi_worker_model.fit(
        multi_worker_dataset,
        epochs=epochs,
        steps_per_epoch=steps_per_epoch,
        callbacks=[SGDReportCallback()])
    results = history.history
    print(results)
    return results


def test_tensorflow_mnist_gpu(ray_start_2_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("tensorflow", num_workers=num_workers, use_gpu=True)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(tf_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers
    result = results[0]

    loss = result["loss"]
    assert len(loss) == epochs
    assert loss[-1] < loss[0]

    accuracy = result["accuracy"]
    assert len(accuracy) == epochs
    assert accuracy[-1] > accuracy[0]


def test_torch_fashion_mnist_gpu(ray_start_2_cpus_2_gpus):
    num_workers = 2
    epochs = 3

    trainer = Trainer("torch", num_workers=num_workers, use_gpu=True)
    config = {"lr": 1e-3, "batch_size": 64, "epochs": epochs}
    trainer.start()
    results = trainer.run(fashion_mnist_train_func, config)
    trainer.shutdown()

    assert len(results) == num_workers

    for result in results:
        assert len(result) == epochs
        assert result[-1] < result[0]


# def test_horovod_torch_mnist_gpu(ray_start_2_cpus_2_gpus):
#     num_workers = 2
#     num_epochs = 2
#     trainer = Trainer("horovod", num_workers, use_gpu=True)
#     trainer.start()
#     results = trainer.run(
#         horovod_torch_train_func,
#         config={
#             "num_epochs": num_epochs,
#             "lr": 1e-3
#         })
#     trainer.shutdown()
#
#     assert len(results) == num_workers
#     for worker_result in results:
#         assert len(worker_result) == num_epochs
#         assert worker_result[num_epochs - 1] < worker_result[0]


def test_tune_fashion_mnist_gpu(ray_start_2_cpus_2_gpus):
    torch_fashion_mnist(num_workers=2, use_gpu=True, num_samples=1)


def test_tune_tensorflow_mnist_gpu(ray_start_2_cpus_2_gpus):
    tune_tensorflow_mnist(num_workers=2, use_gpu=True, num_samples=1)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
