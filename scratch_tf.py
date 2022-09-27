import argparse
import numpy as np

from ray.air import session
from ray.air.config import ScalingConfig
from ray.train._internal.utils import construct_train_func
from ray.train._internal.backend_executor import BackendExecutor
from ray.train.tensorflow.config import TensorflowConfig
from ray.train import TrainingIterator
from ray.train._internal.checkpoint import CheckpointManager
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.util.queue import Queue

import tensorflow as tf

from ray.air import session
from ray.air.config import ScalingConfig


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def train_func(config):
    queue = config["queues"][session.get_local_rank()]

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        multi_worker_model1 = build_model()
        multi_worker_model2 = build_model()
        optimizer1 = tf.keras.optimizers.Adam()
        optimizer2 = tf.keras.optimizers.Adam()

    @tf.function
    def _train(_data):
        x, y = _data

        x = tf.convert_to_tensor(x, dtype=tf.float32)
        y = tf.convert_to_tensor(y, dtype=tf.float32)

        with tf.GradientTape() as tape1:
            out1 = multi_worker_model1(x)
            loss1 = tf.reduce_mean((out1-y)**2)

        with tf.GradientTape() as tape2:
            out2 = multi_worker_model2(x)
            loss2 = tf.reduce_mean((out2-y)**2)

        grad1 = tape1.gradient(loss1, multi_worker_model1.trainable_variables)
        grad2 = tape2.gradient(loss2, multi_worker_model2.trainable_variables)
        optimizer1.apply_gradients(zip(grad1, multi_worker_model1.trainable_variables))
        optimizer2.apply_gradients(zip(grad2, multi_worker_model2.trainable_variables))
        return {"loss1" : loss1, "loss2" : loss2, "grad1": grad1, "grad2": grad2}

    counter = 0
    while 1:
        data = queue.get()

        rets = strategy.run(_train, args=(data,))
        print(rets)
        model1_params = multi_worker_model1.get_weights()
        model2_params = multi_worker_model2.get_weights()
        print(f"training_step: {counter}, rank: {session.get_local_rank()}, model1_params: {model1_params}, model2_params: {model2_params}")
        counter += 1

        session.report({})

    # return required for backwards compatibility with the old API
    # TODO(team-ml) clean up and remove return
    # return results
    session.report({})


def train_linear(num_workers=2, use_gpu=True, epochs=3):
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
    queues = [Queue(maxsize=1) for _ in range(num_workers)]
    backend_config = TensorflowConfig()
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs, "queues": queues}
    train_loop_per_worker = construct_train_func(
        train_func,
        config,
        fn_arg_name="train_loop_per_worker",
        discard_returns=False,
    )

    backend_executor = BackendExecutor(
            backend_config=backend_config,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            max_retries=0,
        )

    checkpoint_manager = CheckpointManager(
                checkpoint_strategy=None, run_dir=None
            )

    # Start the remote actors.
    backend_executor.start(initialization_hook=None)

    empty_spec = RayDatasetSpec(dataset_or_dict=None)

    training_iterator = TrainingIterator(
        backend_executor=backend_executor,
        backend_config=backend_config,
        train_func=train_loop_per_worker,
        dataset_spec=empty_spec,
        checkpoint_manager=checkpoint_manager,
        checkpoint=None,
        checkpoint_strategy=None,
    )

    size = 1000
    x = np.arange(0, 10, 10 / size, dtype=np.float32)
    a, b = 2, 5
    y = a * x + b
    # I just did some manual sharding over here myself. The details of this aren't important
    # as what I will do is pass a whole sample batch to a single queue shared across all workers,
    # then each worker will slice the sample batch itself using its global rank, and then train on that.
    x1 = x[0::2]
    y1 = y[0::2]
    x2 = x[1::2]
    y2 = y[1::2]
    d1 = (x1, y1)
    d2 = (x2, y2)
    
    for i in range(3):
        for queue, (_x, _y) in zip(queues, [d1, d2]):
            queue.put((_x[i:i+1], _y[i:i+1]))
        next(training_iterator)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", help="Whether to use GPU for training."
    )
    parser.add_argument(
        "--epochs", type=int, default=3, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=4)
        train_linear()
    else:
        ray.init(address=args.address)
        train_linear(
            num_workers=args.num_workers, use_gpu=args.use_gpu, epochs=args.epochs
        )
