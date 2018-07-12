from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.async_samples_optimizer import AsyncSamplesOptimizer
from ray.rllib.optimizers.async_gradients_optimizer import \
    AsyncGradientsOptimizer
from ray.rllib.optimizers.sync_samples_optimizer import SyncSamplesOptimizer
from ray.rllib.optimizers.sync_replay_optimizer import SyncReplayOptimizer
from ray.rllib.optimizers.multi_gpu_optimizer import LocalMultiGPUOptimizer

import logging
logging.getLogger("ray").setLevel(logging.ERROR)


def run_optimizer(optimizer, tag="", seconds=30):
    from ray.tune.logger import UnifiedLogger
    from ray.tune.result import TrainingResult
    import os
    import shutil
    import time
    path = os.path.join("/tmp/demo/", tag)
    try:
        shutil.rmtree(path)
    except OSError:
        print("could not remove path")
    try:
        os.makedirs(path)
    except OSError:
        print("could not make path")

    def set_keras_threads(ev, idx):
        if idx:
            import tensorflow as tf
            from keras import backend as K

            config = tf.ConfigProto(intra_op_parallelism_threads=1,
                                    inter_op_parallelism_threads=1,
                                    allow_soft_placement=True)
            session = tf.Session(config=config)
            K.set_session(session)
        return idx
    optimizer.foreach_evaluator_with_index(set_keras_threads)
    optimizer.collect_metrics() # hack to warmup
    logger = UnifiedLogger({}, path, verbose=False)
    result = TrainingResult(
        episode_reward_mean=0,
        training_iteration=0,
        timesteps_total=0)
    logger.on_result(result)
    logger.flush()
    print("Warmup done.")
    print("Running for {} seconds...".format(seconds))
    timesteps_total = 0
    start = time.time()
    itr = 1
    while time.time() - start < seconds:
        optimizer.step()
        result = optimizer.collect_metrics()
        timesteps_total = optimizer.num_steps_sampled
        result = result._replace(
            training_iteration=itr,
            timesteps_total=timesteps_total)
        logger.on_result(result)
        logger.flush()
        itr += 1
    logger.close()
    termination = [r.__ray_terminate__.remote() for r in optimizer.remote_evaluators]
    print("done")
    # return termination


__all__ = [
    "PolicyOptimizer", "AsyncSamplesOptimizer", "AsyncGradientsOptimizer",
    "SyncSamplesOptimizer", "SyncReplayOptimizer", "LocalMultiGPUOptimizer",
    "run_optimizer"
]
