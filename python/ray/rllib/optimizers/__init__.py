from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.async_samples_optimizer import AsyncSamplesOptimizer
from ray.rllib.optimizers.async_gradients_optimizer import \
    AsyncGradientsOptimizer
from ray.rllib.optimizers.sync_samples_optimizer import SyncSamplesOptimizer
from ray.rllib.optimizers.sync_replay_optimizer import SyncReplayOptimizer
from ray.rllib.optimizers.multi_gpu_optimizer import LocalMultiGPUOptimizer


def run_optimizer(optimizer, num_steps, tag=""):
    from ray.tune.logger import UnifiedLogger
    import os
    import shutil
    path = os.path.join("/tmp/demo/", tag)
    try:
        shutil.rmtree(path)
    except OSError:
        print("could not remove path")
    try:
        os.makedirs(path)
    except OSError:
        print("could not make path")
    logger = UnifiedLogger({}, path, verbose=False)
    timesteps_total = 0
    for itr in range(num_steps + 1):
        optimizer.step()
        if itr:  #warm start
            result = optimizer.collect_metrics()
            timesteps_total = optimizer.num_steps_sampled
        else:
            from ray.tune.result import TrainingResult
            result = TrainingResult(episode_reward_mean=0)
        result = result._replace(
            training_iteration=itr,
            timesteps_total=timesteps_total)
        logger.on_result(result)
    logger.close()


__all__ = [
    "PolicyOptimizer", "AsyncSamplesOptimizer", "AsyncGradientsOptimizer",
    "SyncSamplesOptimizer", "SyncReplayOptimizer", "LocalMultiGPUOptimizer",
    "run_optimizer"
]
