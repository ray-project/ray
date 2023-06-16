from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchCompileConfig
from ray.rllib.models.catalog import MODEL_DEFAULTS
from ray.rllib.core.learner.learner import FrameworkHyperparameters, LearnerHyperparameters
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.test_utils import check
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
import numpy as np
from typing import Union
import gymnasium as gym
import torch
import torch._dynamo as dynamo

torch._dynamo.allow_in_graph(torch.distributions.kl.kl_divergence)
torch._dynamo.disallow_in_graph(torch.distributions.kl.kl_divergence)
torch.set_float32_matmul_precision('high')

# Some choices on what/how to test:
BATCH_SIZE_EVAL = 1
BATCH_SIZE_TRAIN = 8
N_ITERS = 500


def get_ppo_batch_for_env(env: Union[str, gym.Env], batch_size):
    """Create a dummy sample batch for the given environment.
    
    Args:
        env: The environment to create a sample batch for. If a string is given,
        it is assumed to be a gym environment ID.
        batch_size: The batch size to use.

    Returns:
        A sample batch for the given environment.
    """
    env.reset()
    action = env.action_space.sample()
    if type(env.action_space) is gym.spaces.Box:
        action_inputs = [0.5, 0.5]
    else:
        action_inputs = [0.5]
    obs, reward, truncated, terminated, info = env.step(action)

    def batchify(x):
        x = np.array(x)
        return np.repeat(x[np.newaxis], batch_size, axis=0)

    # Fake CartPole episode of n time steps.
    return SampleBatch({
        SampleBatch.OBS: batchify(obs),
        SampleBatch.NEXT_OBS: batchify(obs),
        SampleBatch.ACTIONS: batchify([action]),
        SampleBatch.PREV_ACTIONS: batchify([action]),
        SampleBatch.REWARDS: batchify([reward]),
        SampleBatch.PREV_REWARDS: batchify([reward]),
        SampleBatch.TERMINATEDS: batchify([terminated]),
        SampleBatch.TRUNCATEDS: batchify([truncated]),
        SampleBatch.VF_PREDS: batchify(.0),
        SampleBatch.ACTION_DIST_INPUTS: batchify(action_inputs),
        SampleBatch.ACTION_LOGP: batchify(.0),
        SampleBatch.EPS_ID: batchify(0),
        "advantages": batchify(.0),
        "value_targets": batchify(.0),
        SampleBatch.AGENT_INDEX: batchify(0),
    })

env = wrap_deepmind(gym.make("GymV26Environment-v0", env_id="ALE/Breakout-v5"))
# env = gym.make("CartPole-v1")

eval_batch = get_ppo_batch_for_env(env, batch_size=BATCH_SIZE_EVAL)

model_cfg = MODEL_DEFAULTS.copy()

spec = SingleAgentRLModuleSpec(
    module_class=PPOTorchRLModule,
    observation_space=env.observation_space,
    action_space=env.action_space,
    catalog_class=PPOCatalog,
    model_config_dict=model_cfg
)

eager_module = spec.build().to(0)
compiled_module = spec.build().to(0)

compile_config = TorchCompileConfig()

# Compile only one of the modules.
compiled_module = compiled_module.compile(compile_config)

def timed(fn, no_grad=True):
    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record()
    if no_grad:
        with torch.no_grad():
            result = fn()
    else:
        result = fn()
    end.record()
    torch.cuda.synchronize()
    return result, start.elapsed_time(end) / 1000

# Evaluation
def evaluate(mod, inp):
    return mod.forward_exploration(inp)

# Generates random input and targets data for the model, where `b` is
# batch size.
def generate_data():
    return {n: torch.Tensor(t).to(torch.float32).to(0) for n, t in eval_batch.items()}

def generate_target():
    return torch.Tensor(np.array([[-1.0, 2.5], [-1.0, -2.3], [-1, 2.5]])).to(torch.float32).to(0)


inp = generate_data()
print("~" * 10)
print("Eager warmup time:", timed(lambda: evaluate(eager_module, inp))[1])
print("Compile warmup time:", timed(lambda: evaluate(compiled_module, inp))[1])

eager_times = []
compile_times = []

print("~" * 10)

for i in range(N_ITERS):
    inp = generate_data()
    _, eager_time = timed(lambda: evaluate(eager_module, inp))
    eager_times.append(eager_time)
    # print(f"eager eval time {i}: {eager_time}")

print("~" * 10)

for i in range(N_ITERS):
    inp = generate_data()
    _, compile_time = timed(lambda: evaluate(compiled_module, inp))
    compile_times.append(compile_time)
    # print(f"compile eval time {i}: {compile_time}")

eager_med = np.median(eager_times)
eager_mean = np.mean(eager_times)
compile_med = np.median(compile_times)
compile_mean = np.mean(compile_times)
speedup_median = eager_med / compile_med
speedup_mean = eager_mean / compile_mean
print(f"(eval) eager median: {eager_med}, compile median: {compile_med}, speedup median: {speedup_median}x")
print(f"(eval) eager mean: {eager_mean}, compile mean: {compile_mean}, speedup mean: {speedup_mean}x")
print("~" * 10)


# Time learner update
framework_hps_eager = FrameworkHyperparameters(
    torch_compile=False,
    torch_compile_cfg=TorchCompileConfig(),
)
framework_hps_compile_update = FrameworkHyperparameters(
    torch_compile=True,
    what_to_compile="gradient_computation",
    torch_compile_cfg=TorchCompileConfig(
        torch_dynamo_backend="inductor",
        torch_dynamo_mode="max-autotune"
    ),
)
framework_hps_compile_forward_train = FrameworkHyperparameters(
    torch_compile=True,
    what_to_compile="forward_train",
    torch_compile_cfg=TorchCompileConfig(
        torch_dynamo_backend="inductor",
        torch_dynamo_mode="max-autotune"
    ),
)

ppo_config = PPOConfig()
learner_hps = ppo_config.get_learner_hyperparameters()
scaling_config = LearnerGroupScalingConfig(num_gpus_per_worker=1)
learner_eager = PPOTorchLearner(
    learner_hyperparameters=learner_hps,
    module_spec=spec,
    framework_hyperparameters=framework_hps_eager,
    learner_group_scaling_config=scaling_config,
)
learner_update_compiled = PPOTorchLearner(
    learner_hyperparameters=learner_hps,
    module_spec=spec,
    framework_hyperparameters=framework_hps_compile_update,
    learner_group_scaling_config=scaling_config,
)
learner_forward_train_compiled = PPOTorchLearner(
    learner_hyperparameters=learner_hps,
    module_spec=spec,
    framework_hyperparameters=framework_hps_compile_forward_train,
    learner_group_scaling_config=scaling_config,
)
learner_eager.build()
learner_update_compiled.build()
learner_forward_train_compiled.build()

train_batch = get_ppo_batch_for_env(env, batch_size=128)
batch = train_batch.as_multi_agent()

eager_times = []
del compile_times
update_compiled_times = []
forward_train_compiled_times = []

print("~" * 10)

for i in range(N_ITERS):
    _, compile_time = timed(lambda: learner_update_compiled.update(batch), no_grad=False)
    update_compiled_times.append(compile_time)
    # print(f"update_compiled train time {i}: {compile_time}")
# This is a helper method of dynamo to analyze where breaks occur.

print("~" * 10)

for i in range(N_ITERS):
    _, eager_time = timed(lambda: learner_eager.update(batch), no_grad=False)
    eager_times.append(eager_time)
    # print(f"eager train time {i}: {eager_time}")
# This is a helper method of dynamo to analyze where breaks occur.

print("~" * 10)

for i in range(N_ITERS):
    _, compile_time = timed(lambda: learner_forward_train_compiled.update(batch), no_grad=False)
    forward_train_compiled_times.append(compile_time)
    # print(f"forward_compiled train time {i}: {compile_time}")
# This is a helper method of dynamo to analyze where breaks occur.

eager_med = np.median(eager_times)
eager_mean = np.mean(eager_times)

print("torch compile Learner speedups:")
print(f"(train) eager median: {eager_med}")
print(f"(train) eager mean: {eager_mean}")

update_compiled_med = np.median(update_compiled_times)
update_compiled_mean = np.mean(update_compiled_times)
update_compiled_speedup_median = eager_med / update_compiled_med
update_compiled_speedup_mean = eager_mean / update_compiled_mean
print(f"(train) update compile median: {update_compiled_med}, speedup median: {update_compiled_speedup_median}x")
print(f"(train) update compile mean: {update_compiled_mean}, speedup mean: {update_compiled_speedup_mean}x")

forward_train_compiled_med = np.median(forward_train_compiled_times)
forward_train_compiled_mean = np.mean(forward_train_compiled_times)
forward_train_compiled_speedup_median = eager_med / forward_train_compiled_med
forward_train_compiled_speedup_mean = eager_mean / forward_train_compiled_mean
print(f"(train) forward compile median: {forward_train_compiled_med}, speedup median: {forward_train_compiled_speedup_median}x")
print(f"(train) forward compile mean: {forward_train_compiled_mean}, speedup mean: {forward_train_compiled_speedup_mean}x")


print("~" * 10)

# Convert batch to dict of dicts (which normally haps under the hood inside Learner.update())
batch = learner_eager._convert_batch_type(batch)
batch = {pid: {k:v for k,v in b.items()} for pid, b in batch.items()}

print(f"Torch dynamo explain output to check if graphs hav any breaks:")
print(f"TorchLearner._uncompiled_compute_gradients:")

import torch._dynamo as dynamo

dynamo_explanation = dynamo.explain(
    learner_eager._uncompiled_compute_gradients, batch)

print(dynamo_explanation[5])

print("~" * 10)

print(f"TorchRLModule.forward_train:")

dynamo_explanation = dynamo.explain(
    learner_eager._module._rl_modules["default_policy"].forward_train, batch["default_policy"])

print(dynamo_explanation[5])

print("~" * 10)

print(f"TorchRLModule.forward_exploration:")

dynamo_explanation = dynamo.explain(
    learner_eager._module._rl_modules["default_policy"].forward_exploration, batch["default_policy"])

print(dynamo_explanation[5])

print("~" * 10)