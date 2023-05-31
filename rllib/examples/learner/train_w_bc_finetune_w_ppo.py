import gymnasium as gym

import ray
from ray.air import session, ScalingConfig, RunConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.learner.learner_group_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.examples.datasets.dataset_utils import convert_json_sample_batch_to_df
from ray.train.torch import TorchTrainer

ray.init()

def train_func(config):
    module_spec = config["module_spec"]
    available_gpus = ray.get_gpu_ids()
    scaling_config = LearnerGroupScalingConfig(num_workers=session.get_world_size(),
                                               num_cpus_per_worker=1,
                                               num_gpus_per_worker=bool(available_gpus))
    learner = BCTorchLearner(module_spec=module_spec,
                             optimizer_config={"lr": config["lr"]},
                             learner_group_scaling_config=scaling_config,
                            )
    ds = session.get_dataset_shard("train")
    for epoch in range(config["num_epochs"]):
        for batch in ds.iter_batches(batch_size=config["batch_size"]):

            stats = learner.update(batch=batch)
        session.report({"epoch": epoch, **stats})


# Read a directory of files in remote storage.
dataset_path = "../../tests/data/cartpole/large.json"
df = convert_json_sample_batch_to_df(dataset_path)
ds = ray.data.from_pandas(df)

def convert_to_sample_batch(batch):
    return SampleBatch(batch)

ds = ds.map_batches(convert_to_sample_batch)
import ipdb; ipdb.set_trace()

for batch in ds.iter_batches(batch_size=2):
    import ipdb; ipdb.set_trace()
    print(batch)

# env = gym.make("CartPole-v1")


# module_spec = SingleAgentRLModuleSpec(module_class=PPOTorchRLModule,
#                                       observation_space=env.observation_space,
#                                       action_space=env.action_space,
#                                       model_config_dict={"fcnet_hiddens": [64, 64]})


# trainer = TorchTrainer(
#     train_func,
#     train_loop_config={"num_epochs": 4, "module_spec": module_spec, "lr": 1e-3, 
#                        "batch_size": 128},
#     scaling_config=ScalingConfig(num_workers=1),
#     datasets={"train": ds},
#     run_config=RunConfig(verbose=0)
# )
# result = trainer.fit()