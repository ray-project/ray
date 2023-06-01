import gymnasium as gym
import numpy as np
import tempfile

import ray
from ray import tune
from ray.air import Checkpoint, session, ScalingConfig, RunConfig, FailureConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.learner.learner_group_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.examples.datasets.dataset_utils import convert_json_sample_batch_to_df
from ray.train.torch import TorchTrainer

GYM_ENV_NAME = "CartPole-v1"
GYM_ENV = gym.make(GYM_ENV_NAME)


def train_ppo_module_with_bc_finetune(dataset: ray.data.Dataset) -> str:
    """Train a PPO module with BC finetuning on dataset.

    Args:
        dataset: The dataset to train on.
    
    Returns:
        The path to the checkpoint of the trained module.
    """
    def train_func(config):
        module_spec = config["module_spec"]
        available_gpus = ray.get_gpu_ids()
        scaling_config = LearnerGroupScalingConfig(
            num_workers=session.get_world_size(),
            num_cpus_per_worker=1,
            num_gpus_per_worker=bool(available_gpus),
        )
        learner = BCTorchLearner(
            module_spec=module_spec,
            optimizer_config={"lr": config["lr"]},
            learner_group_scaling_config=scaling_config,
        )
        learner.build()
        ds = session.get_dataset_shard("train")

        num_steps_trained = 0
        for epoch in range(config["num_epochs"]):
            for batch in ds.iter_batches(batch_size=config["batch_size"]):
                batch_dict = {}
                for key in batch.columns:
                    batch_dict[key] = np.array(batch[key].tolist())
                sample_batch: MultiAgentBatch = SampleBatch(batch_dict).as_multi_agent()
                stats = learner.update(batch=sample_batch)
                num_steps_trained += sample_batch.count
            ckpt_dir = tempfile.mkdtemp()
            learner.save_state(ckpt_dir)
            ckpt = Checkpoint.from_directory(ckpt_dir)
            session.report(
                metrics={
                        "epoch": epoch,
                        "num_steps_trained": num_steps_trained,
                        "loss": stats["__all__"]["total_loss"],
                    },
                checkpoint=ckpt
            )

    module_spec = SingleAgentRLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=GYM_ENV.observation_space,
        action_space=GYM_ENV.action_space,
        model_config_dict={"fcnet_hiddens": [64, 64]},
        catalog_class=PPOCatalog,
    )
    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "num_epochs": 100,
            "module_spec": module_spec,
            "lr": 1e-3,
            "batch_size": 128,
        },
        scaling_config=ScalingConfig(num_workers=1),
        datasets={"train": dataset},
        run_config=RunConfig(verbose=1),
    )
    result = trainer.fit()
    return result.checkpoint.path + "/module_state/default_policy"


def train_ppo_agent_from_checkpointed_module(ckpt_path: str):

    module_spec_from_ckpt = SingleAgentRLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=GYM_ENV.observation_space,
        action_space=GYM_ENV.action_space,
        model_config_dict={"fcnet_hiddens": [64, 64]},
        catalog_class=PPOCatalog,
        load_state_path=ckpt_path
    )

    config = (  
            PPOConfig()
            .training(_enable_learner_api=True)
            .rl_module(_enable_rl_module_api=True, 
                       rl_module_spec=module_spec_from_ckpt)
            .environment(GYM_ENV_NAME)
        )

    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=RunConfig(
            stop={"training_iteration": 10},
            failure_config=FailureConfig(fail_fast="raise"),
            verbose=2
        ),
    )
    tuner.fit()


if __name__ == "__main__":
    ray.init()

    ray.data.set_progress_bars(False)

    # Read a directory of files in remote storage.
    dataset_path = "../../tests/data/cartpole/large.json"
    df = convert_json_sample_batch_to_df(dataset_path)
    ds = ray.data.from_pandas(df)

    # train a PPO Module with BC finetuning
    module_checkpoint_path = train_ppo_module_with_bc_finetune(ds)
    train_ppo_agent_from_checkpointed_module(module_checkpoint_path)
