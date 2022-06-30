
from pathlib import Path
import pandas as pd
import numpy as np
import time

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from datetime import datetime


def convert_to_df(sample_batch_rows):
    # sample_batch_rows is a panda dataframe where each row represents a SampleBatch
    # we intentionally remove next_obs as it has to be normally inferred from obs[1:]
    df = {
        SampleBatch.EPS_ID: [],
        SampleBatch.OBS: [],
        SampleBatch.ACTIONS: [],
        SampleBatch.REWARDS: [],
        SampleBatch.DONES: [],
        SampleBatch.ACTION_PROB: [],
        "timestamp": []
    }

    """
    Concatenate the SampleBatch data in each row into a single dataframe:
    Loop through each row of the panda dataframe and append each column to the corresponding list in df. For those that are numpy arrays, convert them to lists before appending. In the end convert this batch back to a panda dataframe.
    """
    for _, sample_batch_row in sample_batch_rows.iterrows():
        for key in df:
            if key == "timestamp":
                continue
            if isinstance(sample_batch_row[key][0], np.ndarray) and sample_batch_row[key][0].dtype != np.dtype('object'):
                df[key].extend(map(lambda x: x.tolist(), sample_batch_row[key]))
            else:
                df[key] += sample_batch_row[key].tolist()

        # we pretend that this dataset was created elsewhere from interactions in real-world and we need timestamps to construct a temporaliy consistent epsiode
        for _ in range(len(sample_batch_row[SampleBatch.OBS])):
            df["timestamp"].append(time.time())
            time.sleep(0.001)
    df = pd.DataFrame(df)
    return df

import ray
ray.init(local_mode=True)
###### convert the dataset to a panda data frame
# data_file = 'rllib/tests/data/cartpole/large.json'
data_file = "s3://air-example-data/rllib/cartpole/large.json"
ds = ray.data.read_json(data_file)
ds = ds.map_batches(convert_to_df)
ds = ds.random_shuffle()
df = ds.to_pandas()

print(df)

"""here we have a dataset in panda format that is shuffled and has timestamps
we want to convert this to a dataset that conforms to the format of the RLlib dataset
"""
def episode_aggregator(b):
    sorted_batch = b.sort_values(by="timestamp")

    obs = np.stack(sorted_batch[SampleBatch.OBS], 0)
    obs_shape = obs.shape[1:]
    actions = np.stack(sorted_batch[SampleBatch.ACTIONS], 0)
    new_obs = np.concatenate([obs[:-1], np.zeros((1,) + obs_shape)], 0)
    rewards = np.stack(sorted_batch[SampleBatch.REWARDS], 0)
    dones = np.stack(sorted_batch[SampleBatch.DONES], 0)
    action_prob = np.stack(sorted_batch[SampleBatch.ACTION_PROB], 0)
    t = np.stack(sorted_batch["timestamp"], 0)

    return pd.DataFrame({
        "type": ["SampleBatch"],
        SampleBatch.OBS: [obs],
        SampleBatch.NEXT_OBS: [new_obs],
        SampleBatch.ACTIONS: [actions],
        SampleBatch.REWARDS: [rewards],
        SampleBatch.DONES: [dones],
        SampleBatch.T: [t],
        SampleBatch.ACTION_PROB: [action_prob],
    })

ds = ray.data.from_pandas(df)
episode_ds = ds.groupby("eps_id").map_groups(episode_aggregator)

train_ds, eval_ds = episode_ds.split_proportionately([0.8])
output_dir = Path('cartpole_ds').absolute()
output_dir.mkdir(exist_ok=True, parents=True)

train_ds.to_pandas().to_json(str(output_dir / 'train.json'), orient='records', lines=True)
train_ds.to_pandas().to_json(str(output_dir / 'eval.json'), orient='records', lines=True)


"""
This dataset is now in the expected format for the RLlib dataset.
We save the dataset into a json file for later use.
We now call CRR to train the model with the dataset but evaluate using the gym simulator. 
"""

from ray.rllib.algorithms.crr import CRRConfig, CRR
from ray.rllib.utils.replay_buffers import MultiAgentReplayBuffer
from ray import tune

from ray.rllib.offline.estimators import ( ImportanceSampling,
    WeightedImportanceSampling, DirectMethod, DoublyRobust)


config = (
    CRRConfig()
    .environment(env="CartPole-v0")
    .framework("torch")
    .offline_data(
        input_=[str(output_dir / 'train.json')],
        actions_in_input_normalized=True,
        off_policy_estimation_methods={
            "is": {"type": ImportanceSampling},
            "wis": {"type": WeightedImportanceSampling},
            # "dm_fqe": {"type": DirectMethod},
            # "dr_fqe": {"type": DoublyRobust},
        },
    )
    .training(
        train_batch_size=2048,
        critic_hidden_activation='tanh',
        critic_hiddens=[128, 128, 128],
        # critic_lr=0.0003,
        actor_hidden_activation='tanh',
        actor_hiddens=[128, 128, 128],
        # actor_lr=0.0003,
        twin_q=True,
        target_update_grad_intervals=1,
        tau=0.0005,
        replay_buffer_config={
            "type": MultiAgentReplayBuffer,
            "learning_starts": 0,
            "capacity": 100000,
        },
        temperature=1.0,
        weight_type="bin",
        advantage_type="expectation",
        n_action_sample=4,
        max_weight=20.0,
    )
    .evaluation(
        evaluation_interval=1,
        evaluation_num_workers=1,
        evaluation_duration=10,
        evaluation_duration_unit="episodes",
        evaluation_parallel_to_training=True,
        evaluation_config={
            "input": "dataset",
            "input_config": {"format": "json", "path": str(output_dir / 'eval.json')},
            "metrics_episode_collection_timeout_s": 1800,
        },
    )
    .rollouts(num_rollout_workers=0)
)

# specify the criteria used for stopping training
stop_criteria = {"training_iteration": 500}
# stop_criteria = {"evaluation/episode_reward_mean": 200}

tune.run(CRR, stop=stop_criteria, config=config.to_dict())
ray.shutdown()




