import gym
from gym.spaces import Discrete, Box
import numpy as np
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()


class TDModel(nn.Module):
    """Transition Dynamics Model (FC Network with Weight Norm)"""

    def __init__(
        self,
        input_size,
        output_size,
        hidden_layers=(512, 512),
        hidden_nonlinearity=None,
        output_nonlinearity=None,
        weight_normalization=False,
        use_bias=True,
    ):

        super().__init__()
        assert len(hidden_layers) >= 1

        if not hidden_nonlinearity:
            hidden_nonlinearity = nn.ReLU

        if weight_normalization:
            weight_norm = nn.utils.weight_norm

        self.layers = []
        cur_size = input_size
        for h_size in hidden_layers:
            layer = nn.Linear(cur_size, h_size, bias=use_bias)
            if weight_normalization:
                layer = weight_norm(layer)
            self.layers.append(layer)
            if hidden_nonlinearity:
                self.layers.append(hidden_nonlinearity())
            cur_size = h_size

        layer = nn.Linear(cur_size, output_size, bias=use_bias)
        if weight_normalization:
            layer = weight_norm(layer)
        self.layers.append(layer)
        if output_nonlinearity:
            self.layers.append(output_nonlinearity())

        self.model = nn.Sequential(*self.layers)

    def forward(self, x):
        return self.model(x)


if torch:

    class TDDataset(torch.utils.data.Dataset):
        def __init__(self, dataset: SampleBatchType, norms):
            self.count = dataset.count
            obs = dataset[SampleBatch.CUR_OBS]
            actions = dataset[SampleBatch.ACTIONS]
            delta = dataset[SampleBatch.NEXT_OBS] - obs

            if norms:
                obs = normalize(obs, norms[SampleBatch.CUR_OBS])
                actions = normalize(actions, norms[SampleBatch.ACTIONS])
                delta = normalize(delta, norms["delta"])

            self.x = np.concatenate([obs, actions], axis=1)
            self.y = delta

        def __len__(self):
            return self.count

        def __getitem__(self, index):
            return self.x[index], self.y[index]


def normalize(data_array, stats):
    mean, std = stats
    return (data_array - mean) / (std + 1e-10)


def denormalize(data_array, stats):
    mean, std = stats
    return data_array * (std + 1e-10) + mean


def mean_std_stats(dataset: SampleBatchType):
    norm_dict = {}
    obs = dataset[SampleBatch.CUR_OBS]
    act = dataset[SampleBatch.ACTIONS]
    delta = dataset[SampleBatch.NEXT_OBS] - obs

    norm_dict[SampleBatch.CUR_OBS] = (np.mean(obs, axis=0), np.std(obs, axis=0))
    norm_dict[SampleBatch.ACTIONS] = (np.mean(act, axis=0), np.std(act, axis=0))
    norm_dict["delta"] = (np.mean(delta, axis=0), np.std(delta, axis=0))

    return norm_dict


def process_samples(samples: SampleBatchType):
    filter_keys = [SampleBatch.CUR_OBS, SampleBatch.ACTIONS, SampleBatch.NEXT_OBS]
    filtered = {}
    for key in filter_keys:
        filtered[key] = samples[key]
    return SampleBatch(filtered)


class DynamicsEnsembleCustomModel(TorchModelV2, nn.Module):
    """Represents an ensemble of transition dynamics (TD) models."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        """Initializes a DynamicEnsemble object."""
        nn.Module.__init__(self)
        if isinstance(action_space, Discrete):
            input_space = gym.spaces.Box(
                obs_space.low[0],
                obs_space.high[0],
                shape=(obs_space.shape[0] + action_space.n,),
            )
        elif isinstance(action_space, Box):
            input_space = gym.spaces.Box(
                obs_space.low[0],
                obs_space.high[0],
                shape=(obs_space.shape[0] + action_space.shape[0],),
            )
        else:
            raise NotImplementedError
        super(DynamicsEnsembleCustomModel, self).__init__(
            input_space, action_space, num_outputs, model_config, name
        )

        # Keep the original Env's observation space for possible clipping.
        self.env_obs_space = obs_space

        self.num_models = model_config["ensemble_size"]
        self.max_epochs = model_config["train_epochs"]
        self.lr = model_config["lr"]
        self.valid_split = model_config["valid_split_ratio"]
        self.batch_size = model_config["batch_size"]
        self.normalize_data = model_config["normalize_data"]
        self.normalizations = {}
        self.dynamics_ensemble = [
            TDModel(
                input_size=input_space.shape[0],
                output_size=obs_space.shape[0],
                hidden_layers=model_config["fcnet_hiddens"],
                hidden_nonlinearity=nn.ReLU,
                output_nonlinearity=None,
                weight_normalization=True,
            )
            for _ in range(self.num_models)
        ]

        for i in range(self.num_models):
            self.add_module("TD-model-" + str(i), self.dynamics_ensemble[i])
        self.replay_buffer_max = 10000
        self.replay_buffer = None
        self.optimizers = [
            torch.optim.Adam(self.dynamics_ensemble[i].parameters(), lr=self.lr)
            for i in range(self.num_models)
        ]
        # Metric Reporting
        self.metrics = {}
        self.metrics[STEPS_SAMPLED_COUNTER] = 0

        # For each worker, choose a random model to choose trajectories from
        worker_index = get_global_worker().worker_index
        self.sample_index = int((worker_index - 1) / self.num_models)
        self.global_itr = 0

    def forward(self, x):
        """Outputs the delta between next and current observation."""
        return self.dynamics_ensemble[self.sample_index](x)

    # Loss functions for each TD model in Ensemble (Standard L2 Loss)
    def loss(self, x, y):
        xs = torch.chunk(x, self.num_models)
        ys = torch.chunk(y, self.num_models)
        return [
            torch.mean(torch.pow(self.dynamics_ensemble[i](xs[i]) - ys[i], 2.0))
            for i in range(self.num_models)
        ]

    # Fitting Dynamics Ensembles per MBMPO Iter
    def fit(self):
        # Add env samples to Replay Buffer
        local_worker = get_global_worker()
        for pid, pol in local_worker.policy_map.items():
            pol.view_requirements[SampleBatch.NEXT_OBS].used_for_training = True
        new_samples = local_worker.sample()
        # Initial Exploration of 8000 timesteps
        if not self.global_itr:
            extra = local_worker.sample()
            new_samples.concat(extra)

        # Process Samples
        new_samples = process_samples(new_samples)
        if isinstance(self.action_space, Discrete):
            act = new_samples["actions"]
            new_act = np.zeros((act.size, act.max() + 1))
            new_act[np.arange(act.size), act] = 1
            new_samples["actions"] = new_act.astype("float32")

        if not self.replay_buffer:
            self.replay_buffer = new_samples
        else:
            self.replay_buffer = self.replay_buffer.concat(new_samples)

        # Keep Replay Buffer Size Constant
        self.replay_buffer = self.replay_buffer.slice(
            start=-self.replay_buffer_max, end=None
        )

        if self.normalize_data:
            self.normalizations = mean_std_stats(self.replay_buffer)

        # Keep Track of Timesteps from Real Environment Timesteps Sampled
        self.metrics[STEPS_SAMPLED_COUNTER] += new_samples.count

        # Create Train and Val Datasets for each TD model
        train_loaders = []
        val_loaders = []
        for i in range(self.num_models):
            t, v = self.split_train_val(self.replay_buffer)
            train_loaders.append(
                torch.utils.data.DataLoader(
                    TDDataset(t, self.normalizations),
                    batch_size=self.batch_size,
                    shuffle=True,
                )
            )
            val_loaders.append(
                torch.utils.data.DataLoader(
                    TDDataset(v, self.normalizations), batch_size=v.count, shuffle=False
                )
            )

        # List of which models in ensemble to train
        indexes = list(range(self.num_models))

        valid_loss_roll_avg = None
        roll_avg_persitency = 0.95

        def convert_to_str(lst):
            return " ".join([str(elem) for elem in lst])

        device = next(iter(self.dynamics_ensemble[i].parameters()))[0].device
        for epoch in range(self.max_epochs):
            # Training
            for data in zip(*train_loaders):
                x = torch.cat([d[0] for d in data], dim=0).to(device)
                y = torch.cat([d[1] for d in data], dim=0).to(device)
                train_losses = self.loss(x, y)
                for ind in indexes:
                    self.optimizers[ind].zero_grad()
                    train_losses[ind].backward()
                    self.optimizers[ind].step()

                for ind in range(self.num_models):
                    train_losses[ind] = train_losses[ind].detach().cpu().numpy()

            # Validation
            val_lists = []
            for data in zip(*val_loaders):
                x = torch.cat([d[0] for d in data], dim=0).to(device)
                y = torch.cat([d[1] for d in data], dim=0).to(device)
                val_losses = self.loss(x, y)
                val_lists.append(val_losses)

                for ind in indexes:
                    self.optimizers[ind].zero_grad()

                for ind in range(self.num_models):
                    val_losses[ind] = val_losses[ind].detach().cpu().numpy()

            val_lists = np.array(val_lists)
            avg_val_losses = np.mean(val_lists, axis=0)

            if valid_loss_roll_avg is None:
                # Make sure that training doesnt end first epoch
                valid_loss_roll_avg = 1.5 * avg_val_losses
                valid_loss_roll_avg_prev = 2.0 * avg_val_losses

            valid_loss_roll_avg = (
                roll_avg_persitency * valid_loss_roll_avg
                + (1.0 - roll_avg_persitency) * avg_val_losses
            )

            print(
                "Training Dynamics Ensemble - Epoch #%i:"
                "Train loss: %s, Valid Loss: %s,  Moving Avg Valid Loss: %s"
                % (
                    epoch,
                    convert_to_str(train_losses),
                    convert_to_str(avg_val_losses),
                    convert_to_str(valid_loss_roll_avg),
                )
            )
            for i in range(self.num_models):
                if (
                    valid_loss_roll_avg_prev[i] < valid_loss_roll_avg[i]
                    or epoch == self.max_epochs - 1
                ) and i in indexes:
                    indexes.remove(i)
                    print("Stopping Training of Model %i" % i)
            valid_loss_roll_avg_prev = valid_loss_roll_avg
            if len(indexes) == 0:
                break

        self.global_itr += 1
        # Returns Metric Dictionary
        return self.metrics

    def split_train_val(self, samples: SampleBatchType):
        dataset_size = samples.count
        indices = np.arange(dataset_size)
        np.random.shuffle(indices)
        split_idx = int(dataset_size * (1 - self.valid_split))
        idx_train = indices[:split_idx]
        idx_test = indices[split_idx:]

        train = {}
        val = {}
        for key in samples.keys():
            train[key] = samples[key][idx_train, :]
            val[key] = samples[key][idx_test, :]
        return SampleBatch(train), SampleBatch(val)

    def predict_model_batches(self, obs, actions, device=None):
        """Used by worker who gather trajectories via TD models."""
        pre_obs = obs
        if self.normalize_data:
            obs = normalize(obs, self.normalizations[SampleBatch.CUR_OBS])
            actions = normalize(actions, self.normalizations[SampleBatch.ACTIONS])
        x = np.concatenate([obs, actions], axis=-1)
        x = convert_to_torch_tensor(x, device=device)
        delta = self.forward(x).detach().cpu().numpy()
        if self.normalize_data:
            delta = denormalize(delta, self.normalizations["delta"])
        new_obs = pre_obs + delta
        clipped_obs = np.clip(new_obs, self.env_obs_space.low, self.env_obs_space.high)
        return clipped_obs

    def set_norms(self, normalization_dict):
        self.normalizations = normalization_dict
