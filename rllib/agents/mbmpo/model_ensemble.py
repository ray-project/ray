import gym
from gym.spaces import Discrete, Box
import numpy as np
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override
from ray.rllib.execution.replay_buffer import ReplayBuffer
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER, LEARNER_INFO, _get_shared_metrics
from ray.rllib.utils.types import EnvType, AgentID, PolicyID, EnvConfigDict, \
    ModelConfigDict, TrainerConfigDict, SampleBatchType, ModelWeights, \
    ModelGradients, MultiAgentPolicyConfigDict
from ray.rllib.utils.torch_ops import convert_to_torch_tensor

torch, nn = try_import_torch()


class TDModel(nn.Module):
    """Simple Transition Dynamics Model
    """
    def __init__(self,
                 input_size,
                 output_size, 
                 hidden_layers=[512, 512],
                 hidden_nonlinearity=nn.ReLU,
                 output_nonlinearity=None,
                 weight_normalization=True,
                 use_bias=True):

        super().__init__()
        assert len(hidden_layers) >=1

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
            cur_size=h_size

        layer = nn.Linear(cur_size, output_size, bias=use_bias)
        if weight_normalization:
            layer = weight_norm(layer)
        self.layers.append(layer)
        if output_nonlinearity:
            self.layers.append(output_nonlinearity())

        self.model = nn.Sequential(*self.layers)

    def forward(self, x):
        return self.model(x)

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

class DynamicsEnsembleCustomModel(TorchModelV2, nn.Module):
    """Represents a Transition Dyamics ensemble
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        """Initializes a DynamicEnsemble object.
        """

        nn.Module.__init__(self)
        if isinstance(action_space, Discrete):
            input_space = gym.spaces.Box(
                obs_space.low[0],
                obs_space.high[0],
                shape=(obs_space.shape[0] + action_space.n, ))
        elif isinstance(action_space, Box):
            input_space = gym.spaces.Box(
                obs_space.low[0],
                obs_space.high[0],
                shape=(obs_space.shape[0] + action_space.shape[0], ))
        super(DynamicsEnsembleCustomModel, self).__init__(input_space, action_space, num_outputs, model_config, name)

        
        self.num_models = model_config["model_ensemble_size"]
        self.model_lr = model_config["model_lr"]
        self.max_epochs = model_config["model_train_epochs"]
        self.lr = model_config["model_lr"]
        self.valid_split = model_config["valid_split_ratio"]
        self.batch_size = model_config["model_batch_size"]
        self.normalize_data = model_config["normalize_data"]
        self.normalizations = {}
        self.dynamics_ensemble = [TDModel(input_size=input_space.shape[0],
                                    output_size=obs_space.shape[0],
                                    hidden_layers=model_config["model_hiddens"],
                                    hidden_nonlinearity=nn.ReLU,
                                    output_nonlinearity=None,
                                    weight_normalization=True
                                  ) for _ in range(self.num_models)]
        
        for i in range(self.num_models):
            self.add_module("TD-model-"+str(i), self.dynamics_ensemble[i])
        self.replay_buffer_max = 100000
        self.replay_buffer = None
        self.optimizers = [torch.optim.Adam(self.dynamics_ensemble[i].parameters()
                            , lr=self.lr) for i in range(self.num_models)]
        # Metric Reporting
        self.metrics = {}
        self.metrics[STEPS_SAMPLED_COUNTER] = 0

        # For each worker, choose a random model to choose trajectories from
        self.sample_index = np.random.randint(self.num_models)


    def forward(self, x):
        """Outputs the delta between next and current observation.
        """
        hi = self.dynamics_ensemble[self.sample_index](x)
        return hi


    # Loss functions for each TD model in Ensemble (Standard L2 Loss)
    def loss(self, x,y):
        xs = torch.chunk(x, self.num_models)
        ys = torch.chunk(y, self.num_models)
        return [torch.mean(
        torch.sum(
            torch.pow(self.dynamics_ensemble[i](xs[i]) - ys[i], 2.0), dim=-1)) for i in range(self.num_models)]

    # Fitting Dynamics Ensembles per MBMPO Iter
    def fit(self):
        # Add env samples to Replay Buffer
        local_worker = get_global_worker()
        new_samples = local_worker.sample()
        if not self.replay_buffer:
            self.replay_buffer = new_samples
        else:
            self.replay_buffer = self.replay_buffer.concat(new_samples)

        # Keep Replay Buffer Size Constant
        self.replay_buffer = self.replay_buffer.slice(start=-self.replay_buffer_max, end=None)

        if self.normalize_data:
            self.normalizations = mean_std_stats(self.replay_buffer)

        # Keep Track of Timesteps from Real Environment Timesteps Sampled
        self.metrics[STEPS_SAMPLED_COUNTER] += new_samples.count

        # Split into Train and Validation Datasets
        dataset_size = self.replay_buffer.count
        self.replay_buffer.shuffle()
        _train = SampleBatch.slice(self.replay_buffer, 0, int((1.0-self.valid_split)*dataset_size))
        _val =SampleBatch.slice(self.replay_buffer, int((1.0-self.valid_split)*dataset_size), dataset_size)
        train_loader = torch.utils.data.DataLoader(TDDataset(_train, self.normalizations), batch_size=self.batch_size, shuffle=True)
        val_loader = torch.utils.data.DataLoader(TDDataset(_val, self.normalizations), batch_size=self.batch_size, shuffle=False)

        # List of which models in ensemble to train
        indexes = [i for i in range(self.num_models)]

        valid_loss_rolling_average = None
        rolling_average_persitency = 0.95

        def convert_to_str(lst):
            return ' '.join([str(elem.detach().cpu().numpy()) for elem in lst])

        
        for epoch in range(self.max_epochs):
            # Training
            for x,y in train_loader:
                train_losses = self.loss(x, y)
                for ind in indexes:
                    self.optimizers[ind].zero_grad()
                    train_losses[ind].backward()
                    self.optimizers[ind].step()

            # Validation
            val_lists = []
            for x,y in val_loader:
                val_losses = self.loss(x,y)
                val_lists.append(val_losses)
            val_lists = np.array(val_lists)
            avg_val_losses = np.mean(val_lists, axis=0)

            if valid_loss_rolling_average is None:
                    # Make sure that training doesnt end first epoch
                    valid_loss_rolling_average = 1.5 * avg_val_losses
                    valid_loss_rolling_average_prev = 2.0 * avg_val_losses

            valid_loss_rolling_average = rolling_average_persitency*valid_loss_rolling_average \
                                         + (1.0-rolling_average_persitency)* avg_val_losses

            print("Training Dynamics Ensemble - Epoch #%i:"
                                   "Train loss: %s, Valid Loss: %s,  Moving Avg Valid Loss: %s"
                                   %(epoch, convert_to_str(train_losses), convert_to_str(avg_val_losses), convert_to_str(valid_loss_rolling_average)))
            for i in range(self.num_models):
                if (valid_loss_rolling_average_prev[i] < valid_loss_rolling_average[i] or epoch == self.max_epochs - 1) and i in indexes:
                    indexes.remove(i)
                    print('Stopping Training of Model %i'%i)
            valid_loss_rolling_average_prev = valid_loss_rolling_average
            if(len(indexes)==0):
                break
        # Returns Metric Dictionary
        return self.metrics


    """Used by worker who gather trajectories via TD models
    """
    def predict_model_batches(self, obs, actions, device=None):
        pre_obs = obs
        if self.normalize_data:
            obs = normalize(obs, self.normalizations[SampleBatch.CUR_OBS])
            actions = normalize(actions, self.normalizations[SampleBatch.ACTIONS])
        x = np.concatenate([obs, actions], axis=-1)
        x = convert_to_torch_tensor(x, device=device)
        delta = self.forward(x).detach().numpy()
        if self.normalize_data:
            delta = denormalize(delta, self.normalizations["delta"])
        return pre_obs + delta

    def set_norms(self, normalization_dict):
        self.normalizations = normalization_dict

