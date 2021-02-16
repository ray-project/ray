import unittest

import ray
import ray.rllib.agents.dqn as dqn
from ray.rllib.agents.dqn.dqn_torch_model import DQNTorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class TorchRNNModel(DQNTorchModel):
    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 **kwargs):
        kwargs.pop("dueling", None)
        kwargs.pop("q_hiddens", ())
        super().__init__(obs_space, action_space, None, model_config,
                         name, q_hiddens=(), dueling=False, **kwargs)

        self.num_outputs = action_space.n

        self.obs_size = get_preprocessor(obs_space)(obs_space).size
        self.fc_size = 64
        self.lstm_state_size = 8

        # Build the Module from fc + LSTM + 2xfc (action + value outs).
        self.fc1 = nn.Linear(self.obs_size, self.fc_size)
        self.action_branch = nn.Linear(self.fc_size, self.num_outputs)
        self.value_branch = nn.Linear(self.fc_size, 1)
        # Holds the current "base" output (before logits layer).
        self._features = None

    def get_initial_state(self):
        # TODO: (sven): Get rid of `get_initial_state` once Trajectory
        #  View API is supported across all of RLlib.
        # Place hidden states on same device as model.
        h = [
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0),
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0)
        ]
        return h

    def value_function(self):
        assert self._features is not None, "must call forward() first"
        return torch.reshape(self.value_branch(self._features), [-1])

    def forward(self, input_dict, state, seq_lens):
        """Feeds `inputs` (B x T x ..) through the Gru Unit.

        Returns the resulting outputs as a sequence (B x T x ...).
        Values are stored in self._cur_value in simple (B) shape (where B
        contains both the B and T dims!).

        Returns:
            NN Outputs (B x T x ...) as sequence.
            The state batches as a List of two items (c- and h-states).
        """
        self._features = nn.functional.relu(self.fc1(input_dict["obs"]))
        #action_out = self.action_branch(self._features)
        return self._features, state

    def get_state_value(self, model_out):
        return self.value_branch(model_out)

    def get_q_value_distributions(self, model_out):
        action_values = self.action_branch(model_out)
        logits = torch.unsqueeze(torch.ones_like(action_values), -1)
        return action_values, logits, logits


class TestR2D2(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_r2d2_compilation(self):
        """Test whether a R2D2Trainer can be built on all frameworks."""
        config = dqn.R2D2_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["model"]["use_lstm"] = True  # Wrap with an LSTM.
        config["model"]["max_seq_len"] = 20
        config["model"]["fcnet_hiddens"] = [32]
        config["model"]["lstm_cell_size"] = 64

        #config["n_step"] = 5

        config["burn_in"] = 20
        config["zero_init_states"] = True

        config["dueling"] = False
        config["lr"] = 1e-4
        config["exploration_config"]["epsilon_timesteps"] = 100000

        num_iterations = 100#TODO

        for _ in framework_iterator(config, frameworks="torch"):#TODO
            trainer = dqn.R2D2Trainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

            check_compute_single_action(trainer, include_state=True)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
