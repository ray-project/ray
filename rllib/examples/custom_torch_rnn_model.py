import argparse

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.examples.cartpole_lstm import CartPoleStatelessEnv
from ray.rllib.examples.custom_keras_rnn_model import RepeatInitialEnv
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.torch.recurrent_torch_model import RecurrentTorchModel
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_torch
from ray.rllib.models import ModelCatalog
import ray.tune as tune

torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="repeat_initial")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--lstm-cell-size", type=int, default=32)


class RNNModel(RecurrentTorchModel):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        self.obs_size = get_preprocessor(obs_space)(obs_space).size
        self.rnn_hidden_dim = model_config["lstm_cell_size"]
        self.fc1 = nn.Linear(self.obs_size, self.rnn_hidden_dim)
        self.rnn = nn.GRUCell(self.rnn_hidden_dim, self.rnn_hidden_dim)
        self.fc2 = nn.Linear(self.rnn_hidden_dim, num_outputs)

        self.value_branch = nn.Linear(self.rnn_hidden_dim, 1)
        self._cur_value = None

    @override(ModelV2)
    def get_initial_state(self):
        # make hidden states on same device as model
        h = [self.fc1.weight.new(1, self.rnn_hidden_dim).zero_().squeeze(0)]
        return h

    @override(ModelV2)
    def value_function(self):
        assert self._cur_value is not None, "must call forward() first"
        return self._cur_value

    @override(RecurrentTorchModel)
    def forward_rnn(self, inputs, state, seq_lens):
        """Feeds `inputs` (B x T x ..) through the Gru Unit.

        Returns the resulting outputs as a sequence (B x T x ...).
        Values are stored in self._cur_value in simple (B) shape (where B
        contains both the B and T dims!).

        Returns:
            NN Outputs (B x T x ...) as sequence.
            The state batch as a List of one item (only one hidden state b/c
                Gru).
        """
        x = nn.functional.relu(self.fc1(inputs))
        h = state[0]
        outs = []
        for i in range(torch.max(seq_lens)):
            h = self.rnn(x[:, i], h)
            outs.append(h)
        outs = torch.stack(outs, 0)
        q = self.fc2(outs)
        self._cur_value = torch.reshape(self.value_branch(outs), [-1])
        return q, [outs[-1]]


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model("rnn", RNNModel)
    tune.register_env("repeat_initial", lambda c: RepeatInitialEnv())
    tune.register_env("cartpole_stateless", lambda c: CartPoleStatelessEnv())

    config = {
        "num_workers": 0,
        "num_envs_per_worker": 20,
        "gamma": 0.9,
        "entropy_coeff": 0.001,
        "use_pytorch": True,
        "model": {
            "custom_model": "rnn",
            "lstm_use_prev_action_reward": "store_true",
            "lstm_cell_size": args.lstm_cell_size,
            "custom_options": {}
        },
        "lr": 0.0003,
        "num_sgd_iter": 5,
        "vf_loss_coeff": 1e-5,
        "env": args.env,
    }

    #trainer = ppo.PPOTrainer(config)
    #for _ in range(100):
    #    trainer.train()

    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config=config,
    )
