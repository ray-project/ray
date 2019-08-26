from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""An example of customizing PPO to leverage a centralized critic.

Here the model and policy are hard-coded to implement a centralized critic
for TwoStepGame, but you can adapt this for your own use cases.

Compared to simply running `twostep_game.py --run=PPO`, this centralized
critic version reaches vf_explained_variance=1.0 more stably since it takes
into account the opponent actions as well as the policy's. Note that this is
also using two independent policies instead of weight-sharing with one.

See also: centralized_critic_2.py for a simpler approach that instead
modifies the environment.
"""

import argparse
import numpy as np

from ray import tune
from ray.rllib.agents.ppo.ppo import PPOTrainer
from ray.rllib.agents.ppo.ppo_policy import PPOTFPolicy, KLCoeffMixin, \
    PPOLoss, BEHAVIOUR_LOGITS
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.examples.twostep_game import TwoStepGame
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import LearningRateSchedule, \
    EntropyCoeffSchedule, ACTION_LOGP
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet_v2 import FullyConnectedNetwork
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

OPPONENT_OBS = "opponent_obs"
OPPONENT_ACTION = "opponent_action"

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=100000)


class CentralizedCriticModel(TFModelV2):
    """Multi-agent model that implements a centralized VF."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(CentralizedCriticModel, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)
        # Base of the model
        self.model = FullyConnectedNetwork(obs_space, action_space,
                                           num_outputs, model_config, name)
        self.register_variables(self.model.variables())

        # Central VF maps (obs, opp_ops, opp_act) -> vf_pred
        obs = tf.keras.layers.Input(shape=(6, ), name="obs")
        opp_obs = tf.keras.layers.Input(shape=(6, ), name="opp_obs")
        opp_act = tf.keras.layers.Input(shape=(2, ), name="opp_act")
        concat_obs = tf.keras.layers.Concatenate(axis=1)(
            [obs, opp_obs, opp_act])
        central_vf_dense = tf.keras.layers.Dense(
            16, activation=tf.nn.tanh, name="c_vf_dense")(concat_obs)
        central_vf_out = tf.keras.layers.Dense(
            1, activation=None, name="c_vf_out")(central_vf_dense)
        self.central_vf = tf.keras.Model(
            inputs=[obs, opp_obs, opp_act], outputs=central_vf_out)
        self.register_variables(self.central_vf.variables)

    def forward(self, input_dict, state, seq_lens):
        return self.model.forward(input_dict, state, seq_lens)

    def central_value_function(self, obs, opponent_obs, opponent_actions):
        return tf.reshape(
            self.central_vf(
                [obs, opponent_obs,
                 tf.one_hot(opponent_actions, 2)]), [-1])

    def value_function(self):
        return self.model.value_function()  # not used


class CentralizedValueMixin(object):
    """Add methods to evaluate the central value function from the model."""

    def __init__(self):
        self.central_value_function = self.model.central_value_function(
            self.get_placeholder(SampleBatch.CUR_OBS),
            self.get_placeholder(OPPONENT_OBS),
            self.get_placeholder(OPPONENT_ACTION))

    def compute_central_vf(self, obs, opponent_obs, opponent_actions):
        feed_dict = {
            self.get_placeholder(SampleBatch.CUR_OBS): obs,
            self.get_placeholder(OPPONENT_OBS): opponent_obs,
            self.get_placeholder(OPPONENT_ACTION): opponent_actions,
        }
        return self.get_session().run(self.central_value_function, feed_dict)


# Grabs the opponent obs/act and includes it in the experience train_batch,
# and computes GAE using the central vf predictions.
def centralized_critic_postprocessing(policy,
                                      sample_batch,
                                      other_agent_batches=None,
                                      episode=None):
    if policy.loss_initialized():
        assert sample_batch["dones"][-1], \
            "Not implemented for train_batch_mode=truncate_episodes"
        assert other_agent_batches is not None
        [(_, opponent_batch)] = list(other_agent_batches.values())

        # also record the opponent obs and actions in the trajectory
        sample_batch[OPPONENT_OBS] = opponent_batch[SampleBatch.CUR_OBS]
        sample_batch[OPPONENT_ACTION] = opponent_batch[SampleBatch.ACTIONS]

        # overwrite default VF prediction with the central VF
        sample_batch[SampleBatch.VF_PREDS] = policy.compute_central_vf(
            sample_batch[SampleBatch.CUR_OBS], sample_batch[OPPONENT_OBS],
            sample_batch[OPPONENT_ACTION])
    else:
        # policy hasn't initialized yet, use zeros
        sample_batch[OPPONENT_OBS] = np.zeros_like(
            sample_batch[SampleBatch.CUR_OBS])
        sample_batch[OPPONENT_ACTION] = np.zeros_like(
            sample_batch[SampleBatch.ACTIONS])
        sample_batch[SampleBatch.VF_PREDS] = np.zeros_like(
            sample_batch[SampleBatch.ACTIONS], dtype=np.float32)

    train_batch = compute_advantages(
        sample_batch,
        0.0,
        policy.config["gamma"],
        policy.config["lambda"],
        use_gae=policy.config["use_gae"])
    return train_batch


# Copied from PPO but optimizing the central value function
def loss_with_central_critic(policy, model, dist_class, train_batch):
    CentralizedValueMixin.__init__(policy)

    logits, state = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    policy.loss_obj = PPOLoss(
        policy.action_space,
        dist_class,
        model,
        train_batch[Postprocessing.VALUE_TARGETS],
        train_batch[Postprocessing.ADVANTAGES],
        train_batch[SampleBatch.ACTIONS],
        train_batch[BEHAVIOUR_LOGITS],
        train_batch[ACTION_LOGP],
        train_batch[SampleBatch.VF_PREDS],
        action_dist,
        policy.central_value_function,
        policy.kl_coeff,
        tf.ones_like(train_batch[Postprocessing.ADVANTAGES], dtype=tf.bool),
        entropy_coeff=policy.entropy_coeff,
        clip_param=policy.config["clip_param"],
        vf_clip_param=policy.config["vf_clip_param"],
        vf_loss_coeff=policy.config["vf_loss_coeff"],
        use_gae=policy.config["use_gae"],
        model_config=policy.config["model"])

    return policy.loss_obj.loss


def setup_mixins(policy, obs_space, action_space, config):
    # copied from PPO
    KLCoeffMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    # hack: put in a noop VF so some of the inherited PPO code runs
    policy.value_function = tf.zeros(
        tf.shape(policy.get_placeholder(SampleBatch.CUR_OBS))[0])


def central_vf_stats(policy, train_batch, grads):
    # Report the explained variance of the central value function.
    return {
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS],
            policy.central_value_function),
    }


CCPPO = PPOTFPolicy.with_updates(
    name="CCPPO",
    postprocess_fn=centralized_critic_postprocessing,
    loss_fn=loss_with_central_critic,
    before_loss_init=setup_mixins,
    grad_stats_fn=central_vf_stats,
    mixins=[
        LearningRateSchedule, EntropyCoeffSchedule, KLCoeffMixin,
        CentralizedValueMixin
    ])

CCTrainer = PPOTrainer.with_updates(name="CCPPOTrainer", default_policy=CCPPO)

if __name__ == "__main__":
    args = parser.parse_args()
    ModelCatalog.register_custom_model("cc_model", CentralizedCriticModel)
    tune.run(
        CCTrainer,
        stop={
            "timesteps_total": args.stop,
            "episode_reward_mean": 7.99,
        },
        config={
            "env": TwoStepGame,
            "batch_mode": "complete_episodes",
            "num_workers": 0,
            "multiagent": {
                "policies": {
                    "pol1": (None, TwoStepGame.observation_space,
                             TwoStepGame.action_space, {}),
                    "pol2": (None, TwoStepGame.observation_space,
                             TwoStepGame.action_space, {}),
                },
                "policy_mapping_fn": tune.function(
                    lambda x: "pol1" if x == 0 else "pol2"),
            },
            "model": {
                "custom_model": "cc_model",
            },
        })
