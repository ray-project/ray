from gym.spaces import Discrete, MultiDiscrete, Space
import logging
import numpy as np
from typing import Dict, Optional, Union, TYPE_CHECKING

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, TensorType


if TYPE_CHECKING:
    from ray.rllib.evaluation import RolloutWorker

logger = logging.getLogger(__name__)

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()
F = None
if nn is not None:
    F = nn.functional


class NovelD(Exploration):
    """Implements NovelD exploration criterion.

    Implementation of:
    [1] NovelD: A simple yet Effective Exploration Criterion.
    Zhang, Xu, Wang, Wu, Kreutzer, Gonzales & Tian (2021).
    NeurIPS Proceedings 2021

    which is based on Random Network Distillation proposed in
    [2] Exploration By Random Network Distillation.
    Burda, Edwards, Storkey & Klimov (2018).
    7th International Conference on Learning Representations (ICLR 2019)

    Estimates the novelty of a state by a distilled network approach.
    The states novelty thereby increases at the boundary between explored
    and unexplored states. It compares the prior state novelty with the
    actual state novelty.

    The novelty difference between the prior and actual state is considered
    as intrinsic reward and added to the environment's extrinsic reward
    for policy optimization.

    Novelty is actual a very general approach and any novelty measure could be
    used. Here the distillation error is used.
    """

    def __init__(
        self,
        action_space: Space,
        *,
        framework: str,
        model: ModelV2,
        embed_dim: int = 128,
        distill_net_config: Optional[ModelConfigDict] = None,
        lr: float = 1e-3,
        alpha: float = 0.5,
        beta: float = 0.0,
        intrinsic_reward_coeff: float = 5e-3,
        normalize: bool = True,
        random_timesteps: int = 10000,
        sub_exploration: Optional[FromConfigSpec] = None,
        **kwargs,
    ):
        """Initializes a NovelD exploration scheme.

        Args:
            action_space: The action space of the environment. At
                present NovelD exploration works only with
                (Multi)Discrete action spaces.
            framework: The ml framework used to train the model.
                Can be either one of ["tf", "tf2", "torch"].
                tf: TensorFlow (static-graph); tf2: TensorFlow 2.x
                (eager or traced, if eager_tracing=True); torch: PyTorch.
                This should be the same framework as used in the Trainer.
            embed_dim: The embedding dimension of the distillation networks
                used to compute the novelty of a state. This is the output
                size of the distillation networks. A larger embedding size
                will generalize less and therefore states have to be very
                similar for the intrinsic reward o shrink to zero. Note
                that large embedding sizes will necessarily result in slower
                training times for the agent as the distillation network is
                trained for one iteration after each single rollout.
            distill_net_config: An optional model configuration for the
                distillation networks. If None, the configuration of the
                Policy model is used.
            lr: The learning rate of the distillation network optimizer. The
                optimizer used is `Adam`. Note the network usually approaches
                its target easily. Too high learning rates will result in
                the intrinsic rewards vanishing faster and exploring similar
                states results in smaller rewards. Learning rates too small
                cause the opposite effect: intrinsic rewards are getting still
                paid for highly similar states even though they are not that
                novel anymore.
            alpha: The scaling factor of the state's novelty. Smaller values
                increase the intrinsic rewards every time new states are visited.
                An ablation study has shown an optimal value for alpha is 0.5.
            beta: The clipping factor of NovelD. Intrinsic rewards will be not
                smaller than beta. A value too large will result in all intrinsic
                rewards being the same until a specific state has been visited.
                The ablation study in the paper shows that a value of 0 is
                preferable.
            intrinsic_reward_coeff: Scaling factor of the next state's intrinsic
                reward. The default value appears to be a good choice. Values
                too high might be contraproductive leading to no signal from the
                original (sparse) rewards. Values too low make exploration less
                efficient as the agent has less incentive to do so.
            normalize: Indicates, if intrinsic rewards should be normalized. In
                experiments with distillation networks a normalization of intrinsic
                rewards results in more stable exploration (after a burn-in).
            random_timesteps: The number of timesteps to act fully random when the
                default sub-exploration is used (`subexploration=None`).
            subexploration: The config dict for the underlying Exploration
                to use (e.g. epsilon-greedy for DQN). If None, `EpsilonGreedy` is
                used with a `PiecewiseSchedule`, i.e. using the `random_timesteps`.
        """
        if not isinstance(action_space, (Discrete, MultiDiscrete)):
            raise ValueError(
                "Only (Multi)Discrete action spaces supported for NovelD so far!"
            )

        super().__init__(action_space, framework=framework, model=model, **kwargs)

        if self.policy_config["num_workers"] != 0:
            raise ValueError(
                "NovelD exploration currently does not support parallelism."
                " `num_workers` must be 0!"
            )

        # Try to import xxhash.
        try:
            import xxhash

            self._hash_state = self._xxhash_state
            logger.info(
                "Initializing NovelD: Found `xxhash`. Using it for hashing "
                "visited states in exploration as it offers higher performance "
                "and can provide hashing for larger observation spaces."
            )
        except ImportError as e:
            self._hash_state = self._defaulthash_state
            logger.warning(
                "Initializing NovelD: `xxhash` not found. Falling back to "
                "default hashing. If you want to install `xxhash` use "
                "`pip install xxhash`. `xxhash` shows higher performance and "
                "can provide hashing for larger observation spaces."
            )

        self.embed_dim = embed_dim
        # In case no configuration is passed in, use the Policy's model config.
        # it has the same input dimensions as needed for the distill network.
        if distill_net_config is None:
            distill_net_config = self.policy_config["model"].copy()
        self.distill_net_config = distill_net_config
        self.lr = lr
        self.alpha = alpha
        self.beta = beta
        self.intrinsic_reward_coeff = intrinsic_reward_coeff
        self.normalize = normalize
        self._moving_mean_std = None

        self._state_counts = {}
        self._state_counts_total = 0.0
        self._state_counts_avg = 0.0
        if self.normalize:
            # Use the `_Moving_MeanStd` class to normalize the intrinsic rewards.
            from ray.rllib.utils.exploration.random_encoder import _MovingMeanStd

            self._moving_mean_std = _MovingMeanStd()

        self.action_dim = (
            self.action_space.n
            if isinstance(self.action_space, Discrete)
            else np.sum(self.action_space.nvec)
        )

        if sub_exploration is None:
            sub_exploration = {
                "type": "EpsilonGreedy",
                "epsilon_schedule": {
                    "type": "PiecewiseSchedule",
                    # Step function.
                    "endpoints": [
                        (0, 1.0),
                        (random_timesteps + 1, 1.0),
                        (random_timesteps + 2, 0.01),
                    ],
                    "outside_value": 0.01,
                },
            }
        self.sub_exploration = sub_exploration

        # Creates modules/layers inside the actual ModelV2 object.
        self._distill_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="_noveld_distill_net",
        )
        self._distill_target_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="_noveld_distill_target_net",
        )

        # This is only used to select the correct action.
        self.exploration_submodule = from_config(
            cls=Exploration,
            config=self.sub_exploration,
            action_space=self.action_space,
            framework=self.framework,
            policy_config=self.policy_config,
            model=self.model,
            num_workers=self.num_workers,
            worker_index=self.worker_index,
        )

    @override(Exploration)
    def get_exploration_action(
        self,
        *,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        # Simply delegate to sub-Exploration module.
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore,
        )

    @override(Exploration)
    def get_exploration_optimizer(self, optimizers):
        """Prepares the optimizer for the distillation network."""

        if self.framework == "torch":
            # We do not train the target network.
            distill_params = list(self._distill_net.parameters())
            self.model._noveld_distill_net = self._distill_net.to(self.device)
            self._optimizer = torch.optim.Adam(
                distill_params,
                lr=self.lr,
            )
        else:
            self.model._noveld_distill_net = self._distill_net

            # We do not train the target network.
            self._optimizer_var_list = self._distill_net.base_model.variables
            self._optimizer = tf1.train.AdamOptimizer(learning_rate=self.lr)

            # Create placeholders and initialize the loss.
            if self.framework == "tf":
                self._obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_noveld_obs"
                )
                self._next_obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_noveld_next_obs"
                )
                # TODO: @simonsays1980: Check how it works with actions.
                (
                    self._novelty,
                    self._update_op,
                ) = self._postprocess_helper_tf(self._obs_ph)

        return optimizers

    @override(Exploration)
    def on_episode_start(
        self,
        policy: "Policy",
        *,
        environment: BaseEnv = None,
        episode: int = None,
        tf_sess: Optional[tf1.Session] = None,
    ):
        """Resets the ERIR.

        Episodic Restriction on Intrinsic Reward (ERIR) is
        used to increase the incentive for the agent to not bounce
        forth and back between discovered and undiscovered states.
        """
        # Reset the state counts.
        self._state_counts = {}
        # Also reset the metrics.
        self._state_counts_total = 0.0
        self._state_counts_avg = 0.0

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Calculates phi values for the novelty and intrinsic reward.

        Also calculates distillation loss and updates the noveld
        module on the provided batch using the optimizer.
        """
        if self.framework != "torch":
            self._postprocess_tf(policy, sample_batch, tf_sess)
        else:
            self._postprocess_torch(policy, sample_batch)

    @override(Exploration)
    def get_state(self, sess: Optional["tf.Session"] = None):
        """Returns the main variables of NovelD.

        This can be used for metrics. See the `NovelDMetricsCallbacks`.
        """
        return (
            self._intrinsic_reward_np,
            self._novelty_np,
            self._novelty_next_np,
            self._state_counts_total,
            self._state_counts_avg,
        )

    def _postprocess_tf(self, policy, sample_batch, tf_sess):
        """Calculates the intrinsic reward and updates the parameters."""
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            self._novelty_np, _ = tf_sess.run(
                [self._novelty, self._update_op],
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.OBS],
                },
            )
            # The update operation is not run here to not train the network on
            # the same observations twice.
            self._novelty_next_np = tf_sess.run(
                self._novelty,
                feed_dict={self._obs_ph: sample_batch[SampleBatch.NEXT_OBS]},
            )
        # tf-eager: Perform model calls, loss calculation, and optimizer
        # stepping on the fly.
        else:
            self._novelty_np, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
            )
            self._novelty_next_np, _ = tf.stop_gradient(
                self._postprocess_helper_tf(sample_batch[SampleBatch.NEXT_OBS])
            )

        # TODO: @simonsays1980: Add the ERIR option.
        self._update_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._compute_intrinsic_reward(sample_batch)
        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS]
            + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )

        return sample_batch

    def _postprocess_helper_tf(
        self,
        obs,
    ):
        with (
            tf.GradientTape() if self.framework == "tf" else NullContextManager()
        ) as tape:
            # Push observations through the distillation networks.
            phi, _ = self.model._noveld_distill_net({SampleBatch.OBS: obs})
            # TODO: It needs the target of the actual obs.
            phi_target, _ = self._distill_target_net({SampleBatch.OBS: obs})
            # Avoid dividing by zero in the gradient by adding a small epsilon.
            novelty = tf.norm(phi - phi_target + 1e-12, axis=1)
            # TODO: @simonsays1980: Check, if using the NEXT_OBS here in the loss is better.
            # TODO: @simonsays1980: Should be probably mean over dim=1 and then sum over batches.
            distill_loss = tf.reduce_mean(novelty, axis=-1)

            # Step the optimizer
            if self.framework != "tf":
                grads = tape.gradient(distill_loss, self._optimizer_var_list)
                grads_and_vars = [
                    (g, v)
                    for g, v in zip(grads, self._optimizer_var_list)
                    if g is not None
                ]
                update_op = self._optimizer.apply_gradients(grads_and_vars)
            else:
                update_op = self._optimizer.minimize(
                    distill_loss, var_list=self._optimizer_var_list
                )

        return novelty, update_op

    def _postprocess_torch(self, policy, sample_batch):
        """Calculates the intrinsic reward and updates the parameters."""
        # Push observations through the distillation networks.
        phis, _ = self.model._noveld_distill_net(
            {
                SampleBatch.OBS: torch.cat(
                    [
                        torch.from_numpy(sample_batch[SampleBatch.OBS]),
                        torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS]),
                    ]
                )
            }
        )
        phi_targets, _ = self._distill_target_net(
            {
                SampleBatch.OBS: torch.cat(
                    [
                        torch.from_numpy(sample_batch[SampleBatch.OBS]),
                        torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS]),
                    ]
                )
            }
        )
        phi, phi_next = torch.chunk(phis, 2)
        phi_target, phi_target_next = torch.chunk(phi_targets, 2)
        # Avoid dividing by zero in the gradient by adding a small epsilon.
        novelty = torch.norm(phi - phi_target + 1e-12, dim=1)
        self._novelty_np = novelty.detach().cpu().numpy()
        # Avoid dividing by zero in the gradient by adding a small epsilon.
        novelty_next = torch.norm(phi_next - phi_target_next + 1e-12, dim=1)
        self._novelty_next_np = novelty_next.detach().cpu().numpy()

        # Calculate the intrinsic reward.
        self._update_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._compute_intrinsic_reward(sample_batch)

        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS]
            + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )

        # Perform an optimizer step.
        distill_loss = torch.mean(novelty)
        self._optimizer.zero_grad()
        distill_loss.backward()
        self._optimizer.step()

        return sample_batch

    def _compute_intrinsic_reward(self, sample_batch):
        """Computes the intrinsic reward."""
        state_counts = self._get_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._intrinsic_reward_np = np.maximum(
            self._novelty_next_np - self.alpha * self._novelty_np, self.beta
        ) * (state_counts == 1)

    def _defaulthash_state(
        self,
        obs,
    ):
        """Creates a unique hash code for states with the same values, if
        xxhash is not installed.

        Similar to `_xxhash_state()' this is used to count states for the
        intrinsic rewards.
        """
        data = bytes() + b"," + obs.tobytes()
        return hash(data)

    def _xxhash_state(
        self,
        obs,
    ):
        """Creates a unique hash code for states with the same values.

        This is used to count states for the intrinsic rewards.
        """
        import xxhash

        data = bytes() + b"," + obs.tobytes()
        return xxhash.xxh3_64_hexdigest(data)

    def _update_state_counts(
        self,
        obs,
    ):
        """Increases the state counts.

        Also updates the running total count and mean.
        """
        states_hashes = [self._hash_state(single_obs) for single_obs in obs]
        for hash in states_hashes:
            self._state_counts[hash] = self._state_counts.get(hash, 0) + 1
        self._state_counts_avg = len(self._state_counts) * self._state_counts_avg + len(
            states_hashes
        )
        self._state_counts_total += len(states_hashes)
        self._state_counts_avg /= self._state_counts_total

    def _get_state_counts(
        self,
        obs,
    ):
        """Returns the state counts.

        This is used in calculating the intrinsic reward.
        """
        states_hashes = [self._hash_state(single_obs) for single_obs in obs]
        return np.array([self._state_counts[hash] for hash in states_hashes])


class NovelDMetricsCallbacks(DefaultCallbacks):
    """Collects metrics for NovelD exploration.

    The metrics should help users monitor the exploration of
    the environment. The metrics tracked are:

    intrinsic_reward: The intrinsic reward given by NovelD for
        exploring new states. These are averaged over the
        timesteps in the episode. A high metric indicates that
        a lot of new states are explored over the course of an
        episode.
    novelty: The novelty in NovelD is the distillation error.
        This error decreases over the run of an experiment for
        already explored states. A low metric indicates that
        the agent visits states where it has already been or
        states that are very similar to states he visited before.
        If the state is truly novel this metric increases.
    novelty_next: This is the equivalent metric for the next
        state visited (see `novelty`). Together with `novelty`
        this metric helps the user to understand the values for
        the `intrinsic_reward`.
    state_counts_total: The number of states explored over the
        course of the experiment. If this metric stagnates it is
        a sign of little exploration.
    state_counts_avg: The average number of state visits. This
        metric averages the visits to single states. If this metric
        rises it is a sign of either little exploration or of
        states that have to be crossed by the agent to go further.
        Together with `state_counts_total` this metric helps user
        to get a glimpse at state exploration. A low
        `state_counts_total` with high `state_counts_avg` is a
        strong sign of little exploration, whereas a high
        `state_counts_total` together with a low `state_counts_avg`
        is a good indicator of much exploration.
    """

    def __init__(self):
        super().__init__()

    def on_episode_start(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length == 0, (
            "ERROR: `on_episode_start()` callback should be called right "
            "after `env.reset()`."
        )

        episode.user_data["intrinsic_reward"] = []
        episode.user_data["novelty"] = []
        episode.user_data["novelty_next"] = []
        episode.user_data["state_counts_total"] = []
        episode.user_data["state_counts_avg"] = []

    def on_episode_step(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        assert episode.length > 0, (
            "ERROR: `on_episode_step()` callback should not be called right "
            "after `env.reset()`."
        )

        # Get the actual state values of the NovelD exploration.
        (
            intrinsic_reward,
            novelty,
            novelty_next,
            state_counts_total,
            state_counts_avg,
        ) = policies["default_policy"].get_exploration_state()

        # Average over batch.
        episode.user_data["intrinsic_reward"].append(np.mean(intrinsic_reward))
        episode.user_data["novelty"].append(np.mean(novelty))
        episode.user_data["novelty_next"].append(np.mean(novelty_next))
        episode.user_data["state_counts_total"].append(np.mean(state_counts_total))
        episode.user_data["state_counts_avg"].append(np.mean(state_counts_avg))

    def on_episode_end(
        self,
        *,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs,
    ):
        # Average over episode.
        episode.custom_metrics["noveld/intrinsic_reward"] = np.mean(
            episode.user_data["intrinsic_reward"]
        )
        episode.custom_metrics["noveld/novelty"] = np.mean(episode.user_data["novelty"])
        episode.custom_metrics["noveld/novelty_next"] = np.mean(
            episode.user_data["novelty_next"]
        )
        episode.custom_metrics["noveld/state_counts_total"] = np.mean(
            episode.user_data["state_counts_total"]
        )
        episode.custom_metrics["noveld/state_counts_avg"] = np.mean(
            episode.user_data["state_counts_avg"]
        )

        # Show also histograms of episodic intrinsic rewards.
        episode.hist_data["noveld/intrinsic_reward"] = episode.user_data[
            "intrinsic_reward"
        ]
        episode.hist_data["noveld/novelty"] = episode.user_data["novelty"]
        episode.hist_data["noveld/novelty_next"] = episode.user_data["novelty_next"]
        episode.hist_data["noveld/state_counts_total"] = episode.user_data[
            "state_counts_total"
        ]
        episode.hist_data["noveld/state_counts_avg"] = episode.user_data[
            "state_counts_avg"
        ]
