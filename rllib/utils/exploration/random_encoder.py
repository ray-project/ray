from gym.spaces import Box, Discrete, Space
import numpy as np
from typing import List, Optional, Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


class MovingMeanStd:
    """Track moving mean, std and count."""

    def __init__(self, epsilon: float = 1e-4, shape: Optional[List[int]] = None):
        """Initialize object.

        Args:
            epsilon: Initial count.
            shape: Shape of the trackables mean and std.
        """
        if not shape:
            shape = []
        self.mean = np.zeros(shape, dtype=np.float32)
        self.var = np.ones(shape, dtype=np.float32)
        self.count = epsilon

    def __call__(self, inputs: np.ndarray) -> np.ndarray:
        """Normalize input batch using moving mean and std.

        Args:
            inputs: Input batch to normalize.

        Returns:
            Logarithmic scaled normalized output.
        """
        batch_mean = np.mean(inputs, axis=0)
        batch_var = np.var(inputs, axis=0)
        batch_count = inputs.shape[0]
        self.update_params(batch_mean, batch_var, batch_count)
        return np.log(inputs / self.std + 1)

    def update_params(
        self, batch_mean: float, batch_var: float, batch_count: float
    ) -> None:
        """Update moving mean, std and count.

        Args:
            batch_mean: Input batch mean.
            batch_var: Input batch variance.
            batch_count: Number of cases in the batch.
        """
        delta = batch_mean - self.mean
        tot_count = self.count + batch_count

        # This moving mean calculation is from reference implementation.
        self.mean = self.mean + delta + batch_count / tot_count
        m_a = self.var * self.count
        m_b = batch_var * batch_count
        M2 = m_a + m_b + np.power(delta, 2) * self.count * batch_count / tot_count
        self.var = M2 / tot_count
        self.count = tot_count

    @property
    def std(self) -> float:
        """Get moving standard deviation.

        Returns:
            Returns moving standard deviation.
        """
        return np.sqrt(self.var)


def update_beta(beta_schedule: str, beta: float, rho: float, step: int) -> float:
    """Update beta based on schedule and training step.

    Args:
        beta_schedule: Schedule for beta update.
        beta: Initial beta.
        rho: Schedule decay parameter.
        step: Current training iteration.

    Returns:
        Updated beta as per input schedule.
    """
    if beta_schedule == "linear_decay":
        return beta * ((1.0 - rho) ** step)
    return beta


def compute_states_entropy(
    obs_embeds: np.ndarray, embed_dim: int, k_nn: int
) -> np.ndarray:
    """Compute states entropy using K nearest neighbour method.

    Args:
        obs_embeds: Observation latent representation using
            encoder model.
        embed_dim: Embedding vector dimension.
        k_nn: Number of nearest neighbour for K-NN estimation.

    Returns:
        Computed states entropy.
    """
    obs_embeds_ = np.reshape(obs_embeds, [-1, embed_dim])
    dist = np.linalg.norm(obs_embeds_[:, None, :] - obs_embeds_[None, :, :], axis=-1)
    return dist.argsort(axis=-1)[:, :k_nn][:, -1]


class RE3(Exploration):
    """Random Encoder for Efficient Exploration.

    Implementation of:
    [1] State entropy maximization with random encoders for efficient
    exploration. Seo, Chen, Shin, Lee, Abbeel, & Lee, (2021).
    arXiv preprint arXiv:2102.09430.

    Estimates state entropy using a particle-based k-nearest neighbors (k-NN)
    estimator in the latent space. The state's latent representation is
    calculated using an encoder with randomly initialized parameters.

    The entropy of a state is considered as intrinsic reward and added to the
    environment's extrinsic reward for policy optimization.
    Entropy is calculated per batch, it does not take the distribution of
    the entire replay buffer into consideration.
    """

    def __init__(
        self,
        action_space: Space,
        *,
        framework: str,
        model: ModelV2,
        embeds_dim: int = 128,
        encoder_net_config: Optional[ModelConfigDict] = None,
        beta: float = 0.2,
        beta_schedule: str = "constant",
        rho: float = 0.1,
        k_nn: int = 50,
        random_timesteps: int = 10000,
        sub_exploration: Optional[FromConfigSpec] = None,
        **kwargs
    ):
        """Initialize RE3.

        Args:
            action_space: The action space in which to explore.
            framework: Supports "tf", this implementation does not
                support torch.
            model: The policy's model.
            embeds_dim: The dimensionality of the observation embedding
                vectors in latent space.
            encoder_net_config: Optional model
                configuration for the encoder network, producing embedding
                vectors from observations. This can be used to configure
                fcnet- or conv_net setups to properly process any
                observation space.
            beta: Hyperparameter to choose between exploration and
                exploitation.
            beta_schedule: Schedule to use for beta decay, one of
                "constant" or "linear_decay".
            rho: Beta decay factor, used for on-policy algorithm.
            k_nn: Number of neighbours to set for K-NN entropy
                estimation.
            random_timesteps: The number of timesteps to act completely
                randomly (see [1]).
            sub_exploration: The config dict for the underlying Exploration
                to use (e.g. epsilon-greedy for DQN). If None, uses the
                FromSpecDict provided in the Policy's default config.

        Raises:
            ValueError: If the input framework is Torch.
        """
        # TODO(gjoliver): Add supports for Pytorch.
        if framework == "torch":
            raise ValueError("This RE3 implementation does not support Torch.")
        super().__init__(action_space, model=model, framework=framework, **kwargs)

        self.beta = beta
        self.rho = rho
        self.k_nn = k_nn
        self.embeds_dim = embeds_dim
        if encoder_net_config is None:
            encoder_net_config = self.policy_config["model"].copy()
        self.encoder_net_config = encoder_net_config

        # Auto-detection of underlying exploration functionality.
        if sub_exploration is None:
            # For discrete action spaces, use an underlying EpsilonGreedy with
            # a special schedule.
            if isinstance(self.action_space, Discrete):
                sub_exploration = {
                    "type": "EpsilonGreedy",
                    "epsilon_schedule": {
                        "type": "PiecewiseSchedule",
                        # Step function (see [2]).
                        "endpoints": [
                            (0, 1.0),
                            (random_timesteps + 1, 1.0),
                            (random_timesteps + 2, 0.01),
                        ],
                        "outside_value": 0.01,
                    },
                }
            elif isinstance(self.action_space, Box):
                sub_exploration = {
                    "type": "OrnsteinUhlenbeckNoise",
                    "random_timesteps": random_timesteps,
                }
            else:
                raise NotImplementedError

        self.sub_exploration = sub_exploration

        # Creates ModelV2 embedding module / layers.
        self._encoder_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embeds_dim,
            model_config=self.encoder_net_config,
            framework=self.framework,
            name="encoder_net",
        )
        if self.framework == "tf":
            self._obs_ph = get_placeholder(
                space=self.model.obs_space, name="_encoder_obs"
            )
            self._obs_embeds = tf.stop_gradient(
                self._encoder_net({SampleBatch.OBS: self._obs_ph})[0]
            )

        # This is only used to select the correct action
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
        explore: bool = True
    ):
        # Simply delegate to sub-Exploration module.
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution, timestep=timestep, explore=explore
        )

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Calculate states' latent representations/embeddings.

        Embeddings are added to the SampleBatch object such that it doesn't
        need to be calculated during each training step.
        """
        if self.framework != "torch":
            sample_batch = self._postprocess_tf(policy, sample_batch, tf_sess)
        else:
            raise ValueError("Not implemented for Torch.")
        return sample_batch

    def _postprocess_tf(self, policy, sample_batch, tf_sess):
        """Calculate states' embeddings and add it to SampleBatch."""
        if self.framework == "tf":
            obs_embeds = tf_sess.run(
                self._obs_embeds,
                feed_dict={self._obs_ph: sample_batch[SampleBatch.OBS]},
            )
        else:
            obs_embeds = tf.stop_gradient(
                self._encoder_net({SampleBatch.OBS: sample_batch[SampleBatch.OBS]})[0]
            )
        sample_batch[SampleBatch.OBS_EMBEDS] = obs_embeds
        return sample_batch
