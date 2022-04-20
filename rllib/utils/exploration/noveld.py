from gym.spaces import Discrete, MultiDiscrete, Space, Tuple
import numpy as np
from typing import Optional, Tuple, Union
import xxhash

from ray.rllib.env.base_env import BaseEnv
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, TensorType

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
        alpha: float = .5,
        beta: float = .0,
        intrinsic_reward_coeff: float = 5e-3,
        normalize: bool = True,
        random_timesteps: int = 10000,
        sub_exploration: Optional[FromConfigSpec] = None,
        **kwargs,
    ):
        """Initializes a NovelD exploration scheme.
        
        Args:
            action_space: The action space of the environment.
            framework: The framework used to train the model.
            model: The model used to train the model.
        """
        if not isinstance(action_space, (Discrete, MultiDiscrete)):
            raise ValueError(
                "Curiosity exploration currently does not support parallelism."
                " `num_workers` must be 0!"
            )
        
        super().__init__(action_space, framework=framework, model=model, **kwargs)
        
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
        
        self._state_counts = {}
        if self.normalize:
            from ray.rllib.utils.exploration.random_encoder import MovingMeanStd
            self._moving_mean_std = MovingMeanStd()
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
                        (random_timesteps + 2, .01),                       
                    ],
                    "outside_value": .01,
                }
            }
        self.sub_exploration = sub_exploration
        
        # Creates modules/layers inside the actual ModelV2 object.
        self._distill_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="distill_net",
        )
        self._distill_target_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="distill_target_net",
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
        # Simply delegate t sub-Exploration module.
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore,
        )
    
    @override(Exploration)
    def get_exploration_optimizer(
        self, 
        optimizers
    ):
        """Prepares the optimizer for the distillation network."""
        
        if self.framework == "torch":   
            # We do not train the target network.                     
            distill_params = list(self._distill_net.parameters())
            self.model._noveld_distill_net = self._distill_net.to(
                self.device
            )
            self._optimizer = torch.optim.Adam(
                distill_params, lr=self.lr,
            )      
        else:
            self.model._noveld_distill_net = self._distill_net
            
            # We do not train the target network.
            self._optimizer_var_list = (
                self._distill_net.base_model.variables
            )
            self._optimizer = tf1.train.AdamOptimizer(learning_rate=self.lr)

            # Create placeholders and initialize the loss.
            if self.framework == "tf":
                self._obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_noveld_obs"
                )
                self._next_obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_noveld_next_obs"
                )
                # TODO: Check how it works with actions.
                (
                    self._novelty_squared,
                    self._update_op,
                ) = (
                    self._postprocess_helper_tf(
                        self._obs_ph
                    )
                )
            
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
        used to increase the incentive to not bounce 
        forth and back between discovered and undiscovered 
        states.
        """
        # Reset the state counts.
        self._state_counts = {}            
    
    @override(Exploration)
    def postprocess_trajectory(
        self,
        policy,
        sample_batch,
        tf_sess=None
    ):
        """Calculates phi values for the novelty and intrinsic reward.
        
        Also calculates distillation loss and updates the noveld
        module on the provided batch using the optimizer.
        """
        if self.framework != "torch":
            self._postprocess_tf(policy, sample_batch, tf_sess)
        else:
            self._postprocess_torch(policy, sample_batch)
        
    @override(Exploration)
    def get_state(
        self, 
        sess
    ):
        """Returns the main variables of NovelD.
        
        This can be used for metrics.
        """
        return (
            self._novelty_squared_np, 
            self._novelty_squared_next_np, 
            self._intrinsic_reward_np,
        )
    
    def _postprocess_tf(
        self, 
        policy, 
        sample_batch, 
        tf_sess
    ):
        """Calculates the intrinsic reward and updates the parameters."""
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            self._novelty_squared_np, _ = tf_sess.run(
                [self._novelty_squared, self._update_op],
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.OBS],                                    
                }
            )
            # TODO: check if right two steps of optimizing could be done.
            self._novelty_squared_next_np = tf_sess.run(
                self._novelty_squared,
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.NEXT_OBS]
                }
            )
        # tf-eager: Perform model calls, loss calculation, and optimizer
        # stepping on the fly.
        else:
            self._novelty_squared_np, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
            )
            self._novelty_squared_next_np, _ = tf.stop_gradient(
                self._postprocess_helper_tf(
                    sample_batch[SampleBatch.NEXT_OBS]
                )
            )
        
        # TODO: Add the ERIR option.
        # This could be encapsulated into an function.
        self._set_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        state_counts = self._get_state_counts(sample_batch[SampleBatch.NEXT_OBS])                
        self._intrinsic_reward_np = (
            np.maximum(np.sqrt(self._novelty_squared_next_np) \
                - self.alpha * np.sqrt(self._novelty_squared_np), self.beta) \
                    * (state_counts == 1)
        )
        
        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)
        
        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS] + self._intrinsic_reward_np * self.intrinsic_reward_coeff
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
            phi, _ = self.model._noveld_distill_net(
                {SampleBatch.OBS: obs}
            )
            # TODO: It needs the target of the actual obs.
            phi_target, _ = self._distill_target_net(
                {SampleBatch.OBS: obs}
            )
            novelty_squared = tf.reduce_sum(tf.square(phi_target - phi), axis=-1)
            # TODO: Check, if using the NEXT_OBS here in the loss is better.
            distill_loss = tf.reduce_mean(novelty_squared)
            
            # Step the optimizer
            if self.framework != "tf":
                grads = tape.gradient(distill_loss, self._optimizer_var_list)
                grads_and_vars = [
                    (g, v) for g, v in zip(grads, self._optimizer_var_list) if g is not None
                ]
                update_op = self._optimizer.apply_gradients(grads_and_vars)
            else:
                update_op = self._optimizer.minimize(
                    distill_loss, var_list=self._optimizer_var_list
                )
        
        return novelty_squared, update_op
    
    def _postprocess_torch(
        self,
        policy,
        sample_batch
    ): 
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
        novelty_squared = torch.sum(
            torch.square(phi_target - phi), dim=-1
        )
        self._novelty_squared_np = novelty_squared.detach().cpu().numpy()
        novelty_squared_next = torch.sum(
            torch.square(phi_target_next - phi_next), dim=-1
        )        
        self._novelty_squared_next_np = novelty_squared_next.detach().cpu().numpy()
        
        # Calculate the intrinsic reward.
        self._set_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        state_counts = self._get_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._intrinsic_reward_np = (
            np.maximum(np.sqrt(self._novelty_squared_next_np) \
                - self.alpha * np.sqrt(self._novelty_squared_np), self.beta) \
                    * (state_counts == 1)
        )
        
        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)
        
        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS] + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )
        
        # Perform an optimizer step.
        distill_loss = torch.mean(novelty_squared)
        self._optimizer.zero_grad()
        distill_loss.backward()
        self._optimizer.step()

        return sample_batch
        
    def _hash_state(
        self, 
        obs,
    ):
        """Creates a unique hash code for states with the same values.
        
        This is used to count states for the intrinsic rewards.
        """
        data = bytes() + b',' + obs.tobytes()
        
        return xxhash.xxh3_64_hexdigest(data).upper()
        
    def _set_state_counts(
        self,
        obs,
    ):
        """Increases the state counts."""
        states_hashes = [self._hash_state(obs) for obs in obs]
        for hash in states_hashes:
            if hash in self._state_counts:
                self._state_counts[hash] += 1
            else:
                self._state_counts[hash] = 1
        
    def _get_state_counts(
        self,
        obs,        
    ): 
        """Returns the state counts.
        
        This is used in calculating the intrinsic reward.
        """
        states_hashes = [self._hash_state(single_obs) for single_obs in obs]
        return np.array([self._state_counts[hash] for hash in states_hashes])
        
        
        