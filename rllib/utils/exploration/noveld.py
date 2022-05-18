from gym.spaces import Discrete, MultiDiscrete, Space
import logging
import numpy as np
from typing import Optional, Union

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
            logger.info("Initializing NovelD: Found `xxhash`. Using it for hashing "
                        "visited states in exploration as it offers higher performance " 
                        "and can provide hashing for larger observation spaces.")
        except ImportError as e:     
            self._hash_state = self._defaulthash_state
            logger.warning("Initializing NovelD: `xxhash` not found. Falling back to "
                           "default hashing. If you want to install `xxhash` use "
                           "`pip install xxhash`. `xxhash` shows higher performance and "
                           "can provide hashing for larger observation spaces.")
            
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
        self._state_counts_total = 0.0
        self._state_counts_avg = 0.0
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
                # TODO: @simonsays1980: Check how it works with actions.
                (
                    self._novelty,
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
        used to increase the incentive for the agent to not bounce 
        forth and back between discovered and undiscovered states.
        """
        # Reset the state counts.
        self._state_counts = {}  
        # Also reset the metrics.
        self._state_counts_total = .0
        self._state_counts_avg = .0          
    
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
        sess: Optional["tf.Session"] = None
    ):
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
    
    def _postprocess_tf(
        self, 
        policy, 
        sample_batch, 
        tf_sess
    ):
        """Calculates the intrinsic reward and updates the parameters."""
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            self._novelty_np, _ = tf_sess.run(
                [self._novelty, self._update_op],
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.OBS],                                    
                }
            )
            # The update operation is not run here to not train the network on
            # the same observations twice.
            self._novelty_next_np = tf_sess.run(
                self._novelty,
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.NEXT_OBS]
                }
            )
        # tf-eager: Perform model calls, loss calculation, and optimizer
        # stepping on the fly.
        else:
            self._novelty_np, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
            )
            self._novelty_next_np, _ = tf.stop_gradient(
                self._postprocess_helper_tf(
                    sample_batch[SampleBatch.NEXT_OBS]
                )
            )
        
        # TODO: @simonsays1980: Add the ERIR option.
        self._update_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._compute_intrinsic_reward(sample_batch)
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
            # Avoid dividing by zero in the gradient by adding a small epsilon. 
            novelty = tf.norm(phi - phi_target + 1e-12, axis=1)            
            # TODO: @simonsays1980: Check, if using the NEXT_OBS here in the loss is better.
            # TODO: @simonsays1980: Should be probably mean over dim=1 and then sum over batches.
            distill_loss = tf.reduce_mean(novelty, axis=-1)
            
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
        
        return novelty, update_op
    
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
            sample_batch[SampleBatch.REWARDS] + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )
        
        # Perform an optimizer step.
        distill_loss = torch.mean(novelty)
        self._optimizer.zero_grad()
        distill_loss.backward()
        self._optimizer.step()

        return sample_batch
        
    def _compute_intrinsic_reward(
        self, 
        sample_batch
    ):
        """Computes the intrinsic reward."""
        state_counts = self._get_state_counts(sample_batch[SampleBatch.NEXT_OBS])
        self._intrinsic_reward_np = (
            np.maximum(self._novelty_next_np \
                - self.alpha * self._novelty_np, self.beta) \
                    * (state_counts == 1)                    
        )
        
    def _defaulthash_state(
        self,
        obs,
    ):
        """Creates a unique hash code for states with the same values, if
        xxhash is not installed. 
        
        Similar to `_xxhash_state()' this is used to count states for the 
        intrinsic rewards.
        """
        data = bytes() + b',' + obs.tobytes()        
        return hash(data)
        
    def _xxhash_state(
        self, 
        obs,
    ):
        """Creates a unique hash code for states with the same values.
        
        This is used to count states for the intrinsic rewards.
        """
        import xxhash
        data = bytes() + b',' + obs.tobytes()        
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
        self._state_counts_avg = len(self._state_counts) * self._state_counts_avg + len(states_hashes)
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
        
        
        