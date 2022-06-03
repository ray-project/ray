import logging
from typing import Dict, List, Type, Union

import ray
from ray.rllib.agents.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
    compute_gae_for_sample_batch,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    LocalOptimizer,
    ModelGradients,
    ValueNetworkMixin,
    compute_gradients,
)
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


def PPOLoss(
    dist_class,
    actions,
    curr_logits,
    behaviour_logits,
    advantages,
    value_fn,
    value_targets,
    vf_preds,
    cur_kl_coeff,
    entropy_coeff,
    clip_param,
    vf_clip_param,
    vf_loss_coeff,
    clip_loss=False,
):
    def surrogate_loss(
        actions, curr_dist, prev_dist, advantages, clip_param, clip_loss
    ):
        pi_new_logp = curr_dist.logp(actions)
        pi_old_logp = prev_dist.logp(actions)

        logp_ratio = tf.math.exp(pi_new_logp - pi_old_logp)
        if clip_loss:
            return tf.minimum(
                advantages * logp_ratio,
                advantages
                * tf.clip_by_value(logp_ratio, 1 - clip_param, 1 + clip_param),
            )
        return advantages * logp_ratio

    def kl_loss(curr_dist, prev_dist):
        return prev_dist.kl(curr_dist)

    def entropy_loss(dist):
        return dist.entropy()

    def vf_loss(value_fn, value_targets, vf_preds, vf_clip_param=0.1):
        # GAE Value Function Loss
        vf_loss1 = tf.math.square(value_fn - value_targets)
        vf_clipped = vf_preds + tf.clip_by_value(
            value_fn - vf_preds, -vf_clip_param, vf_clip_param
        )
        vf_loss2 = tf.math.square(vf_clipped - value_targets)
        vf_loss = tf.maximum(vf_loss1, vf_loss2)
        return vf_loss

    pi_new_dist = dist_class(curr_logits, None)
    pi_old_dist = dist_class(behaviour_logits, None)

    surr_loss = tf.reduce_mean(
        surrogate_loss(
            actions, pi_new_dist, pi_old_dist, advantages, clip_param, clip_loss
        )
    )
    kl_loss = tf.reduce_mean(kl_loss(pi_new_dist, pi_old_dist))
    vf_loss = tf.reduce_mean(vf_loss(value_fn, value_targets, vf_preds, vf_clip_param))
    entropy_loss = tf.reduce_mean(entropy_loss(pi_new_dist))

    total_loss = -surr_loss + cur_kl_coeff * kl_loss
    total_loss += vf_loss_coeff * vf_loss - entropy_coeff * entropy_loss
    return total_loss, surr_loss, kl_loss, vf_loss, entropy_loss


# This is the computation graph for workers (inner adaptation steps)
class WorkerLoss(object):
    def __init__(
        self,
        dist_class,
        actions,
        curr_logits,
        behaviour_logits,
        advantages,
        value_fn,
        value_targets,
        vf_preds,
        cur_kl_coeff,
        entropy_coeff,
        clip_param,
        vf_clip_param,
        vf_loss_coeff,
        clip_loss=False,
    ):
        self.loss, surr_loss, kl_loss, vf_loss, ent_loss = PPOLoss(
            dist_class=dist_class,
            actions=actions,
            curr_logits=curr_logits,
            behaviour_logits=behaviour_logits,
            advantages=advantages,
            value_fn=value_fn,
            value_targets=value_targets,
            vf_preds=vf_preds,
            cur_kl_coeff=cur_kl_coeff,
            entropy_coeff=entropy_coeff,
            clip_param=clip_param,
            vf_clip_param=vf_clip_param,
            vf_loss_coeff=vf_loss_coeff,
            clip_loss=clip_loss,
        )
        self.loss = tf1.Print(self.loss, ["Worker Adapt Loss", self.loss])


# This is the Meta-Update computation graph for main (meta-update step)
class MAMLLoss(object):
    def __init__(
        self,
        model,
        config,
        dist_class,
        value_targets,
        advantages,
        actions,
        behaviour_logits,
        vf_preds,
        cur_kl_coeff,
        policy_vars,
        obs,
        num_tasks,
        split,
        inner_adaptation_steps=1,
        entropy_coeff=0,
        clip_param=0.3,
        vf_clip_param=0.1,
        vf_loss_coeff=1.0,
        use_gae=True,
    ):

        self.config = config
        self.num_tasks = num_tasks
        self.inner_adaptation_steps = inner_adaptation_steps
        self.clip_param = clip_param
        self.dist_class = dist_class
        self.cur_kl_coeff = cur_kl_coeff

        # Split episode tensors into [inner_adaptation_steps+1, num_tasks, -1]
        self.obs = self.split_placeholders(obs, split)
        self.actions = self.split_placeholders(actions, split)
        self.behaviour_logits = self.split_placeholders(behaviour_logits, split)
        self.advantages = self.split_placeholders(advantages, split)
        self.value_targets = self.split_placeholders(value_targets, split)
        self.vf_preds = self.split_placeholders(vf_preds, split)

        #  Construct name to tensor dictionary for easier indexing
        self.policy_vars = {}
        for var in policy_vars:
            self.policy_vars[var.name] = var

        # Calculate pi_new for PPO
        pi_new_logits, current_policy_vars, value_fns = [], [], []
        for i in range(self.num_tasks):
            pi_new, value_fn = self.feed_forward(
                self.obs[0][i], self.policy_vars, policy_config=config["model"]
            )
            pi_new_logits.append(pi_new)
            value_fns.append(value_fn)
            current_policy_vars.append(self.policy_vars)

        inner_kls = []
        inner_ppo_loss = []

        # Recompute weights for inner-adaptation (same weights as workers)
        for step in range(self.inner_adaptation_steps):
            kls = []
            for i in range(self.num_tasks):
                # PPO Loss Function (only Surrogate)
                ppo_loss, _, kl_loss, _, _ = PPOLoss(
                    dist_class=dist_class,
                    actions=self.actions[step][i],
                    curr_logits=pi_new_logits[i],
                    behaviour_logits=self.behaviour_logits[step][i],
                    advantages=self.advantages[step][i],
                    value_fn=value_fns[i],
                    value_targets=self.value_targets[step][i],
                    vf_preds=self.vf_preds[step][i],
                    cur_kl_coeff=0.0,
                    entropy_coeff=entropy_coeff,
                    clip_param=clip_param,
                    vf_clip_param=vf_clip_param,
                    vf_loss_coeff=vf_loss_coeff,
                    clip_loss=False,
                )
                adapted_policy_vars = self.compute_updated_variables(
                    ppo_loss, current_policy_vars[i]
                )
                pi_new_logits[i], value_fns[i] = self.feed_forward(
                    self.obs[step + 1][i],
                    adapted_policy_vars,
                    policy_config=config["model"],
                )
                current_policy_vars[i] = adapted_policy_vars
                kls.append(kl_loss)
                inner_ppo_loss.append(ppo_loss)

            self.kls = kls
            inner_kls.append(kls)

        mean_inner_kl = tf.stack(
            [tf.reduce_mean(tf.stack(inner_kl)) for inner_kl in inner_kls]
        )
        self.mean_inner_kl = mean_inner_kl

        ppo_obj = []
        for i in range(self.num_tasks):
            ppo_loss, surr_loss, kl_loss, val_loss, entropy_loss = PPOLoss(
                dist_class=dist_class,
                actions=self.actions[self.inner_adaptation_steps][i],
                curr_logits=pi_new_logits[i],
                behaviour_logits=self.behaviour_logits[self.inner_adaptation_steps][i],
                advantages=self.advantages[self.inner_adaptation_steps][i],
                value_fn=value_fns[i],
                value_targets=self.value_targets[self.inner_adaptation_steps][i],
                vf_preds=self.vf_preds[self.inner_adaptation_steps][i],
                cur_kl_coeff=0.0,
                entropy_coeff=entropy_coeff,
                clip_param=clip_param,
                vf_clip_param=vf_clip_param,
                vf_loss_coeff=vf_loss_coeff,
                clip_loss=True,
            )
            ppo_obj.append(ppo_loss)
        self.mean_policy_loss = surr_loss
        self.mean_kl = kl_loss
        self.mean_vf_loss = val_loss
        self.mean_entropy = entropy_loss
        self.inner_kl_loss = tf.reduce_mean(
            tf.multiply(self.cur_kl_coeff, mean_inner_kl)
        )
        self.loss = tf.reduce_mean(tf.stack(ppo_obj, axis=0)) + self.inner_kl_loss
        self.loss = tf1.Print(
            self.loss, ["Meta-Loss", self.loss, "Inner KL", self.mean_inner_kl]
        )

    def feed_forward(self, obs, policy_vars, policy_config):
        # Hacky for now, reconstruct FC network with adapted weights
        # @mluo: TODO for any network
        def fc_network(
            inp, network_vars, hidden_nonlinearity, output_nonlinearity, policy_config
        ):
            bias_added = False
            x = inp
            for name, param in network_vars.items():
                if "kernel" in name:
                    x = tf.matmul(x, param)
                elif "bias" in name:
                    x = tf.add(x, param)
                    bias_added = True
                else:
                    raise NameError

                if bias_added:
                    if "out" not in name:
                        x = hidden_nonlinearity(x)
                    elif "out" in name:
                        x = output_nonlinearity(x)
                    else:
                        raise NameError
                    bias_added = False
            return x

        policyn_vars = {}
        valuen_vars = {}
        log_std = None
        for name, param in policy_vars.items():
            if "value" in name:
                valuen_vars[name] = param
            elif "log_std" in name:
                log_std = param
            else:
                policyn_vars[name] = param

        output_nonlinearity = tf.identity
        hidden_nonlinearity = get_activation_fn(policy_config["fcnet_activation"])

        pi_new_logits = fc_network(
            obs, policyn_vars, hidden_nonlinearity, output_nonlinearity, policy_config
        )
        if log_std is not None:
            pi_new_logits = tf.concat([pi_new_logits, 0.0 * pi_new_logits + log_std], 1)
        value_fn = fc_network(
            obs, valuen_vars, hidden_nonlinearity, output_nonlinearity, policy_config
        )

        return pi_new_logits, tf.reshape(value_fn, [-1])

    def compute_updated_variables(self, loss, network_vars):
        grad = tf.gradients(loss, list(network_vars.values()))
        adapted_vars = {}
        for i, tup in enumerate(network_vars.items()):
            name, var = tup
            if grad[i] is None:
                adapted_vars[name] = var
            else:
                adapted_vars[name] = var - self.config["inner_lr"] * grad[i]
        return adapted_vars

    def split_placeholders(self, placeholder, split):
        inner_placeholder_list = tf.split(
            placeholder, tf.math.reduce_sum(split, axis=1), axis=0
        )
        placeholder_list = []
        for index, split_placeholder in enumerate(inner_placeholder_list):
            placeholder_list.append(tf.split(split_placeholder, split[index], axis=0))
        return placeholder_list


class KLCoeffMixin:
    def __init__(self, config):
        self.kl_coeff_val = [config["kl_coeff"]] * config["inner_adaptation_steps"]
        self.kl_target = self.config["kl_target"]
        self.kl_coeff = tf1.get_variable(
            initializer=tf.keras.initializers.Constant(self.kl_coeff_val),
            name="kl_coeff",
            shape=(config["inner_adaptation_steps"]),
            trainable=False,
            dtype=tf.float32,
        )

    def update_kls(self, sampled_kls):
        for i, kl in enumerate(sampled_kls):
            if kl < self.kl_target / 1.5:
                self.kl_coeff_val[i] *= 0.5
            elif kl > 1.5 * self.kl_target:
                self.kl_coeff_val[i] *= 2.0
        print(self.kl_coeff_val)
        self.kl_coeff.load(self.kl_coeff_val, session=self.get_session())
        return self.kl_coeff_val


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_maml_tf_policy(base: type) -> type:
    """Construct a MAMLTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAMLTrainer.
    """

    class MAMLTFPolicy(KLCoeffMixin, ValueNetworkMixin, base):
        def __init__(
            self,
            obs_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            config = dict(ray.rllib.algorithms.maml.maml.DEFAULT_CONFIG, **config)
            validate_config(config)

            # Initialize base class.
            base.__init__(
                self,
                obs_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            KLCoeffMixin.__init__(self, config)
            ValueNetworkMixin.__init__(self, config)

            # Create the `split` placeholder before initialize loss.
            if self.framework == "tf":
                self._loss_input_dict["split"] = tf1.placeholder(
                    tf.int32,
                    name="Meta-Update-Splitting",
                    shape=(
                        self.config["inner_adaptation_steps"] + 1,
                        self.config["num_workers"],
                    ),
                )

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            logits, state = model(train_batch)
            self.cur_lr = self.config["lr"]

            if self.config["worker_index"]:
                self.loss_obj = WorkerLoss(
                    dist_class=dist_class,
                    actions=train_batch[SampleBatch.ACTIONS],
                    curr_logits=logits,
                    behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
                    advantages=train_batch[Postprocessing.ADVANTAGES],
                    value_fn=model.value_function(),
                    value_targets=train_batch[Postprocessing.VALUE_TARGETS],
                    vf_preds=train_batch[SampleBatch.VF_PREDS],
                    cur_kl_coeff=0.0,
                    entropy_coeff=self.config["entropy_coeff"],
                    clip_param=self.config["clip_param"],
                    vf_clip_param=self.config["vf_clip_param"],
                    vf_loss_coeff=self.config["vf_loss_coeff"],
                    clip_loss=False,
                )
            else:
                self.var_list = tf1.get_collection(
                    tf1.GraphKeys.TRAINABLE_VARIABLES, tf1.get_variable_scope().name
                )
                self.loss_obj = MAMLLoss(
                    model=model,
                    dist_class=dist_class,
                    value_targets=train_batch[Postprocessing.VALUE_TARGETS],
                    advantages=train_batch[Postprocessing.ADVANTAGES],
                    actions=train_batch[SampleBatch.ACTIONS],
                    behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
                    vf_preds=train_batch[SampleBatch.VF_PREDS],
                    cur_kl_coeff=self.kl_coeff,
                    policy_vars=self.var_list,
                    obs=train_batch[SampleBatch.CUR_OBS],
                    num_tasks=self.config["num_workers"],
                    split=train_batch["split"],
                    config=self.config,
                    inner_adaptation_steps=self.config["inner_adaptation_steps"],
                    entropy_coeff=self.config["entropy_coeff"],
                    clip_param=self.config["clip_param"],
                    vf_clip_param=self.config["vf_clip_param"],
                    vf_loss_coeff=self.config["vf_loss_coeff"],
                    use_gae=self.config["use_gae"],
                )

            return self.loss_obj.loss

        @override(base)
        def optimizer(
            self,
        ) -> Union[
            "tf.keras.optimizers.Optimizer", List["tf.keras.optimizers.Optimizer"]
        ]:
            """
            Workers use simple SGD for inner adaptation
            Meta-Policy uses Adam optimizer for meta-update
            """
            if not self.config["worker_index"]:
                return tf1.train.AdamOptimizer(learning_rate=self.config["lr"])
            return tf1.train.GradientDescentOptimizer(
                learning_rate=self.config["inner_lr"]
            )

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            if self.config["worker_index"]:
                return {"worker_loss": self.loss_obj.loss}
            else:
                return {
                    "cur_kl_coeff": tf.cast(self.kl_coeff, tf.float64),
                    "cur_lr": tf.cast(self.cur_lr, tf.float64),
                    "total_loss": self.loss_obj.loss,
                    "policy_loss": self.loss_obj.mean_policy_loss,
                    "vf_loss": self.loss_obj.mean_vf_loss,
                    "kl": self.loss_obj.mean_kl,
                    "inner_kl": self.loss_obj.mean_inner_kl,
                    "entropy": self.loss_obj.mean_entropy,
                }

        @override(base)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            sample_batch = super().postprocess_trajectory(sample_batch)
            return compute_gae_for_sample_batch(
                self, sample_batch, other_agent_batches, episode
            )

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            return compute_gradients(self, optimizer, loss)

    return MAMLTFPolicy


MAMLStaticGraphTFPolicy = get_maml_tf_policy(DynamicTFPolicyV2)
MAMLEagerTFPolicy = get_maml_tf_policy(EagerTFPolicyV2)
