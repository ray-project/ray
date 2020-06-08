import logging

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import LearningRateSchedule, \
    EntropyCoeffSchedule
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.tf_ops import make_tf_callable
from ray.rllib.utils import try_import_tf
from ray.rllib.agents.ppo.ppo_tf_policy import PPOLoss, postprocess_ppo_gae, vf_preds_fetches, clip_gradients, setup_config


tf = try_import_tf()

logger = logging.getLogger(__name__)

def PPOLoss(self,
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
         clip_loss=False):
    
        def reduce_mean_valid(t):
            return tf.reduce_mean(t)

        def surrogate_loss(actions, curr_dist, prev_dist, advantages, clip_param, clip_loss):
            pi_new_logp = curr_dist.logp(actions)
            pi_old_logp = prev_dist.logp(actions)

            logp_ratio = tf.exp(pi_new_logp - pi_old_logp)
            if clip_loss:
                return tf.minimum(
                advantages * logp_ratio,
                advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                              1 + clip_param))
            return advantages*logp_ratio

        def kl_loss(curr_dist, prev_dist):
            return prev_dist.kl(curr_dist)

        def entropy_loss(dist):
            return dist.entropy()

        def vf_loss(value_fn, value_targets, vf_preds, vf_clip_param=0.1):
            # GAE Value Function Loss
            vf_loss1 = tf.square(value_fn - value_targets)
            vf_clipped = vf_preds + tf.clip_by_value(
                value_fn - vf_preds, -vf_clip_param, vf_clip_param)
            vf_loss2 = tf.square(vf_clipped - value_targets)
            vf_loss = tf.maximum(vf_loss1, vf_loss2)
            return vf_loss

        pi_new_dist = dist_class(curr_logits)
        pi_old_dist = dist_class(behaviour_logits)

        surr_loss = reduce_mean_valid(surrogate_loss(actions, pi_new_dist, pi_old_dist, advantages, clip_loss))
        kl_loss = reduce_mean_valid(kl_loss(pi_new_dist, pi_old_dist))
        vf_loss = reduce_mean_valid(vf_loss(value_fn, value_targets, vf_preds, vf_clip_param))
        entropy_loss = reduce_mean_valid(entropy_loss(pi_new_dist))

        total_loss = - surr_loss + cur_kl_coeff * action_kl + vf_loss_coeff * vf_loss - entropy_coeff * curr_entropy

        return total_loss, surr_loss, kl_loss, vf_loss, entropy_loss

# This is the Meta-Update computation graph for master worker
class MAMLLoss(object):

    def __init__(self,
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
            use_gae=True):

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
            pi_new, value_fn = self.feed_forward(self.obs[0][i], self.policy_vars, policy_config = config["model"])
            pi_new_logits.append(pi_new)
            value_fns.append(value_fn)
            current_policy_vars.append(self.policy_vars)

        self.pi_new_logits = pi_new_logits

        inner_kls = []
        inner_ppo_loss = []

        # Recompute weights for inner-adaptation (since this is also incoporated in meta objective loss function)
        for step in range(self.inner_adaptation_steps):
            kls = []
            for i in range(self.num_tasks):
                # Loss Function Shenanigans
                ppo_loss, _, kl_loss, _, _ = PPOLoss(
                    actions = self.actions[step][i],
                    curr_logits = pi_new_logits[i],
                    behaviour_logits = self.behaviour_logits[step][i],
                    advantages = self.advantages[step][i],
                    value_fn = value_fns[i],
                    value_targets = self.value_targets[step][i],
                    vf_preds = self.vf_preds[step][i],
                    cur_kl_coeff = cur_kl_coeff,
                    entropy_coeff = entropy_coeff,
                    clip_param = clip_param,
                    vf_clip_param = vf_clip_param,
                    vf_loss_coeff = vf_loss_coeff,
                    clip_loss = False
                    )

                adapted_policy_vars = self.compute_updated_variables(ppo_loss, current_policy_vars[i])
                pi_new_logits[i], value_fns[i] = self.feed_forward(self.obs[step+1][i], adapted_policy_vars, policy_config=config["model"])
                current_policy_vars[i] = adapted_policy_vars
                kls.append(kl_loss)
                inner_ppo_loss.append(ppo_loss)

            self.kls = kls
            inner_kls.append(kls)

        mean_inner_kl = tf.stack([tf.reduce_mean(tf.stack(inner_kl)) for inner_kl in inner_kls])
        self.mean_inner_kl = mean_inner_kl

        ppo_obj = []
        for i in range(self.num_tasks):
            ppo_loss, _, kl_loss, _, _ = PPOLoss(
                    actions = self.actions[self.inner_adaptation_steps][i],
                    curr_logits = pi_new_logits[i],
                    behaviour_logits = self.behaviour_logits[self.inner_adaptation_steps][i],
                    advantages = self.advantages[self.inner_adaptation_steps][i],
                    value_fn = value_fns[i],
                    value_targets = self.value_targets[self.inner_adaptation_steps][i],
                    vf_preds = self.vf_preds[self.inner_adaptation_steps][i],
                    cur_kl_coeff = cur_kl_coeff,
                    entropy_coeff = entropy_coeff,
                    clip_param = clip_param,
                    vf_clip_param = vf_clip_param,
                    vf_loss_coeff = vf_loss_coeff,
                    clip_loss = True
                    )
            ppo_obj.append(ppo_loss)
        self.kl_loss = tf.reduce_mean(tf.multiply(cur_kl_coeff, mean_inner_kl))
        self.loss = tf.reduce_mean(tf.stack(ppo_obj, axis=0)) + self.kl_loss
        self.loss = tf.Print(self.loss, ["Meta-Loss", self.loss, mean_inner_kl])

    def feed_forward(self, obs, policy_vars, policy_config, context = None):
        # Hacky for now, reconstruct FC network with defined weights
        # @mluo: TODO for any network
        def fc_network(inp, network_vars, hidden_nonlinearity, output_nonlinearity, policy_config, hyper_vars = None, context=None):
            hidden_sizes = policy_config["fcnet_hiddens"]
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
                    if "fc_out" not in name:
                        x = hidden_nonlinearity(x)
                    elif "fc_out" in name:
                        x = output_nonlinearity(x)
                    else:
                        raise NameError
                    bias_added = False
            return x

        policyn_vars = {}
        valuen_vars = {}
        log_std = None
        for name, param in policy_vars.items():
            if 'value_function' in name:
                valuen_vars[name] = param
            elif "log_std" in name:
                log_std = param
            else:
                policyn_vars[name] = param

        output_nonlinearity = tf.identity
        hidden_nonlinearity = get_activation_fn(policy_config["fcnet_activation"])
        
        pi_new_logits = fc_network(obs, policyn_vars, hidden_nonlinearity, output_nonlinearity, policy_config)
        if log_std is not None:
            pi_new_logits = tf.concat(
                [pi_new_logits, 0.0 * pi_new_logits + log_std], 1)
        value_fn = fc_network(obs, valuen_vars, hidden_nonlinearity, output_nonlinearity, policy_config)

        return pi_new_logits, tf.reshape(value_fn, [-1])

    def compute_updated_variables(self, loss, network_vars):
        grad = tf.gradients(loss, list(network_vars.values()))
        adapted_vars = {}
        counter =0
        for i, tup in enumerate(network_vars.items()):
            name, var = tup
            if grad[i] is None:
                adapted_vars[name] = var
            else:
                adapted_vars[name] = var - self.config["inner_lr"]*grad[i] 
        return adapted_vars

    def split_placeholders(self, placeholder, split):
        inner_placeholder_list = tf.split(placeholder, tf.math.reduce_sum(split, axis=1), axis=0)
        placeholder_list = []
        for index, split_placeholder in enumerate(inner_placeholder_list):
            placeholder_list.append(tf.split(split_placeholder, split[index], axis=0))
        return placeholder_list

def maml_loss(policy, model, dist_class, train_batch):
    logits, state = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    split_ph = tf.placeholder(tf.int32, name="meta-batch-splitting", shape=(policy.config["inner_adaptation_steps"]+1, policy.config["num_workers"]))

    if policy.config["worker_index"]:
        policy.loss_obj, _, _, _, _ = PPOLoss(
            dist_class=dist_class,
            actions=train_batch[SampleBatch.ACTIONS],
            curr_logits=,
            behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
            advantages=train_batch[Postprocessing.ADVANTAGES],
            value_fn=model.value_function(),
            value_targets=train_batch[Postprocessing.VALUE_TARGETS],
            vf_preds=train_batch[SampleBatch.VF_PREDS],
            cur_kl_coeff=policy.kl_coeff,
            entropy_coeff=policy.entropy_coeff,
            clip_param=policy.config["clip_param"],
            vf_clip_param=policy.config["vf_clip_param"],
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            clip_loss=False
        )
    else:
        policy.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                              tf.get_variable_scope().name)
        policy.loss_obj = MAMLLoss(
                model = self.model,
                value_targets = train_batch[Postprocessing.VALUE_TARGETS],
                advantages = train_batch[Postprocessing.ADVANTAGES],
                actions = train_batch[SampleBatch.ACTIONS],
                behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS],
                vf_preds = train_batch[SampleBatch.VF_PREDS],
                cur_kl_coeff = policy.kl_coeff, 
                policy_vars = policy.var_list, 
                obs = train_batch[SampleBatch.CUR_OBS],  
                num_tasks = policy.config["num_workers"],
                split = split_ph,
                config = policy.config,
                inner_adaptation_steps=policy.config["inner_adaptation_steps"],
                entropy_coeff=policy.config["entropy_coeff"],
                clip_param=policy.config["clip_param"],
                vf_clip_param=policy.config["vf_clip_param"],
                vf_loss_coeff=policy.config["vf_loss_coeff"],
                use_gae=policy.config["use_gae"]
                )

    return policy.loss_obj.loss

def maml_stats(policy, train_batch):
    if policy.config["worker_index"]:
        return {
            "worker_loss": policy.loss_obj.loss
        }
    else:
        return {
            "cur_kl_coeff": tf.cast(policy.kl_coeff, tf.float64),
            "cur_lr": tf.cast(policy.cur_lr, tf.float64),
            "total_loss": policy.loss_obj.loss,
            "policy_loss": policy.loss_obj.mean_policy_loss,
            "vf_loss": policy.loss_obj.mean_vf_loss,
            "vf_explained_var": explained_variance(
                train_batch[Postprocessing.VALUE_TARGETS],
                policy.model.value_function()),
            "kl": policy.loss_obj.mean_kl,
            "entropy": policy.loss_obj.mean_entropy,
            "entropy_coeff": tf.cast(policy.entropy_coeff, tf.float64),
        }

class ValueNetworkMixin:
    def __init__(self, obs_space, action_space, config):
        if config["use_gae"]:
            @make_tf_callable(self.get_session())
            def value(ob, prev_action, prev_reward, *state):
                model_out, _ = self.model({
                    SampleBatch.CUR_OBS: tf.convert_to_tensor([ob]),
                    SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                        [prev_action]),
                    SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                        [prev_reward]),
                    "is_training": tf.convert_to_tensor([False]),
                }, [tf.convert_to_tensor([s]) for s in state],
                                          tf.convert_to_tensor([1]))
                return self.model.value_function()[0]

        else:

            @make_tf_callable(self.get_session())
            def value(ob, prev_action, prev_reward, *state):
                return tf.constant(0.0)

        self._value = value


class KLCoeffMixin:
    def __init__(self, config):
        self.kl_coeff_val = [config["kl_coeff"]]*config["inner_adaptation_steps"]
        self.kl_target = self.config["kl_target"]
        self.kl_coeff = tf.get_variable(
            initializer=tf.constant_initializer(self.kl_coeff_val),
            name="kl_coeff",
            shape=(config["inner_adaptation_steps"]),
            trainable=False,
            dtype=tf.float32)

    def update_kls(self, sampled_kls):
        for i, kl in enumerate(sampled_kls): 
            if kl < self.kl_target/1.5:
                self.kl_coeff_val[i] *= 0.5
            elif kl > 1.5 * self.kl_target:
                self.kl_coeff_val[i] *= 2.0
        self.kl_coeff.load(self.kl_coeff_val, session=self.sess)
        return self.kl_coeff_val


# Simple SGD Optimizer for Task-specific Works
class SimpleOptimizer(optimizer.Optimizer):
    def __init__(self, learning_rate=0.001,use_locking=False, name="SimpleOptimizer"):
        super(SimpleOptimizer, self).__init__(use_locking, name)
        self._lr = learning_rate
        # Tensor versions of the constructor arguments, created in _prepare().
        self._lr_t = None

    def _prepare(self):
        self._lr_t = ops.convert_to_tensor(self._lr, name="learning_rate")

    def _apply_dense(self, grad, var):
        lr_t = math_ops.cast(self._lr_t, var.dtype.base_dtype)
        var_update = state_ops.assign_sub(var, lr_t*grad) #Update 'ref' by subtracting 'value
        #Create an op that groups multiple operations.
        #When this op finishes, all ops in input have finished
        return control_flow_ops.group(*[var_update])

    def _apply_sparse(self, grad, var):
        raise NotImplementedError("Sparse gradient updates are not supported.")


def maml_optimizer_fn(self, config):
    if not config["worker_index"]:
        return tf.train.AdamOptimizer(learning_rate=config["lr"])
    return SimpleOptimizer(learning_rate=config["inner_lr"])


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    KLCoeffMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])


MAMLTFPolicy = build_tf_policy(
    name="MAMLTFPolicy",
    get_default_config=lambda: ray.rllib.agents.maml.maml.DEFAULT_CONFIG,
    loss_fn=maml_loss,
    stats_fn=maml_stats,
    optimizer_fn = maml_optimizer_fn,
    extra_action_fetches_fn=vf_preds_fetches,
    postprocess_fn=postprocess_ppo_gae,
    gradients_fn=clip_gradients,
    before_init=setup_config,
    before_loss_init=setup_mixins,
    mixins=[
        EntropyCoeffSchedule, KLCoeffMixin,
        ValueNetworkMixin
    ])