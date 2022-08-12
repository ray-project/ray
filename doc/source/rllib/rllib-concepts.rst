.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-policy-walkthrough:

How To Customize Policies
=========================

This page describes the internal concepts used to implement algorithms in RLlib.
You might find this useful if modifying or adding new algorithms to RLlib.

Policy classes encapsulate the core numerical components of RL algorithms.
This typically includes the policy model that determines actions to take, a trajectory postprocessor for experiences, and a loss function to improve the policy given post-processed experiences.
For a simple example, see the policy gradients `policy definition <https://github.com/ray-project/ray/blob/master/rllib/algorithms/pg/pg_tf_policy.py>`__.

Most interaction with deep learning frameworks is isolated to the `Policy interface <https://github.com/ray-project/ray/blob/master/rllib/policy/policy.py>`__, allowing RLlib to support multiple frameworks.
To simplify the definition of policies, RLlib includes `Tensorflow <#building-policies-in-tensorflow>`__ and `PyTorch-specific <#building-policies-in-pytorch>`__ templates.
You can also write your own from scratch. Here is an example:

.. code-block:: python

    class CustomPolicy(Policy):
        """Example of a custom policy written from scratch.

        You might find it more convenient to use the `build_tf_policy` and
        `build_torch_policy` helpers instead for a real policy, which are
        described in the next sections.
        """

        def __init__(self, observation_space, action_space, config):
            Policy.__init__(self, observation_space, action_space, config)
            # example parameter
            self.w = 1.0

        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):
            # return action batch, RNN states, extra values to include in batch
            return [self.action_space.sample() for _ in obs_batch], [], {}

        def learn_on_batch(self, samples):
            # implement your learning code here
            return {}  # return stats

        def get_weights(self):
            return {"w": self.w}

        def set_weights(self, weights):
            self.w = weights["w"]


The above basic policy, when run, will produce batches of observations with the basic ``obs``, ``new_obs``, ``actions``, ``rewards``, ``dones``, and ``infos`` columns.
There are two more mechanisms to pass along and emit extra information:

**Policy recurrent state**: Suppose you want to compute actions based on the current timestep of the episode.
While it is possible to have the environment provide this as part of the observation, we can instead compute and store it as part of the Policy recurrent state:

.. code-block:: python

    def get_initial_state(self):
        """Returns initial RNN state for the current policy."""
        return [0]  # list of single state element (t=0)
                    # you could also return multiple values, e.g., [0, "foo"]

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        assert len(state_batches) == len(self.get_initial_state())
        new_state_batches = [[
            t + 1 for t in state_batches[0]
        ]]
        return ..., new_state_batches, {}

    def learn_on_batch(self, samples):
        # can access array of the state elements at each timestep
        # or state_in_1, 2, etc. if there are multiple state elements
        assert "state_in_0" in samples.keys()
        assert "state_out_0" in samples.keys()


**Extra action info output**: You can also emit extra outputs at each step which will be available for learning on. For example, you might want to output the behaviour policy logits as extra action info, which can be used for importance weighting, but in general arbitrary values can be stored here (as long as they are convertible to numpy arrays):

.. code-block:: python

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        action_info_batch = {
            "some_value": ["foo" for _ in obs_batch],
            "other_value": [12345 for _ in obs_batch],
        }
        return ..., [], action_info_batch

    def learn_on_batch(self, samples):
        # can access array of the extra values at each timestep
        assert "some_value" in samples.keys()
        assert "other_value" in samples.keys()


Policies in Multi-Agent
~~~~~~~~~~~~~~~~~~~~~~~

Beyond being agnostic of framework implementation, one of the main reasons to have a Policy abstraction is for use in multi-agent environments. For example, the `rock-paper-scissors example <rllib-env.html#rock-paper-scissors-example>`__ shows how you can leverage the Policy abstraction to evaluate heuristic policies against learned policies.

Building Policies in TensorFlow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section covers how to build a TensorFlow RLlib policy using ``tf_policy_template.build_tf_policy()``.

To start, you first have to define a loss function. In RLlib, loss functions are defined over batches of trajectory data produced by policy evaluation. A basic policy gradient loss that only tries to maximize the 1-step reward can be defined as follows:

.. code-block:: python

    import tensorflow as tf
    from ray.rllib.policy.sample_batch import SampleBatch

    def policy_gradient_loss(policy, model, dist_class, train_batch):
        actions = train_batch[SampleBatch.ACTIONS]
        rewards = train_batch[SampleBatch.REWARDS]
        logits, _ = model.from_batch(train_batch)
        action_dist = dist_class(logits, model)
        return -tf.reduce_mean(action_dist.logp(actions) * rewards)

In the above snippet, ``actions`` is a Tensor placeholder of shape ``[batch_size, action_dim...]``, and ``rewards`` is a placeholder of shape ``[batch_size]``. The ``action_dist`` object is an `ActionDistribution <rllib-package-ref.html#ray.rllib.models.ActionDistribution>`__ that is parameterized by the output of the neural network policy model. Passing this loss function to ``build_tf_policy`` is enough to produce a very basic TF policy:

.. code-block:: python

    from ray.rllib.policy.tf_policy_template import build_tf_policy

    # <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
    MyTFPolicy = build_tf_policy(
        name="MyTFPolicy",
        loss_fn=policy_gradient_loss)

We can create an `Algorithm <#algorithms>`__ and try running this policy on a toy env with two parallel rollout workers:

.. code-block:: python

    import ray
    from ray import tune
    from ray.rllib.algorithms.algorithm import Algorithm

    class MyAlgo(Algorithm):
        def get_default_policy_class(self, config):
            return MyTFPolicy

    ray.init()
    tune.Tuner(MyAlgo, param_space={"env": "CartPole-v0", "num_workers": 2}).fit()


If you run the above snippet `(runnable file here) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_tf_policy.py>`__, you'll probably notice that CartPole doesn't learn so well:

.. code-block:: bash

    == Status ==
    Using FIFO scheduling algorithm.
    Resources requested: 3/4 CPUs, 0/0 GPUs
    Memory usage on this node: 4.6/12.3 GB
    Result logdir: /home/ubuntu/ray_results/MyAlgTrainer
    Number of trials: 1 ({'RUNNING': 1})
    RUNNING trials:
     - MyAlgTrainer_CartPole-v0_0:	RUNNING, [3 CPUs, 0 GPUs], [pid=26784],
                                        32 s, 156 iter, 62400 ts, 23.1 rew

Let's modify our policy loss to include rewards summed over time. To enable this advantage calculation, we need to define a *trajectory postprocessor* for the policy. This can be done by defining ``postprocess_fn``:

.. code-block:: python

    from ray.rllib.evaluation.postprocessing import compute_advantages, \
        Postprocessing

    def postprocess_advantages(policy,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        return compute_advantages(
            sample_batch, 0.0, policy.config["gamma"], use_gae=False)

    def policy_gradient_loss(policy, model, dist_class, train_batch):
        logits, _ = model.from_batch(train_batch)
        action_dist = dist_class(logits, model)
        return -tf.reduce_mean(
            action_dist.logp(train_batch[SampleBatch.ACTIONS]) *
            train_batch[Postprocessing.ADVANTAGES])

    MyTFPolicy = build_tf_policy(
        name="MyTFPolicy",
        loss_fn=policy_gradient_loss,
        postprocess_fn=postprocess_advantages)

The ``postprocess_advantages()`` function above uses calls RLlib's ``compute_advantages`` function to compute advantages for each timestep. If you re-run the algorithm with this improved policy, you'll find that it quickly achieves the max reward of 200.

You might be wondering how RLlib makes the advantages placeholder automatically available as ``train_batch[Postprocessing.ADVANTAGES]``. When building your policy, RLlib will create a "dummy" trajectory batch where all observations, actions, rewards, etc. are zeros. It then calls your ``postprocess_fn``, and generates TF placeholders based on the numpy shapes of the postprocessed batch. RLlib tracks which placeholders that ``loss_fn`` and ``stats_fn`` access, and then feeds the corresponding sample data into those placeholders during loss optimization. You can also access these placeholders via ``policy.get_placeholder(<name>)`` after loss initialization.

**Example 1: Proximal Policy Optimization**

In the above section you saw how to compose a simple policy gradient algorithm with RLlib.
In this example, we'll dive into how PPO is defined within RLlib and how you can modify it.
First, check out the `PPO definition <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py>`__:

.. code-block:: python

    class PPO(Algorithm):
        @classmethod
        @override(Algorithm)
        def get_default_config(cls) -> AlgorithmConfigDict:
            return DEFAULT_CONFIG

        @override(Algorithm)
        def validate_config(self, config: AlgorithmConfigDict) -> None:
            ...

        @override(Algorithm)
        def get_default_policy_class(self, config):
            return PPOTFPolicy

        @override(Algorithm)
        def training_step(self):
            ...

Besides some boilerplate for defining the PPO configuration and some warnings, the most important method to take note of is the ``training_step``.

The algorithm's `training step method <core-concepts.html#training-step-method>`__ defines the distributed training workflow.
Depending on the ``simple_optimizer`` config setting,
PPO can switch between a simple, synchronous optimizer, or a multi-GPU one that implements
pre-loading of the batch to the GPU for higher performance on repeated minibatch updates utilizing
the same pre-loaded batch:

.. code-block:: python

        def training_step(self) -> ResultDict:
        # Collect SampleBatches from sample workers until we have a full batch.
        if self._by_agent_steps:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_agent_steps=self.config["train_batch_size"]
            )
        else:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_env_steps=self.config["train_batch_size"]
            )
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Standardize advantages
        train_batch = standardize_fields(train_batch, ["advantages"])
        # Train
        if self.config["simple_optimizer"]:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
        }

        # Update weights - after learning on the local worker - on all remote
        # workers.
        if self.workers.remote_workers():
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights(global_vars=global_vars)

        # For each policy: update KL scale and warn about possible issues
        for policy_id, policy_info in train_results.items():
            # Update KL loss with dynamic scaling
            # for each (possibly multiagent) policy we are training
            kl_divergence = policy_info[LEARNER_STATS_KEY].get("kl")
            self.get_policy(policy_id).update_kl(kl_divergence)

        # Update global vars on local worker as well.
        self.workers.local_worker().set_global_vars(global_vars)

        return train_results

Now let's look at each PPO policy definition:

.. code-block:: python

    PPOTFPolicy = build_tf_policy(
        name="PPOTFPolicy",
        get_default_config=lambda: ray.rllib.algorithms.ppo.ppo.PPOConfig().to_dict(),
        loss_fn=ppo_surrogate_loss,
        stats_fn=kl_and_loss_stats,
        extra_action_out_fn=vf_preds_and_logits_fetches,
        postprocess_fn=postprocess_ppo_gae,
        gradients_fn=clip_gradients,
        before_loss_init=setup_mixins,
        mixins=[LearningRateSchedule, KLCoeffMixin, ValueNetworkMixin])

``stats_fn``: The stats function returns a dictionary of Tensors that will be reported with the training results. This also includes the ``kl`` metric which is used by the algorithm to adjust the KL penalty. Note that many of the values below reference ``policy.loss_obj``, which is assigned by ``loss_fn`` (not shown here since the PPO loss is quite complex). RLlib will always call ``stats_fn`` after ``loss_fn``, so you can rely on using values saved by ``loss_fn`` as part of your statistics:

.. code-block:: python

    def kl_and_loss_stats(policy, train_batch):
        policy.explained_variance = explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS], policy.model.value_function())

        stats_fetches = {
            "cur_kl_coeff": policy.kl_coeff,
            "cur_lr": tf.cast(policy.cur_lr, tf.float64),
            "total_loss": policy.loss_obj.loss,
            "policy_loss": policy.loss_obj.mean_policy_loss,
            "vf_loss": policy.loss_obj.mean_vf_loss,
            "vf_explained_var": policy.explained_variance,
            "kl": policy.loss_obj.mean_kl,
            "entropy": policy.loss_obj.mean_entropy,
        }

        return stats_fetches

``extra_actions_fetches_fn``: This function defines extra outputs that will be recorded when generating actions with the policy. For example, this enables saving the raw policy logits in the experience batch, which e.g. means it can be referenced in the PPO loss function via ``batch[BEHAVIOUR_LOGITS]``. Other values such as the current value prediction can also be emitted for debugging or optimization purposes:

.. code-block:: python

    def vf_preds_and_logits_fetches(policy):
        return {
            SampleBatch.VF_PREDS: policy.model.value_function(),
            BEHAVIOUR_LOGITS: policy.model.last_output(),
        }

``gradients_fn``: If defined, this function returns TF gradients for the loss function. You'd typically only want to override this to apply transformations such as gradient clipping:

.. code-block:: python

    def clip_gradients(policy, optimizer, loss):
        if policy.config["grad_clip"] is not None:
            grads = tf.gradients(loss, policy.model.trainable_variables())
            policy.grads, _ = tf.clip_by_global_norm(grads,
                                                     policy.config["grad_clip"])
            clipped_grads = list(zip(policy.grads, policy.model.trainable_variables()))
            return clipped_grads
        else:
            return optimizer.compute_gradients(
                loss, colocate_gradients_with_ops=True)

``mixins``: To add arbitrary stateful components, you can add mixin classes to the policy. Methods defined by these mixins will have higher priority than the base policy class, so you can use these to override methods (as in the case of ``LearningRateSchedule``), or define extra methods and attributes (e.g., ``KLCoeffMixin``, ``ValueNetworkMixin``). Like any other Python superclass, these should be initialized at some point, which is what the ``setup_mixins`` function does:

.. code-block:: python

    def setup_mixins(policy, obs_space, action_space, config):
        ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
        KLCoeffMixin.__init__(policy, config)
        LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])

In PPO we run ``setup_mixins`` before the loss function is called (i.e., ``before_loss_init``), but other callbacks you can use include ``before_init`` and ``after_init``.

**Example 2: Deep Q Networks**

Let's look at how to implement a different family of policies, by looking at the `SimpleQ policy definition <https://github.com/ray-project/ray/blob/master/rllib/algorithms/simple_q/simple_q_tf_policy.py>`__:

.. code-block:: python

    SimpleQPolicy = build_tf_policy(
        name="SimpleQPolicy",
        get_default_config=lambda: ray.rllib.algorithms.dqn.dqn.DEFAULT_CONFIG,
        make_model=build_q_models,
        action_sampler_fn=build_action_sampler,
        loss_fn=build_q_losses,
        extra_action_feed_fn=exploration_setting_inputs,
        extra_action_out_fn=lambda policy: {"q_values": policy.q_values},
        extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
        before_init=setup_early_mixins,
        after_init=setup_late_mixins,
        obs_include_prev_action_reward=False,
        mixins=[
            ExplorationStateMixin,
            TargetNetworkMixin,
        ])

The biggest difference from the policy gradient policies you saw previously is that SimpleQPolicy defines its own ``make_model`` and ``action_sampler_fn``. This means that the policy builder will not internally create a model and action distribution, rather it will call ``build_q_models`` and ``build_action_sampler`` to get the output action tensors.

The model creation function actually creates two different models for DQN: the base Q network, and also a target network. It requires each model to be of type ``SimpleQModel``, which implements a ``get_q_values()`` method. The model catalog will raise an error if you try to use a custom ModelV2 model that isn't a subclass of SimpleQModel. Similarly, the full DQN policy requires models to subclass ``DistributionalQModel``, which implements ``get_q_value_distributions()`` and ``get_state_value()``:

.. code-block:: python

    def build_q_models(policy, obs_space, action_space, config):
        ...

        policy.q_model = ModelCatalog.get_model_v2(
            obs_space,
            action_space,
            num_outputs,
            config["model"],
            framework="tf",
            name=Q_SCOPE,
            model_interface=SimpleQModel,
            q_hiddens=config["hiddens"])

        policy.target_q_model = ModelCatalog.get_model_v2(
            obs_space,
            action_space,
            num_outputs,
            config["model"],
            framework="tf",
            name=Q_TARGET_SCOPE,
            model_interface=SimpleQModel,
            q_hiddens=config["hiddens"])

        return policy.q_model

The action sampler is straightforward, it just takes the q_model, runs a forward pass, and returns the argmax over the actions:

.. code-block:: python

    def build_action_sampler(policy, q_model, input_dict, obs_space, action_space,
                             config):
        # do max over Q values...
        ...
        return action, action_logp

The remainder of DQN is similar to other algorithms. Target updates are handled by a ``after_optimizer_step`` callback that periodically copies the weights of the Q network to the target.

Finally, note that you do not have to use ``build_tf_policy`` to define a TensorFlow policy. You can alternatively subclass ``Policy``, ``TFPolicy``, or ``DynamicTFPolicy`` as convenient.

Building Policies in TensorFlow Eager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Policies built with ``build_tf_policy`` (most of the reference algorithms are)
can be run in eager mode by setting
the ``"framework": "tf2"`` / ``"eager_tracing": true`` config options or
using ``rllib train '{"framework": "tf2", "eager_tracing": true}'``.
This will tell RLlib to execute the model forward pass, action distribution,
loss, and stats functions in eager mode.

Eager mode makes debugging much easier, since you can now use line-by-line
debugging with breakpoints or Python ``print()`` to inspect
intermediate tensor values.
However, eager can be slower than graph mode unless tracing is enabled.

You can also selectively leverage eager operations within graph mode
execution with `tf.py_function <https://www.tensorflow.org/api_docs/python/tf/py_function>`__.
Here's an example of using eager ops embedded
`within a loss function <https://github.com/ray-project/ray/blob/master/rllib/examples/eager_execution.py>`__.

Building Policies in PyTorch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Defining a policy in PyTorch is quite similar to that for TensorFlow (and the process of defining a algorithm given a Torch policy is exactly the same).
Here's a simple example of a trivial torch policy `(runnable file here) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_torch_policy.py>`__:

.. code-block:: python

    from ray.rllib.policy.sample_batch import SampleBatch
    from ray.rllib.policy.torch_policy_template import build_torch_policy

    def policy_gradient_loss(policy, model, dist_class, train_batch):
        logits, _ = model.from_batch(train_batch)
        action_dist = dist_class(logits)
        log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
        return -train_batch[SampleBatch.REWARDS].dot(log_probs)

    # <class 'ray.rllib.policy.torch_policy_template.MyTorchPolicy'>
    MyTorchPolicy = build_torch_policy(
        name="MyTorchPolicy",
        loss_fn=policy_gradient_loss)

Now, building on the TF examples above, let's look at how the `A3C torch policy <https://github.com/ray-project/ray/blob/master/rllib/algorithms/a3c/a3c_torch_policy.py>`__ is defined:

.. code-block:: python

    A3CTorchPolicy = build_torch_policy(
        name="A3CTorchPolicy",
        get_default_config=lambda: ray.rllib.algorithms.a3c.a3c.DEFAULT_CONFIG,
        loss_fn=actor_critic_loss,
        stats_fn=loss_and_entropy_stats,
        postprocess_fn=add_advantages,
        extra_action_out_fn=model_value_predictions,
        extra_grad_process_fn=apply_grad_clipping,
        optimizer_fn=torch_optimizer,
        mixins=[ValueNetworkMixin])

``loss_fn``: Similar to the TF example, the actor critic loss is defined over ``batch``. We imperatively execute the forward pass by calling ``model()`` on the observations followed by ``dist_class()`` on the output logits. The output Tensors are saved as attributes of the policy object (e.g., ``policy.entropy = dist.entropy.mean()``), and we return the scalar loss:

.. code-block:: python

    def actor_critic_loss(policy, model, dist_class, train_batch):
        logits, _ = model.from_batch(train_batch)
        values = model.value_function()
        action_dist = dist_class(logits)
        log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
        policy.entropy = action_dist.entropy().mean()
        ...
        return overall_err

``stats_fn``: The stats function references ``entropy``, ``pi_err``, and ``value_err`` saved from the call to the loss function, similar in the PPO TF example:

.. code-block:: python

    def loss_and_entropy_stats(policy, train_batch):
        return {
            "policy_entropy": policy.entropy.item(),
            "policy_loss": policy.pi_err.item(),
            "vf_loss": policy.value_err.item(),
        }

``extra_action_out_fn``: We save value function predictions given model outputs. This makes the value function predictions of the model available in the trajectory as ``batch[SampleBatch.VF_PREDS]``:

.. code-block:: python

    def model_value_predictions(policy, input_dict, state_batches, model):
        return {SampleBatch.VF_PREDS: model.value_function().cpu().numpy()}

``postprocess_fn`` and ``mixins``: Similar to the PPO example, we need access to the value function during postprocessing (i.e., ``add_advantages`` below calls ``policy._value()``. The value function is exposed through a mixin class that defines the method:

.. code-block:: python

    def add_advantages(policy,
                       sample_batch,
                       other_agent_batches=None,
                       episode=None):
        completed = sample_batch[SampleBatch.DONES][-1]
        if completed:
            last_r = 0.0
        else:
            last_r = policy._value(sample_batch[SampleBatch.NEXT_OBS][-1])
        return compute_advantages(sample_batch, last_r, policy.config["gamma"],
                                  policy.config["lambda"])

    class ValueNetworkMixin(object):
        def _value(self, obs):
            with self.lock:
                obs = torch.from_numpy(obs).float().unsqueeze(0).to(self.device)
                _, _, vf, _ = self.model({"obs": obs}, [])
                return vf.detach().cpu().numpy().squeeze()

You can find the full policy definition in `a3c_torch_policy.py <https://github.com/ray-project/ray/blob/master/rllib/algorithms/a3c/a3c_torch_policy.py>`__.

In summary, the main differences between the PyTorch and TensorFlow policy builder functions is that the TF loss and stats functions are built symbolically when the policy is initialized, whereas for PyTorch (or TensorFlow Eager) these functions are called imperatively each time they are used.

Extending Existing Policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``with_updates`` method on Trainers and Policy objects built with ``make_*`` to create a copy of the object with some changes, for example:

.. code-block:: python

    from ray.rllib.algorithms.ppo import PPO
    from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTFPolicy

    CustomPolicy = PPOTFPolicy.with_updates(
        name="MyCustomPPOTFPolicy",
        loss_fn=some_custom_loss_fn)

    CustomTrainer = PPOTrainer.with_updates(
        default_policy=CustomPolicy)

.. include:: /_includes/rllib/announcement_bottom.rst
