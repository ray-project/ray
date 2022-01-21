.. include:: we_are_hiring.rst

RLlib Concepts and Custom Algorithms
====================================

This page describes the internal concepts used to implement algorithms in RLlib. You might find this useful if modifying or adding new algorithms to RLlib. The three main concepts covered here are `policies <#policies>`__, `policy evaluation <#policy-evaluation>`__, and `execution plans <#execution-plans>`__. RLlib relies on these concepts to define algorithms in a distributed, framework-agnostic way that is amenable to customization and operation in many types of RL environments.

Policies
--------

Policy classes encapsulate the core numerical components of RL algorithms. This typically includes the policy model that determines actions to take, a trajectory postprocessor for experiences, and a loss function to improve the policy given postprocessed experiences. For a simple example, see the policy gradients `policy definition <https://github.com/ray-project/ray/blob/master/rllib/agents/pg/pg_tf_policy.py>`__.

Most interaction with deep learning frameworks is isolated to the `Policy interface <https://github.com/ray-project/ray/blob/master/rllib/policy/policy.py>`__, allowing RLlib to support multiple frameworks. To simplify the definition of policies, RLlib includes `Tensorflow <#building-policies-in-tensorflow>`__ and `PyTorch-specific <#building-policies-in-pytorch>`__ templates. You can also write your own from scratch. Here is an example:

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


The above basic policy, when run, will produce batches of observations with the basic ``obs``, ``new_obs``, ``actions``, ``rewards``, ``dones``, and ``infos`` columns. There are two more mechanisms to pass along and emit extra information:

**Policy recurrent state**: Suppose you want to compute actions based on the current timestep of the episode. While it is possible to have the environment provide this as part of the observation, we can instead compute and store it as part of the Policy recurrent state:

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

We can create a `Trainer <#trainers>`__ and try running this policy on a toy env with two parallel rollout workers:

.. code-block:: python

    import ray
    from ray import tune
    from ray.rllib.agents.trainer_template import build_trainer

    # <class 'ray.rllib.agents.trainer_template.MyCustomTrainer'>
    MyTrainer = build_trainer(
        name="MyCustomTrainer",
        default_policy=MyTFPolicy)

    ray.init()
    tune.run(MyTrainer, config={"env": "CartPole-v0", "num_workers": 2})


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

The ``postprocess_advantages()`` function above uses calls RLlib's ``compute_advantages`` function to compute advantages for each timestep. If you re-run the trainer with this improved policy, you'll find that it quickly achieves the max reward of 200.

You might be wondering how RLlib makes the advantages placeholder automatically available as ``train_batch[Postprocessing.ADVANTAGES]``. When building your policy, RLlib will create a "dummy" trajectory batch where all observations, actions, rewards, etc. are zeros. It then calls your ``postprocess_fn``, and generates TF placeholders based on the numpy shapes of the postprocessed batch. RLlib tracks which placeholders that ``loss_fn`` and ``stats_fn`` access, and then feeds the corresponding sample data into those placeholders during loss optimization. You can also access these placeholders via ``policy.get_placeholder(<name>)`` after loss initialization.

**Example 1: Proximal Policy Optimization**

In the above section you saw how to compose a simple policy gradient algorithm with RLlib. In this example, we'll dive into how PPO was built with RLlib and how you can modify it. First, check out the `PPO trainer definition <https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo.py>`__:

.. code-block:: python

    PPOTrainer = build_trainer(
        name="PPOTrainer",
        default_config=DEFAULT_CONFIG,
        default_policy=PPOTFPolicy,
        validate_config=validate_config,
        execution_plan=execution_plan)

Besides some boilerplate for defining the PPO configuration and some warnings, the most important argument to take note of is the ``execution_plan``.

The trainer's `execution plan <#execution-plans>`__ defines the distributed training workflow. Depending on the ``simple_optimizer`` trainer config, PPO can switch between a simple synchronous plan, or a multi-GPU plan that implements minibatch SGD (the default):

.. code-block:: python

    def execution_plan(workers: WorkerSet, config: TrainerConfigDict):
        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # Collect large batches of relevant experiences & standardize.
        rollouts = rollouts.for_each(
            SelectExperiences(workers.trainable_policies()))
        rollouts = rollouts.combine(
            ConcatBatches(min_batch_size=config["train_batch_size"]))
        rollouts = rollouts.for_each(StandardizeFields(["advantages"]))

        if config["simple_optimizer"]:
            train_op = rollouts.for_each(
                TrainOneStep(
                    workers,
                    num_sgd_iter=config["num_sgd_iter"],
                    sgd_minibatch_size=config["sgd_minibatch_size"]))
        else:
            train_op = rollouts.for_each(
                TrainTFMultiGPU(
                    workers,
                    sgd_minibatch_size=config["sgd_minibatch_size"],
                    num_sgd_iter=config["num_sgd_iter"],
                    num_gpus=config["num_gpus"],
                    rollout_fragment_length=config["rollout_fragment_length"],
                    num_envs_per_worker=config["num_envs_per_worker"],
                    train_batch_size=config["train_batch_size"],
                    shuffle_sequences=config["shuffle_sequences"],
                    _fake_gpus=config["_fake_gpus"]))

        # Update KL after each round of training.
        train_op = train_op.for_each(lambda t: t[1]).for_each(UpdateKL(workers))

        return StandardMetricsReporting(train_op, workers, config) \
            .for_each(lambda result: warn_about_bad_reward_scales(config, result))

Suppose we want to customize PPO to use an asynchronous-gradient optimization strategy similar to A3C. To do that, we could swap out its execution plan to that of A3C's:

.. code-block:: python

    from ray.rllib.agents.ppo import PPOTrainer
    from ray.rllib.execution.rollout_ops import AsyncGradients
    from ray.rllib.execution.train_ops import ApplyGradients
    from ray.rllib.execution.metric_ops import StandardMetricsReporting

    def a3c_execution_plan(workers, config):
        # For A3C, compute policy gradients remotely on the rollout workers.
        grads = AsyncGradients(workers)

        # Apply the gradients as they arrive. We set update_all to False so that
        # only the worker sending the gradient is updated with new weights.
        train_op = grads.for_each(ApplyGradients(workers, update_all=False))

        return StandardMetricsReporting(train_op, workers, config)

    CustomTrainer = PPOTrainer.with_updates(
        execution_plan=a3c_execution_plan)


The ``with_updates`` method that we use here is also available for Torch and TF policies built from templates.

Now let's look at each PPO policy definition:

.. code-block:: python

    PPOTFPolicy = build_tf_policy(
        name="PPOTFPolicy",
        get_default_config=lambda: ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG,
        loss_fn=ppo_surrogate_loss,
        stats_fn=kl_and_loss_stats,
        extra_action_out_fn=vf_preds_and_logits_fetches,
        postprocess_fn=postprocess_ppo_gae,
        gradients_fn=clip_gradients,
        before_loss_init=setup_mixins,
        mixins=[LearningRateSchedule, KLCoeffMixin, ValueNetworkMixin])

``stats_fn``: The stats function returns a dictionary of Tensors that will be reported with the training results. This also includes the ``kl`` metric which is used by the trainer to adjust the KL penalty. Note that many of the values below reference ``policy.loss_obj``, which is assigned by ``loss_fn`` (not shown here since the PPO loss is quite complex). RLlib will always call ``stats_fn`` after ``loss_fn``, so you can rely on using values saved by ``loss_fn`` as part of your statistics:

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

Let's look at how to implement a different family of policies, by looking at the `SimpleQ policy definition <https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/simple_q_tf_policy.py>`__:

.. code-block:: python

    SimpleQPolicy = build_tf_policy(
        name="SimpleQPolicy",
        get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
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

Defining a policy in PyTorch is quite similar to that for TensorFlow (and the process of defining a trainer given a Torch policy is exactly the same). Here's a simple example of a trivial torch policy `(runnable file here) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_torch_policy.py>`__:

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

Now, building on the TF examples above, let's look at how the `A3C torch policy <https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a3c_torch_policy.py>`__ is defined:

.. code-block:: python

    A3CTorchPolicy = build_torch_policy(
        name="A3CTorchPolicy",
        get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
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

You can find the full policy definition in `a3c_torch_policy.py <https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a3c_torch_policy.py>`__.

In summary, the main differences between the PyTorch and TensorFlow policy builder functions is that the TF loss and stats functions are built symbolically when the policy is initialized, whereas for PyTorch (or TensorFlow Eager) these functions are called imperatively each time they are used.

Extending Existing Policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``with_updates`` method on Trainers and Policy objects built with ``make_*`` to create a copy of the object with some changes, for example:

.. code-block:: python

    from ray.rllib.agents.ppo import PPOTrainer
    from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy

    CustomPolicy = PPOTFPolicy.with_updates(
        name="MyCustomPPOTFPolicy",
        loss_fn=some_custom_loss_fn)

    CustomTrainer = PPOTrainer.with_updates(
        default_policy=CustomPolicy)

Policy Evaluation
-----------------

Given an environment and policy, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `RolloutWorker <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can use rollout workers standalone to produce batches of experiences. This can be done by calling ``worker.sample()`` on a worker instance, or ``worker.sample.remote()`` in parallel on worker instances created as Ray actors (see `WorkerSet <https://github.com/ray-project/ray/blob/master/rllib/evaluation/worker_set.py>`__).

Here is an example of creating a set of rollout workers and using them gather experiences in parallel. The trajectories are concatenated, the policy learns on the trajectory batch, and then we broadcast the policy weights to the workers for the next round of rollouts:

.. code-block:: python

    # Setup policy and rollout workers
    env = gym.make("CartPole-v0")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    workers = WorkerSet(
        policy_class=CustomPolicy,
        env_creator=lambda c: gym.make("CartPole-v0"),
        num_workers=10)

    while True:
        # Gather a batch of samples
        T1 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in workers.remote_workers()]))

        # Improve the policy using the T1 batch
        policy.learn_on_batch(T1)

        # Broadcast weights to the policy evaluation workers
        weights = ray.put({"default_policy": policy.get_weights()})
        for w in workers.remote_workers():
            w.set_weights.remote(weights)

Execution Plans
---------------

Execution plans let you easily express the execution of an RL algorithm as a sequence of steps that
occur either sequentially in the learner, or in parallel across many actors.
Under the hood, RLlib *translates* these plans into ``ray.get()`` and ``ray.wait()`` operations over Ray actors,
so you easily write high-performance algorithms without needing to manage individual low-level Ray actor calls.

Execution plans represent the **dataflow of the RL training job**. For example, the A2C algorithm can be thought
of a sequence of repeating steps, or *dataflow*, of:

 1. ``ParallelRollouts``: Generate experiences from many envs in parallel using rollout workers.
 2. ``ConcatBatches``: The experiences are concatenated into one batch for training.
 3. ``TrainOneStep``: Take a gradient step with respect to the policy loss, and update the worker weights.

In code, this dataflow can be expressed as the following execution plan, which is a simple function that can be passed to ``build_trainer`` to define a new algorithm. It takes in a ``WorkerSet`` and config, and returns an iterator over training results:

.. code-block:: python

    def execution_plan(workers: WorkerSet, config: TrainerConfigDict):
        # type: LocalIterator[SampleBatchType]
        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # type: LocalIterator[(SampleBatchType, List[LearnerStatsDict])]
        train_op = rollouts \
            .combine(ConcatBatches(
                min_batch_size=config["train_batch_size"])) \
            .for_each(TrainOneStep(workers))

        # type: LocalIterator[ResultDict]
        return StandardMetricsReporting(train_op, workers, config)


As you can see, each step returns an *iterator* over objects (if you're unfamiliar with distributed iterators, see Ray's `parallel iterators implementation <https://github.com/ray-project/ray/blob/master/python/ray/util/iter.py>`__). The reason it is a ``LocalIterator`` is that, though it is based on a parallel computation, the iterator has been turned into one that can be consumed locally in sequence by the program. A couple other points to note:

 - The reason the plan returns an iterator over training results, is that ``trainer.train()`` is pulling results from this iterator to return as the result of the train call.
 - The rollout workers have been already created ahead of time in the ``WorkerSet``, so the execution plan function is only defining a sequence of operations over the results of the rollouts.

These iterators represent the infinite stream of data items that can be produced from the dataflow. Each operator (e.g., ``ConcatBatches``, ``TrainOneStep``), executes an operation over each item and returns a transformed item (e.g., concatenated batches, learner stats from training). Finally, some operators such as TrainOneStep have the *side-effect* of updating the rollout worker weights (that's why ``TrainOneStep`` takes the list of worker actors ``workers`` as an argument).

Understanding and Debugging Execution Plans
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Execution plans are based on Ray `parallel iterators <https://github.com/ray-project/ray/blob/master/python/ray/util/iter.py>`__ and can be inspected similarly. For example, suppose you wanted to print out the intermediate data items during training. This can be done by inserting a print function into the dataflow, e.g., for A2C:

.. code-block:: python

    def debug_print(item):
        print("I saw", type(item))
        return item

    train_op = rollouts \
        .combine(ConcatBatches(
            min_batch_size=config["train_batch_size"])) \
        .for_each(debug_print) \
        .for_each(TrainOneStep(workers))

You'll see output like this on the console:

.. code-block:: bash

    (pid=6555) I saw <class 'ray.rllib.policy.sample_batch.SampleBatch'>
    (pid=6555) I saw <class 'ray.rllib.policy.sample_batch.SampleBatch'>
    (pid=6555) I saw <class 'ray.rllib.policy.sample_batch.SampleBatch'>
    (pid=6555) I saw <class 'ray.rllib.policy.sample_batch.SampleBatch'>

It is important to understand that the iterators of an execution plan are evaluated *lazily*. This means that no computation happens until the `trainer <#trainers>`__ tries to read the next item from the iterator (i.e., get the next training result for a ``Trainer.train()`` call).

Execution Plan Concepts
~~~~~~~~~~~~~~~~~~~~~~~

RLlib provides a library of operators `(GitHub link) <https://github.com/ray-project/ray/tree/master/rllib/execution>`__ that can be used in execution plans. You can of course write your own operators (which are just normal Python functions). As a reminder, operators are simply functions (or stateful function objects) that can be chained on the iterator (e.g., the ``debug_print`` operator above). A few categories of operators are summarized below:

**Rollout ops** (`rollout_ops.py <https://github.com/ray-project/ray/blob/master/rllib/execution/rollout_ops.py>`__): These are functions for generating and working with experiences, including ``ParallelRollouts`` (for generating experiences synchronously or asynchronously), ``ConcatBatches`` (for combining batches together), ``SelectExperiences`` (for selecting relevant experiences in a multi-agent setting), and ``AsyncGradients`` (for computing gradients over new experiences on the fly, asynchronously, as in A3C).

**Train ops** (`train_ops.py <https://github.com/ray-project/ray/blob/master/rllib/execution/train_ops.py>`__): These are functions that improve the policy and update workers. The most basic operator, ``TrainOneStep``, take in as input a batch of experiences and emit metrics as output. Important operators here include ``TrainOneStep``, ``TrainTFMultiGPU`` (for multi-GPU optimization), ``ComputeGradients`` (to compute gradients without updating the policy), and ``ApplyGradients`` (to apply computed gradients to a policy).

**Replay ops** (`replay_ops.py <https://github.com/ray-project/ray/blob/master/rllib/execution/replay_ops.py>`__): The main operator provided here is ``StoreToReplayBuffer``, which can save experiences batches to either a local replay buffer or a set of distributed replay actors. It has a counterpart, ``Replay``, that produces a new stream of experiences replayed from one of the aforementioned replay buffers. Algorithms that use ``StoreToReplayBuffer`` and ``Replay`` are necessarily composed of *multiple sub-dataflows* (different iterators), that are combined with *concurrency ops*.

**Concurrency ops** (`concurrency_ops.py <https://github.com/ray-project/ray/blob/master/rllib/execution/concurrency_ops.py>`__): The main operator provided here is ``Concurrently``, which composes multiple iterators (dataflows) into a single dataflow by executing them in an interleaved fashion. The output can be defined to be the mixture of the two dataflows, or filtered to that of one of the sub-dataflows. It has two modes:

 - ``round_robin``: Alternate taking items from each input dataflow. This ensures a fixed ratio of computations between, e.g., experience generation and experience replay. The ratio can be adjusted by setting ``round_robin_weights``.
 - ``async``: Execute each input dataflow as fast as possible without blocking. You might want to use this when, e.g., you want replay to proceed as fast as possible irregardless of how fast experiences are being generated.

**Metric ops** (`metric_ops.py <https://github.com/ray-project/ray/blob/master/rllib/execution/metric_ops.py>`__): Finally, we provide a ``StandardMetricsReporting`` operator that collects training metrics from the rollout workers in a unified fashion, and returns a stream of training result dicts. Execution plans should always end with this operator. This metrics op also reports various internal performance metrics stored by other operators in the shared metrics context accessible via ``_get_shared_metrics()``.

Example: Asynchrony
~~~~~~~~~~~~~~~~~~~~

Suppose we wanted to make the above A2C example asynchronous (i.e., A3C). We would switch the synchronous ``ParallelRollouts`` operator with ``AsyncGradients``, and use ``ApplyGradients`` to apply gradient updates as fast as they are collected. The ``AsyncGradients`` operator is going to execute rollouts in parallel, compute the policy gradient over the new batches (of size ``rollout_fragment_length``) on the remote workers, and then return a stream of the computed gradients:

.. code-block:: python

    def execution_plan(workers: WorkerSet, config: TrainerConfigDict):
        # type: LocalIterator[(ModelGradients, int)]
        grads = AsyncGradients(workers)

        # type: LocalIterator[_]
        train_op = grads.for_each(ApplyGradients(workers, update_all=False))

        # type: LocalIterator[ResultDict]
        return StandardMetricsReporting(train_op, workers, config)

See also the `actual A3C implementation <https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a3c.py>`__.

Example: Replay
~~~~~~~~~~~~~~~

Let's try adding a replay buffer to A2C. This can be done as follows by inserting store / replay ops and using ``Concurrently`` to compose them together:

.. code-block:: python

    def execution_plan(workers: WorkerSet, config: TrainerConfigDict):
        # Construct a replay buffer.
        replay_buffer = LocalReplayBuffer(...)

        # type: LocalIterator[_]
        store_op = ParallelRollouts(workers, mode="bulk_sync") \
            .for_each(StoreToReplayBuffer(local_buffer=replay_buffer))

        # type: LocalIterator[(SampleBatchType, List[LearnerStatsDict])]
        replay_op = Replay(local_buffer=replay_buffer) \
            .for_each(TrainOneStep(workers))

        # type: LocalIterator[(SampleBatchType, List[LearnerStatsDict])]
        train_op = Concurrently(
            [store_op, replay_op], mode="round_robin", output_indexes=[1])

        # type: LocalIterator[ResultDict]
        return StandardMetricsReporting(train_op, workers, config)


Note that here we set ``output_indexes=[1]`` for the ``Concurrently`` operator, which makes it only return results from the replay op. See also the `DQN implementation of replay <https://github.com/ray-project/ray/blob/master/rllib/agents/dqn/dqn.py>`__ for a complete example including the implementation of options such as *training intensity*.


Example: Multi-agent
~~~~~~~~~~~~~~~~~~~~

One of the primary motivations behind execution plans, beyond their conciseness, is to enable complex multi-agent training workflows to be easily composed. For example, suppose one wanted to, in a multi-agent environment, concurrently train one set of agents with ``DQN``, and another set with ``PPO``. This requires stitching together two entirely different distributed dataflows. Fortunately, as we've seen earlier, this is quite simple with the ``Concurrently`` operator.

Check out the `PPO + DQN multi-agent workflow example <https://github.com/ray-project/ray/blob/master/rllib/examples/two_trainer_workflow.py>`__ for more details. One line to pay particular attention to in this example is the use of ``LocalIterator.duplicate()`` to clone the iterator of experiences into two separate iterators, which are filtered via ``SelectExperiences`` and then consumed by PPO and DQN sub-dataflows respectively.

Trainers
--------

Trainers are the boilerplate classes that put the above components together, making algorithms accessible via Python API and the command line. They manage algorithm configuration, setup of the rollout workers and optimizer, and collection of training metrics. Trainers also implement the :ref:`Tune Trainable API <tune-60-seconds>` for easy experiment management.

Example of three equivalent ways of interacting with the PPO trainer, all of which log results in ``~/ray_results``:

.. code-block:: python

    trainer = PPOTrainer(env="CartPole-v0", config={"train_batch_size": 4000})
    while True:
        print(trainer.train())

.. code-block:: bash

    rllib train --run=PPO --env=CartPole-v0 --config='{"train_batch_size": 4000}'

.. code-block:: python

    from ray import tune
    tune.run(PPOTrainer, config={"env": "CartPole-v0", "train_batch_size": 4000})
