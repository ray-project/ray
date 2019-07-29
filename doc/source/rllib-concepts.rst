RLlib Concepts and Custom Algorithms
====================================

This page describes the internal concepts used to implement algorithms in RLlib. You might find this useful if modifying or adding new algorithms to RLlib.

Policies
--------

Policy classes encapsulate the core numerical components of RL algorithms. This typically includes the policy model that determines actions to take, a trajectory postprocessor for experiences, and a loss function to improve the policy given postprocessed experiences. For a simple example, see the policy gradients `policy definition <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/pg/pg_policy.py>`__.

Most interaction with deep learning frameworks is isolated to the `Policy interface <https://github.com/ray-project/ray/blob/master/python/ray/rllib/policy/policy.py>`__, allowing RLlib to support multiple frameworks. To simplify the definition of policies, RLlib includes `Tensorflow <#building-policies-in-tensorflow>`__ and `PyTorch-specific <#building-policies-in-pytorch>`__ templates. You can also write your own from scratch. Here is an example:

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


Building Policies in TensorFlow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section covers how to build a TensorFlow RLlib policy using ``tf_policy_template.build_tf_policy()``.

To start, you first have to define a loss function. In RLlib, loss functions are defined over batches of trajectory data produced by policy evaluation. A basic policy gradient loss that only tries to maximize the 1-step reward can be defined as follows:

.. code-block:: python

    import tensorflow as tf
    from ray.rllib.policy.sample_batch import SampleBatch

    def policy_gradient_loss(policy, batch_tensors):
        actions = batch_tensors[SampleBatch.ACTIONS]
        rewards = batch_tensors[SampleBatch.REWARDS]
        return -tf.reduce_mean(policy.action_dist.logp(actions) * rewards)

In the above snippet, ``actions`` is a Tensor placeholder of shape ``[batch_size, action_dim...]``, and ``rewards`` is a placeholder of shape ``[batch_size]``. The ``policy.action_dist`` object is an `ActionDistribution <rllib-package-ref.html#ray.rllib.models.ActionDistribution>`__ that represents the output of the neural network policy model. Passing this loss function to ``build_tf_policy`` is enough to produce a very basic TF policy:

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


If you run the above snippet `(runnable file here) <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/custom_tf_policy.py>`__, you'll probably notice that CartPole doesn't learn so well:

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

    def policy_gradient_loss(policy, batch_tensors):
        actions = batch_tensors[SampleBatch.ACTIONS]
        advantages = batch_tensors[Postprocessing.ADVANTAGES]
        return -tf.reduce_mean(policy.action_dist.logp(actions) * advantages)

    MyTFPolicy = build_tf_policy(
        name="MyTFPolicy",
        loss_fn=policy_gradient_loss,
        postprocess_fn=postprocess_advantages)

The ``postprocess_advantages()`` function above uses calls RLlib's ``compute_advantages`` function to compute advantages for each timestep. If you re-run the trainer with this improved policy, you'll find that it quickly achieves the max reward of 200.

You might be wondering how RLlib makes the advantages placeholder automatically available as ``batch_tensors[Postprocessing.ADVANTAGES]``. When building your policy, RLlib will create a "dummy" trajectory batch where all observations, actions, rewards, etc. are zeros. It then calls your ``postprocess_fn``, and generates TF placeholders based on the numpy shapes of the postprocessed batch. RLlib tracks which placeholders that ``loss_fn`` and ``stats_fn`` access, and then feeds the corresponding sample data into those placeholders during loss optimization. You can also access these placeholders via ``policy.get_placeholder(<name>)`` after loss initialization.

**Example 1: Proximal Policy Optimization**

In the above section you saw how to compose a simple policy gradient algorithm with RLlib. In this example, we'll dive into how PPO was built with RLlib and how you can modify it. First, check out the `PPO trainer definition <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/ppo/ppo.py>`__:

.. code-block:: python

    PPOTrainer = build_trainer(
        name="PPOTrainer",
        default_config=DEFAULT_CONFIG,
        default_policy=PPOTFPolicy,
        make_policy_optimizer=choose_policy_optimizer,
        validate_config=validate_config,
        after_optimizer_step=update_kl,
        before_train_step=warn_about_obs_filter,
        after_train_result=warn_about_bad_reward_scales)

Besides some boilerplate for defining the PPO configuration and some warnings, there are two important arguments to take note of here: ``make_policy_optimizer=choose_policy_optimizer``, and ``after_optimizer_step=update_kl``.

The ``choose_policy_optimizer`` function chooses which `Policy Optimizer <#policy-optimization>`__ to use for distributed training. You can think of these policy optimizers as coordinating the distributed workflow needed to improve the policy. Depending on the trainer config, PPO can switch between a simple synchronous optimizer, or a multi-GPU optimizer that implements minibatch SGD (the default):

.. code-block:: python

    def choose_policy_optimizer(workers, config):
        if config["simple_optimizer"]:
            return SyncSamplesOptimizer(
                workers,
                num_sgd_iter=config["num_sgd_iter"],
                train_batch_size=config["train_batch_size"])

        return LocalMultiGPUOptimizer(
            workers,
            sgd_batch_size=config["sgd_minibatch_size"],
            num_sgd_iter=config["num_sgd_iter"],
            num_gpus=config["num_gpus"],
            sample_batch_size=config["sample_batch_size"],
            num_envs_per_worker=config["num_envs_per_worker"],
            train_batch_size=config["train_batch_size"],
            standardize_fields=["advantages"],
            straggler_mitigation=config["straggler_mitigation"])

Suppose we want to customize PPO to use an asynchronous-gradient optimization strategy similar to A3C. To do that, we could define a new function that returns ``AsyncGradientsOptimizer`` and override the ``make_policy_optimizer`` component of ``PPOTrainer``.

.. code-block:: python

    from ray.rllib.agents.ppo import PPOTrainer
    from ray.rllib.optimizers import AsyncGradientsOptimizer

    def make_async_optimizer(workers, config):
        return AsyncGradientsOptimizer(workers, grads_per_step=100)

    CustomTrainer = PPOTrainer.with_updates(
        make_policy_optimizer=make_async_optimizer)


The ``with_updates`` method that we use here is also available for Torch and TF policies built from templates.
 
Now let's take a look at the ``update_kl`` function. This is used to adaptively adjust the KL penalty coefficient on the PPO loss, which bounds the policy change per training step. You'll notice the code handles both single and multi-agent cases (where there are be multiple policies each with different KL coeffs):

.. code-block:: python

    def update_kl(trainer, fetches):
        if "kl" in fetches:
            # single-agent
            trainer.workers.local_worker().for_policy(
                lambda pi: pi.update_kl(fetches["kl"]))
        else:

            def update(pi, pi_id):
                if pi_id in fetches:
                    pi.update_kl(fetches[pi_id]["kl"])
                else:
                    logger.debug("No data for {}, not updating kl".format(pi_id))

            # multi-agent
            trainer.workers.local_worker().foreach_trainable_policy(update)

The ``update_kl`` method on the policy is defined in `PPOTFPolicy <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/ppo/ppo_policy.py>`__ via the ``KLCoeffMixin``, along with several other advanced features. Let's look at each new feature used by the policy:

.. code-block:: python

    PPOTFPolicy = build_tf_policy(
        name="PPOTFPolicy",
        get_default_config=lambda: ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG,
        loss_fn=ppo_surrogate_loss,
        stats_fn=kl_and_loss_stats,
        extra_action_fetches_fn=vf_preds_and_logits_fetches,
        postprocess_fn=postprocess_ppo_gae,
        gradients_fn=clip_gradients,
        before_loss_init=setup_mixins,
        mixins=[LearningRateSchedule, KLCoeffMixin, ValueNetworkMixin])

``stats_fn``: The stats function returns a dictionary of Tensors that will be reported with the training results. This also includes the ``kl`` metric which is used by the trainer to adjust the KL penalty. Note that many of the values below reference ``policy.loss_obj``, which is assigned by ``loss_fn`` (not shown here since the PPO loss is quite complex). RLlib will always call ``stats_fn`` after ``loss_fn``, so you can rely on using values saved by ``loss_fn`` as part of your statistics:

.. code-block:: python

    def kl_and_loss_stats(policy, batch_tensors):
        policy.explained_variance = explained_variance(
            batch_tensors[Postprocessing.VALUE_TARGETS], policy.value_function)

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

``extra_actions_fetches_fn``: This function defines extra outputs that will be recorded when generating actions with the policy. For example, this enables saving the raw policy logits in the experience batch, which e.g. means it can be referenced in the PPO loss function via ``batch_tensors[BEHAVIOUR_LOGITS]``. Other values such as the current value prediction can also be emitted for debugging or optimization purposes:

.. code-block:: python

    def vf_preds_and_logits_fetches(policy):
        return {
            SampleBatch.VF_PREDS: policy.value_function,
            BEHAVIOUR_LOGITS: policy.model.outputs,
        }

``gradients_fn``: If defined, this function returns TF gradients for the loss function. You'd typically only want to override this to apply transformations such as gradient clipping:

.. code-block:: python

    def clip_gradients(policy, optimizer, loss):
        if policy.config["grad_clip"] is not None:
            policy.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                                tf.get_variable_scope().name)
            grads = tf.gradients(loss, policy.var_list)
            policy.grads, _ = tf.clip_by_global_norm(grads,
                                                     policy.config["grad_clip"])
            clipped_grads = list(zip(policy.grads, policy.var_list))
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

Let's look at how to implement a different family of policies, by looking at the `SimpleQ policy definition <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/dqn/simple_q_policy.py>`__:

.. code-block:: python

    SimpleQPolicy = build_tf_policy(
        name="SimpleQPolicy",
        get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
        make_model=build_q_models,
        action_sampler_fn=build_action_sampler,
        loss_fn=build_q_losses,
        extra_action_feed_fn=exploration_setting_inputs,
        extra_action_fetches_fn=lambda policy: {"q_values": policy.q_values},
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
        return action, action_prob

The remainder of DQN is similar to other algorithms. Target updates are handled by a ``after_optimizer_step`` callback that periodically copies the weights of the Q network to the target.

Finally, note that you do not have to use ``build_tf_policy`` to define a TensorFlow policy. You can alternatively subclass ``Policy``, ``TFPolicy``, or ``DynamicTFPolicy`` as convenient.

Building Policies in TensorFlow Eager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While RLlib runs all TF operations in graph mode, you can still leverage TensorFlow eager using `tf.py_function <https://www.tensorflow.org/api_docs/python/tf/py_function>`__. However, note that eager and non-eager tensors cannot be mixed within the ``py_function``. Here's an example of embedding eager execution within a policy loss function:

.. code-block:: python

    def eager_loss(policy, batch_tensors):
        """Example of using embedded eager execution in a custom loss.

        Here `compute_penalty` prints the actions and rewards for debugging, and
        also computes a (dummy) penalty term to add to the loss.
        """

        def compute_penalty(actions, rewards):
            penalty = tf.reduce_mean(tf.cast(actions, tf.float32))
            if random.random() > 0.9:
                print("The eagerly computed penalty is", penalty, actions, rewards)
            return penalty

        actions = batch_tensors[SampleBatch.ACTIONS]
        rewards = batch_tensors[SampleBatch.REWARDS]
        penalty = tf.py_function(
            compute_penalty, [actions, rewards], Tout=tf.float32)

        return penalty - tf.reduce_mean(policy.action_dist.logp(actions) * rewards)

You can find a runnable file for the above eager execution example `here <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/eager_execution.py>`__.

Building Policies in PyTorch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Defining a policy in PyTorch is quite similar to that for TensorFlow (and the process of defining a trainer given a Torch policy is exactly the same). Here's a simple example of a trivial torch policy `(runnable file here) <https://github.com/ray-project/ray/blob/master/python/ray/rllib/examples/custom_torch_policy.py>`__:

.. code-block:: python

    from ray.rllib.policy.sample_batch import SampleBatch
    from ray.rllib.policy.torch_policy_template import build_torch_policy

    def policy_gradient_loss(policy, batch_tensors):
        logits, _, values, _ = policy.model({
            SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
        }, [])
        action_dist = policy.dist_class(logits)
        log_probs = action_dist.logp(batch_tensors[SampleBatch.ACTIONS])
        return -batch_tensors[SampleBatch.REWARDS].dot(log_probs)

    # <class 'ray.rllib.policy.torch_policy_template.MyTorchPolicy'>
    MyTorchPolicy = build_torch_policy(
        name="MyTorchPolicy",
        loss_fn=policy_gradient_loss)

Now, building on the TF examples above, let's look at how the `A3C torch policy <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/a3c/a3c_torch_policy.py>`__ is defined:

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

``loss_fn``: Similar to the TF example, the actor critic loss is defined over ``batch_tensors``. We imperatively execute the forward pass by calling ``policy.model()`` on the observations followed by ``policy.dist_class()`` on the output logits. The output Tensors are saved as attributes of the policy object (e.g., ``policy.entropy = dist.entropy.mean()``), and we return the scalar loss:

.. code-block:: python

    def actor_critic_loss(policy, batch_tensors):
        logits, _, values, _ = policy.model({
            SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
        }, [])
        dist = policy.dist_class(logits)
        log_probs = dist.logp(batch_tensors[SampleBatch.ACTIONS])
        policy.entropy = dist.entropy().mean()
        ...
        return overall_err

``stats_fn``: The stats function references ``entropy``, ``pi_err``, and ``value_err`` saved from the call to the loss function, similar in the PPO TF example:

.. code-block:: python

    def loss_and_entropy_stats(policy, batch_tensors):
        return {
            "policy_entropy": policy.entropy.item(),
            "policy_loss": policy.pi_err.item(),
            "vf_loss": policy.value_err.item(),
        }

``extra_action_out_fn``: We save value function predictions given model outputs. This makes the value function predictions of the model available in the trajectory as ``batch_tensors[SampleBatch.VF_PREDS]``:

.. code-block:: python

    def model_value_predictions(policy, input_dict, state_batches, model_out):
        return {SampleBatch.VF_PREDS: model_out[2].cpu().numpy()}

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

You can find the full policy definition in `a3c_torch_policy.py <https://github.com/ray-project/ray/blob/master/python/ray/rllib/agents/a3c/a3c_torch_policy.py>`__.

In summary, the main differences between the PyTorch and TensorFlow policy builder functions is that the TF loss and stats functions are built symbolically when the policy is initialized, whereas for PyTorch these functions are called imperatively each time they are used.

Extending Existing Policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``with_updates`` method on Trainers and Policy objects built with ``make_*`` to create a copy of the object with some changes, for example:

.. code-block:: python

    from ray.rllib.agents.ppo import PPOTrainer
    from ray.rllib.agents.ppo.ppo_policy import PPOTFPolicy

    CustomPolicy = PPOTFPolicy.with_updates(
        name="MyCustomPPOTFPolicy",
        loss_fn=some_custom_loss_fn)

    CustomTrainer = PPOTrainer.with_updates(
        default_policy=CustomPolicy)

Policy Evaluation
-----------------

Given an environment and policy, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/python/ray/rllib/policy/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `RolloutWorker <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/rollout_worker.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can use rollout workers standalone to produce batches of experiences. This can be done by calling ``worker.sample()`` on a worker instance, or ``worker.sample.remote()`` in parallel on worker instances created as Ray actors (see `WorkerSet <https://github.com/ray-project/ray/blob/master/python/ray/rllib/evaluation/worker_set.py>`__).

Here is an example of creating a set of rollout workers and using them gather experiences in parallel. The trajectories are concatenated, the policy learns on the trajectory batch, and then we broadcast the policy weights to the workers for the next round of rollouts:

.. code-block:: python

    # Setup policy and rollout workers
    env = gym.make("CartPole-v0")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    workers = WorkerSet(
        policy=CustomPolicy,
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

Policy Optimization
-------------------

Similar to how a `gradient-descent optimizer <https://www.tensorflow.org/api_docs/python/tf/train/GradientDescentOptimizer>`__ can be used to improve a model, RLlib's `policy optimizers <https://github.com/ray-project/ray/tree/master/python/ray/rllib/optimizers>`__ implement different strategies for improving a policy.

For example, in A3C you'd want to compute gradients asynchronously on different workers, and apply them to a central policy replica. This strategy is implemented by the `AsyncGradientsOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/async_gradients_optimizer.py>`__. Another alternative is to gather experiences synchronously in parallel and optimize the model centrally, as in `SyncSamplesOptimizer <https://github.com/ray-project/ray/blob/master/python/ray/rllib/optimizers/sync_samples_optimizer.py>`__. Policy optimizers abstract these strategies away into reusable modules.

This is how the example in the previous section looks when written using a policy optimizer:

.. code-block:: python

    # Same setup as before
    workers = WorkerSet(
        policy=CustomPolicy,
        env_creator=lambda c: gym.make("CartPole-v0"),
        num_workers=10)
    
    # this optimizer implements the IMPALA architecture
    optimizer = AsyncSamplesOptimizer(workers, train_batch_size=500)

    while True:
        optimizer.step()


Trainers
--------

Trainers are the boilerplate classes that put the above components together, making algorithms accessible via Python API and the command line. They manage algorithm configuration, setup of the rollout workers and optimizer, and collection of training metrics. Trainers also implement the `Trainable API <https://ray.readthedocs.io/en/latest/tune-usage.html#training-api>`__ for easy experiment management.

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
