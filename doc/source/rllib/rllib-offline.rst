.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

Working With Offline Data
=========================

Getting started
---------------

RLlib's offline RL API enables you to work with experiences read from offline storage (e.g., disk, cloud storage, 
streaming systems, HDFS). For example, you might want to read experiences saved from previous training runs, collected 
from experts, or gathered from policies deployed in `web applications <https://arxiv.org/abs/1811.00260>`__. You can 
also log new agent experiences produced during online training for future use.

RLlib represents trajectory sequences (i.e., ``(s, a, r, s', ...)`` tuples) with `SingleAgentEpisode 
<single-agent-episode.rst>`__ objects (multi-agent offline training is currently not supported). Using this episode format 
enables efficient encoding and compression of experiences, rewriting trajectories, and user-friendly data access via getters. 
During online training, RLlib uses `SingleAgentEnvRunner <rllib-concepts.html#policy-evaluation>`__ actors to generate episodes 
of experiences in parallel using the current policy. RLlib can also uses this same episode format for reading and writing 
experiences to offline storage. For offline writing RLlib uses the `OfflineSingleAgentEnvRunner 
<https://github.com/ray-project/ray/blob/master/rllib/offline/offline_single_agent_env_runner.py>`__ that samples episodes 
and writes them to offline storage. 

You can store experiences either directly in RLlib's episode format or in table (columns) 
format. You should use the episode format when 

a. You need experiences grouped by their trajectory and ordered in time (e.g. to train stateful modules).
b. You want to use recorded experiences exclusively within RLlib.

Contrary, you should prefer the table (columns) format, if

a. You need to read the data easily with other data tools or ML libraries.
b. You do not need full (usually complete) trajectories in training.

.. note:: RLlib's redesigned API stack incorporates principles that support standalone applications. Consequently, the 
    `SingleAgentEpisode` class is available for use independently of RLlib. To enable faster access through external data tools 
    (e.g., for data transformations), it is recommended to use the table record format in these scenarios.

Most importantly, RLlib's offline RL API builds on top of :ref:`Ray Data <data>` and therefore features in general all read and 
write methods supported by Ray Data (e.g. ``read_parquet``, ``read_json``, etc.) with ``read/write_parquet`` being the default 
read and write methods. A core design principle of the API is to apply as many data transformations as possible on-the-fly to
the learner, allowing the learner to focus exclusively on model updates. 

.. note:: During the transition phase from old to new API stack you can use the new offline RL API also with your 
    `SampleBatch <https://github.com/ray-project/ray/blob/master/rllib/offline/offline_single_agent_env_runner.py>`__ data recorded 
    with the old API stack. To enable this feature set ``input_read_sample_batches=True``.

Example: Training an expert policy.
--------------------------------------------
In this example you train an PPO agent to play ``CartPole-v1`` until it collects an episode mean reward of ``450.0``. You checkpoint 
this agent and later use its policy to record expert data to local disk.

.. testsetup:: 
    
    # Define a shared variable to store the path to the
    # best checkpoint.
    best_checkpoint = None

    # Define a shared variable to store the path to the
    # recorded data.
    data_path = None

    # Define another shared variable to store the path to
    # the tabular recording data.
    tabular_data_path = None

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
    from ray.rllib.utils.metrics import (
        ENV_RUNNER_RESULTS,
        EVALUATION_RESULTS,
        EPISODE_RETURN_MEAN,
    )
    from ray import train, tune

    # Configure the PPO algorithm.
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        # Enable the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .training(
            lr=0.0003,
            # Run 6 SGD minibatch iterations on a batch.
            num_epochs=6,
            # Weigh the value function loss smaller than 
            # the policy loss.
            vf_loss_coeff=0.01,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                # Share encoder layers between value network
                # and policy.
                vf_share_layers=True,
            ),
        )
    )

    # Define the metric to use for stopping.
    metric = f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"

    # Define the Tuner.
    tuner = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=train.RunConfig(
            stop={
                metric: 450.0,
            },
            name="docs_rllib_offline_pretrain_ppo",
            verbose=2,
            checkpoint_config=train.CheckpointConfig(
                checkpoint_frequency=1,
                checkpoint_at_end=True,
            ),
        ),
    )
    analysis = tuner.fit()

    best_checkpoint = (
        analysis
        .get_best_result(
            metric=metric,
            mode="max"
        )
        .checkpoint.path
    )

Example: Record expert data to local disk
-----------------------------------------
After you trained an expert policy to play ``CartPole-v1`` you load its policy here to record expert data during evaluation. You will use 
5 ``OfflineSingleAgentEnvRunner`` s to collect in each rollout 50 complete episodes. In this example you store experiences directly in RLlib's 
``SingleAgentEpisode`` objects with no more than 25 episode objects per Parquet file. Altogether you run 10 evaluation runs, which should
result in 500 recorded episodes from the expert policy. You will use these data in the next example to train via Offline RL a new policy 
that should reach a return of 450.0 when playing ``CartPole-v1``.

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core import (
        COMPONENT_LEARNER_GROUP,
        COMPONENT_LEARNER,
        COMPONENT_RL_MODULE,
        DEFAULT_MODULE_ID,
    )
    from ray.rllib.core.rl_module import RLModuleSpec
    
    # Store recording data under the following path.
    data_path = "/tmp/docs_rllib_offline_recording"

    # Configure the algorithm for recording.
    config = (
        PPOConfig()
        # The environment needs to be specified.
        .environment(
            env="CartPole-v1",
        )
        # Make sure you use the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        # Make sure to sample complete episodes because
        # you want to record RLlib's episode objects.
        .env_runners(
            batch_mode="complete_episodes",
        )
        # Set up 5 evaluation `EnvRunners` for recording.
        # Sample 50 episodes in each evaluation rollout.
        .evaluation(
            evaluation_num_env_runners=5,
            evaluation_duration=50,
        )
        # Use the checkpointed expert policy from PPO training
        # above. Note, we have to use the same `model_config` as
        # the one with which the expert policy was trained, otherwise
        # the module state cannot be loaded.
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                # Share encoder layers between value network
                # and policy.
                vf_share_layers=True,
            ),
        )
        # Define the output path and format. In this example you
        # want to store data directly in RLlib's episode objects.
        # Each Parquet file should hold no more than 25 episodes. 
        .offline_data(
            output=data_path,
            output_write_episodes=True,
            output_max_rows_per_file=25,
        )
    )

    # Build the algorithm.
    algo = config.build()
    # Load now the PPO-trained `RLModule` to use in recording.
    algo.restore_from_path(
        best_checkpoint,
        # Load only the `RLModule` component here.
        component=COMPONENT_RL_MODULE,
    )

    # Run 10 evaluation iterations and record the data.
    for i in range(10):
        print(f"Iteration {i + 1}")
        res_eval = algo.evaluate()
        print(res_eval)

    # Stop the algorithm. Note, this is important for when
    # defining `output_max_rows_per_file`. Otherwise,
    # remaining episodes in the `EnvRunner`s buffer will not
    # be written to disk.
    algo.stop()

.. note:: The stored episode data is formatted as ``binary``. Each episode is converted into its dictionary representation and serialized using ``msgpack-numpy``, 
    ensuring version compatibility.

RLlib's  recording process is efficient because it utilizes multiple ``OfflineSingleAgentEnvRunner`` instances during evaluation, enabling parallel 
data writing. You can explore the folder to review the stored Parquet data:

.. code-block:: text

    $ ls -la /tmp/docs_rllib_offline_recording/cartpole-v1

    drwxr-xr-x.  2 user user 540 18. Nov 18:05 run-000001-00006
    drwxr-xr-x.  2 user user 540 18. Nov 18:05 run-000001-00011
    drwxr-xr-x.  2 user user 540 18. Nov 18:05 run-000001-00016
    drwxr-xr-x.  2 user user 540 18. Nov 18:05 run-000001-00021

.. hint:: RLlib stores records under a folder named by the RL environment. Therein, you see for each ``OfflineSingleAgentEnvRunner`` and write operation 
    one folder of Parquet files. The write operation count is given in the second numbering. 

Example: Training on previously saved experiences
-------------------------------------------------
In this example you are using behavior cloning with the previously recorded Parquet data from your expert policy playing ``CartPole-v1``. The 
data needs to be linked in the configuration of the algorithm (via the ``input_`` attribute).

.. testcode:: python
    
    from ray import train, tune
    from ray.rllib.algorithms.bc import BCConfig

    # Setup the config for behavior cloning.
    config = (
        BCConfig()
        .environment(
            # Use the `CartPole-v1` environment from which the
            # data was recorded. This is merely for receiving 
            # action and observation spaces and to use it during
            # evaluation.
            env="CartPole-v1",
        )
        .api_stack(
            # Enable the new API stack.
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .learners(
            # Use a single learner.
            num_learners=0,
        )
        .training(
            # This has to be defined in the new offline RL API.
            train_batch_size_per_learner=1024,
        )
        .offline_data(
            # Link the data.
            input_=[data_path],
            # You want to read in RLlib's episode format b/c this
            # is how you recorded data.
            input_read_episodes=True,
            # Read smaller batches from the data than the learner
            # trains on. Note, each batch element is an episode
            # with multiple timesteps.
            input_read_batch_size=512,
            # Create exactly 2 `DataWorkers` that transform 
            # the data on-the-fly. Give each of them a single
            # CPU.
            map_batches_kwargs={
                "concurrency": 2,
                "num_cpus": 1,
            },
            # When iterating over the data, prefetch two batches
            # to improve the data pipeline. Do not shuffle the 
            # buffer (the data is too small).
            iter_batches_kwargs={
                "prefetch_batches": 2,
                "local_shuffle_buffer_size": None,
            },
            # You must set this for single-learner setups.
            dataset_num_iters_per_learner=1,
        )
        .evaluation(
            # Run evaluation to see how well the learned policy
            # performs. Run every 3rd training iteration an evaluation.
            evaluation_interval=3,
            # Use a single `EnvRunner` for evaluation.
            evaluation_num_env_runners=1,
            # In each evaluation rollout, collect 5 episodes of data.
            evaluation_duration=5,
            # Evaluate the policy parallel to training.
            evaluation_parallel_to_training=True,
        )
    )

    # Set the stopping metric to be the evaluation episode return mean.
    metric = f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"

    # Configure the Ray Tune.
    tuner = tune.Tuner(
        "BC",
        param_space=config,
        run_config=train.RunConfig(
            name="docs_rllib_offline_bc",
            # Stop behavior cloning when we reach 450 in return.
            stop={metric: 450.0},
            checkpoint_config=train.CheckpointConfig(
                # Only checkpoint at the end to be faster.
                checkpoint_frequency=0,
                checkpoint_at_end=True,
            ),
            verbose=2,
        )
    )
    # Run the experiment.
    analysis = tuner.fit()

Behavior cloning in RLlib is highly performant, completing a single training iteration in approximately 2 milliseconds. The experiment's 
results should resemble the following:

.. image:: images/offline/docs_rllib_offline_bc_episode_return_mean.svg
    :alt: Episode mean return over the course of BC training.
    :width: 750
    :align: left

It should take you around 98 seconds (456 iterations) to achieve the same episode return mean as the PPO agent. While this may not seem 
impressive compared to the PPO training time, it is important to note that ``CartPole-v1`` is a very simple environment to learn. In more 
complex environments, which require more sophisticated agents and significantly longer training times, pretraining via behavior cloning 
can be highly beneficial. Combining behavior cloning with subsequent fine-tuning using a reinforcement learning algorithm can substantially 
reduce training time, resource consumption, and associated costs.

Using external expert experiences
---------------------------------
Your expert data is often already available, either recorded from an operational system or directly provided by human experts. Typically, 
this data is stored in a tabular (columnar) format. RLlib's new Offline RL API simplifies the use of such data by allowing direct ingestion 
via a specified schema that organizes the expert data. The API's default schema for reading data is provided in `offline_prelearner.SCHEMA 
<http://github.com/ray-project/ray/blob/master/rllib/offline/offline_prelearner.py#33>`__.

Lets consider a simple example in which your expert data is stored with the schema: ``(o_t, a_t, r_t, o_tp1, d_t, i_t, logprobs_t)``. In this case 
you provide this schema as follows:

.. code-block:: python

    from ray.rllib.algorithms.bc import BCConfig
    from ray.rllib.core.columns import Columns

    config = (
        BCConfig()
        ...
        .offline_data(
            input_=[<input_path>],
            # Provide the schema of your data.
            input_read_schema={
                Columns.OBS: "o_t",
                Columns.ACTIONS: "a_t",
                Columns.REWARDS: "r_t",
                Columns.NEXT_OBS: "o_tp1",
                Columns.INFOS: "i_t",
                "done": "d_t",
            },
        )
    )
        
.. note:: Internally, the legacy ``gym``'s ``done`` signals are mapped to ``gymnasium``'s ``terminated`` signals, with ``truncated`` values defaulting to 
    ``False``. RLlib's ``SingleAgentEpisode`` structures align with ``gymnasium``, adhering to the updated environment API standards in reinforcement learning.

Converting tabular data to RLlib's episode format
-------------------------------------------------
While the tabular format is widely compatible and seamlessly integrates with RLlib's new Offline RL API, there are cases where you may prefer to use RLlib's native episode format. 
As briefly mentioned earlier, such scenarios typically arise when full expert trajectories are required.

.. note::
    RLlib processes tabular data in batches, converting each row into a *single-step episode*. This approach is primarily for procedural simplicity, as data cannot 
    generally be assumed to arrive in time-ordered rows grouped by episodes, though this may occasionally be the case (however knowledge of such a structure resides  
    with the user as RLlib cannot easily infer it automatically). While it is possible to concatenate ``SingleAgentEpisode`` chunks, this cannot be done with fragmented chunks.

If you require full trajectories you can transform your tabular data into ``SingleAgentEpisode`` objects and store these in Parquet format. The next example shows how to do this. 
First, you will store experiences of the above trained expert policy in tabular format:

.. testcode:: python

    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core import (
        COMPONENT_LEARNER_GROUP,
        COMPONENT_LEARNER,
        COMPONENT_RL_MODULE,
        DEFAULT_MODULE_ID,
    )
    from ray.rllib.core.rl_module import RLModuleSpec
    
    # Set up a path for the tabular data records.
    tabular_data_path = "tmp/docs_rllib_offline_recording_tabular"

    # Configure the algorithm for recording.
    config = (
        PPOConfig()
        # The environment needs to be specified.
        .environment(
            env="CartPole-v1",
        )
        # Make sure you use the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        # Make sure to sample complete episodes because
        # you want to record RLlib's episode objects.
        .env_runners(
            batch_mode="complete_episodes",
        )
        # Set up 5 evaluation `EnvRunners` for recording.
        # Sample 50 episodes in each evaluation rollout.
        .evaluation(
            evaluation_num_env_runners=5,
            evaluation_duration=50,
        )
        # Use the checkpointed expert policy from PPO training
        # above. Note, we have to use the same `model_config` as
        # the one with which the expert policy was trained, otherwise
        # the module state cannot be loaded.
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                # Share encoder layers between value network
                # and policy.
                vf_share_layers=True,
            ),
        )
        # Define the output path and format. In this example you
        # want to store data directly in RLlib's episode objects. 
        .offline_data(
            output=tabular_data_path,
            # You want to store for this example tabular data.
            output_write_episodes=False,
        )
    )

    # Build the algorithm.
    algo = config.build()
    # Load now the PPO-trained `RLModule` to use in recording.
    algo.restore_from_path(
        best_checkpoint,
        # Load only the `RLModule` component here.
        component=COMPONENT_RL_MODULE,
    )

    # Run 10 evaluation iterations and record the data.
    for i in range(10):
        print(f"Iteration {i + 1}")
        res_eval = algo.evaluate()
        print(res_eval)

    # Stop the algorithm. Note, this is important for when
    # defining `output_max_rows_per_file`. Otherwise,
    # remaining episodes in the `EnvRunner`s buffer will not
    # be written to disk.
    algo.stop()

You may have noticed that recording data in tabular format takes significantly longer than recording in episode format. This slower performance is due to the additional post-processing 
required to convert episode data into a columnar format. To confirm that the recorded data is now in columnar format, you can print its schema:

.. testcode:: python

    from ray import data
    
    # Read the tabular data into a Ray dataset.
    ds = ray.data.read_parquet(tabular_data_path)
    # Now, print its schema.
    print("Tabular data schema of expert experiences:\n")
    print(ds.schema())
    
    # Column              Type
    # ------              ----
    # eps_id              string
    # agent_id            null
    # module_id           null
    # obs                 numpy.ndarray(shape=(4,), dtype=float)
    # actions             int32
    # rewards             double
    # new_obs             numpy.ndarray(shape=(4,), dtype=float)
    # terminateds         bool
    # truncateds          bool
    # action_dist_inputs  numpy.ndarray(shape=(2,), dtype=float)
    # action_logp         float
    # weights_seq_no      int64

.. note::
    ``infos`` are not stored to disk when they are all empty.

If your expert data is given in columnar format and you need to train on full expert trajectories you can follow the code in the following example to convert 
your own data into RLlib's ``SingleAgentEpisode`` objects:

.. testcode:: python
    import gymnasium as gym
    import msgpack
    import msgpack_numpy as mnp

    from collections import defaultdict

    from ray import data
    from ray.rllib.env.single_agent_episode import SingleAgentEpisode

    # Load the dataset with the tabular data.
    ds = data.read_parquet(tabular_data_path)

    # Build the environment from which the data was sampled to get the
    # spaces.
    env = gym.make("CartPole-v1")
    # Define buffers for episode data.
    eps_obs = []
    eps_actions = []
    eps_rewards = []
    # Note, extra-model-outputs needs to be a dictionary with list
    # values.
    eps_extra_model_outputs = defaultdict(list)
    # Define a buffer for unwritten episodes.
    episodes = []

    # Start iterating over the rows of your experience data.
    for i, row in enumerate(ds.iter_rows(prefetch_batches=10)):
        # If the episode is not terminated nor truncated, buffer the data.
        if not row["terminateds"] and not row["truncateds"]:
            eps_obs.append(row["obs"])
            eps_actions.append(row["actions"])
            eps_rewards.append(row["rewards"])
            eps_extra_model_outputs["action_dist_inputs"].append(row["action_dist_inputs"])
            eps_extra_model_outputs["action_logp"].append(row["action_logp"])
        # Otherwise, build the episode.
        else:
            eps_obs.append(row["new_obs"])
            episode = SingleAgentEpisode(
                id_=row["eps_id"],
                agent_id=row["agent_id"],
                module_id=row["module_id"],
                observations=eps_obs,
                # Use the spaces from the environment.
                observation_space=env.observation_space,
                action_space=env.action_space,
                actions=eps_actions,
                rewards=eps_rewards,
                # Set the starting timestep to zero.
                t_started=0,
                # You do not want to have a lookback buffer.
                len_lookback_buffer=0,
                terminated=row["terminateds"],
                truncated=row["truncateds"],
                extra_model_outputs=eps_extra_model_outputs,
            )
            # Store the ready-to-write episode to the episode buffer.
            episodes.append(msgpack.packb(episode.get_state(), default=mnp.encode))
            # Clear all episode data buffers.
            eps_obs.clear()
            eps_actions.clear()
            eps_rewards.clear()
            eps_extra_model_outputs = defaultdict(list)

        # Write episodes to disk when the episode buffer holds 50 episodes.
        if len(episodes) > 49:
            # Generate a Ray dataset from episodes.
            episodes_ds = data.from_items(episodes)
            # Write the Parquet data and compress it.
            episodes_ds.write_parquet(
                f"/tmp/test_converting/file-{i}".zfill(6),
                compression="gzip",
            )
            # Delete the dataset in memory and clear the episode buffer.
            del episodes_ds
            episodes.clear()

    # If we are finished and have unwritten episodes, write them now.
    if len(episodes) > 0:
        episodes_ds = data.from_items(episodes)
        episodes_ds.write_parquet(
            f"/tmp/test_converting/file-{i}".zfill(6),
            compression="gzip",
        )
        del episodes_ds
        episodes.clear()

Using old API stack ``SampleBatch`` recordings
----------------------------------------------
If you have expert data previously recorded using RLlib's older API stack, it can be seamlessly utilized in the new Offline RL API by setting ``input_read_sample_batches=True``. Alternatively, you can convert your ``SampleBatch`` recordings into 
``SingleAgentEpisode`` format using RLlib's ``OfflinePreLearner`` as demonstrated below:

.. code-block:: python

    import msgpack
    import msgpack_numpy as mnp

    from ray import data
    from ray.rllib.offline.offline_prelearner import OfflinePreLearner

    # Set up the data path to your `SampleBatch` expert data.
    data_path = ...
    # Set up the write path for the Parquet episode data.
    output_data_path = "/tmp/sample_batch_data"

    # Load the `SampleBatch` recordings.
    ds = data.read_json(data_path)

    # Iterate over batches (of `SampleBatch`es) and convert them to episodes.
    for i, batch in enumerate(ds.iter_batches(batch_size=100, prefetch_batches=2)):
        # Use the RLlib's `OfflinePreLearner` to convert `SampleBatch`es to episodes.
        episodes = OfflinePreLearner._map_sample_batch_to_episode(False, batch)["episodes"]

        # Create a dataset from the episodes. Note, for storing episodes you need to 
        # serialize them via `msgpack-numpy`.
        episode_ds = data.from_items([msgpack.packb(eps.get_state(), default=mnp.encode) for eps in episodes])
        # Write the batch of episodes to local disk.
        episode_ds.write_parquet(output_data_path + f"/file-{i}".zfill(6), compression="gzip")

    print("Finished converting `SampleBatch` data to episode data.")

.. note:: RLlib considers your ``SampleBatch`` to represent a terminated/truncated episode and builds its ``SingleAgentEpisode`` according to this assumption.

Pre-processing, filtering and post-processing
---------------------------------------------

During recording, your expert policy may utilize pre-processing techniques for observations, such as *frame-stacking*, or filtering methods like *mean-std filtering*. Similarly, actions may undergo pre-processing, such as *action 
sampling* or *scaling*. In its ``EnvRunner`` instances, RLlib applies such pre-processing and filtering (via the *env-to-module* connector pipeline) **before** observations are passed to the ``RLModule``. However, raw observations (as received 
directly from the environment) are stored in the episodes. Likewise, actions are recorded in their raw form (as output directly from the ``RLModule``) while undergoing pre-processing (via RLlib's *module-to-env* connectors) before being 
sent to the environment.

It is crucial to carefully consider the pre-processing and filtering applied during the recording of experiences, as they significantly influence how the expert policy learns and subsequently performs in the environment. For example, if 
the expert policy uses *mean-std filtering* for observations, it learns a strategy based on the filtered observations, where the filter itself is highly dependent on the experiences collected during training. When deploying this expert 
policy, it is essential to use the exact same filter during evaluation to avoid performance degradation. Similarly, a policy trained via behavior cloning may also require a *mean-std filter* for observations to accurately replicate the 
behavior of the expert policy.

Scaling I/O throughput
-----------------------

Just as online training can be scaled, offline recording I/O throughput can also be increased by configuring the number of RLlib env-runners. Use the ``num_env_runners`` setting to scale recording during training or ``evaluation_num_env_runners``
for scaling during evaluation-only recording. Each worker operates independently, writing experiences in parallel, enabling linear scaling of I/O throughput for write operations. Within each ``OfflineSingleAgentEnvRunner``, episodes are sampled 
(environments are usually vectorized) and serialized before being written to disk.

Offline RL training in RLlib is highly parallelized, encompassing data reading, post-processing, and, if applicable, updates. When training on offline data, scalability is achieved by increasing the number of ``DataWorker`` instances used to 
transform offline experiences into a learner-compatible format (``MultiAgentBatch``). Ray Data optimizes reading operations under the hood by leveraging file metadata, predefined concurrency settings for batch post-processing, and available 
system resources. It is strongly recommended not to override these defaults, as doing so may disrupt this optimization process.

Data processing in RLlib involves three key layers, all of which are highly scalable:

#. **Read Operations:** This layer handles data ingestion from files in a specified folder. It is automatically optimized by Ray Data and should not be manually scaled or adjusted.
#. **Post-processing (PreLearner):** In this stage, batches are converted, if necessary, into RLlib's ``SingleAgentEpisode`` format and passed through the *learner connector pipeline*. The processed data is then transformed into ``MultiAgentBatch`` objects for updating. This layer can be scaling the ``DataWorker`` instances.
#. **Updating (Learner):** This stage involves updating the policy and associated modules. Scalability is achieved by increasing the number of learners (``num_learners``), enabling parallel processing of batches during updates.

The diagram below illustrates the layers and their scalability:

.. image:: images/offline/key_layers.svg
    :width: 500
    :alt: Key layers of RLlib's fully scalable Offline RL API.

**Read operations** are executed exclusively on the CPU and are primarily scaled by allocating additional resources (see :ref:`How to tune performance` for details), as they are fully managed by Ray Data. **Post-processing** can be scaled by increasing 
the concurrency level specified in the keyword arguments for the mapping operation:

.. code-block:: python
    
    AlgorithmConfig()
    .offline_data(
        map_batches_kwargs={
            "concurrency": 10,
            "num_cpus": 4,
        }
    )

This initiates an actor pool with 10 ``DataWorker`` instances, each running an instance of RLlib's callable ``OfflinePreLearner`` class to post-process batches for updating the ``RLModule``.

.. note:: The ``num_cpus`` (and similarly the ``num_gpus``) attribute defines the resources **allocated to each** ``DataWorker`` not the full actor pool.

You scale the number of learners in RLlib's ``learners`` configuration block:

.. code-block:: python
    
    AlgorithmConfig()
    .learners(
        num_learners=4,
        num_gpus_per_learner=1,
    )

With this configuration you start an application with 4 (remote) ``Learner`` s (see :ref:`Learner (Alpha)` for more details about RLlib's learners) each of them using a single GPU. 

Using cloud storage
-------------------
Unlike RLlib's previous stack, the new Offline RL API is cloud-agnostic and fully integrates with PyArrow. You can utilize any available cloud storage path or PyArrow-compatible filesystem. If 
using a PyArrow (or compatible) filesystem, ensure that your ``input_`` file path is a relative path within this filesystem. Similar to Ray Data, you can also use placeholders, lists of files 
or folders, or simply specify a single folder to read recursively from.

For example, to read from a storage bucket in GCS, you can specify the folder location as follows:

.. code-block:: python

    AlgorithmConfig()
    .offline_data(
        input_="gs://<your-bucket>/dir1",
    )

This configuration allows RLlib to read data recursively from any folder beneath the specified path. If you are using a filesystem for GCS (for instance, due to authentication requirements), 
use the following syntax:

.. code-block:: python

    import pyarrow.fs
    
    # Define the PyArrow filesystem
    gcs = pyarrow.fs.GcsFilesystem(
        # This is needed to resolve the hostname for public buckets.
        anonymous=True, 
        retry_time_limit=timedelta(seconds=15)
    )

    # Define the configuration.
    AlgorithmConfig()
    .offline_data(
        # NOTE: Use a relative file path now
        input_="<public-bucket>/dir1",
        input_filesystem=gcs,
    )

You can learn more about PyArrow's filesystems, particularly regarding cloud filesystems and required authentication, in `PyArrow Filesystem Interface <https://arrow.apache.org/docs/python/filesystems.html#filesystem-interface>`__.

.. note:: When using cloud storage, Ray Data typically streams data, meaning it is consumed in chunks. This allows postprocessing and training to begin after a brief warmup phase. More specifically, even if your cloud storage is large, the same amount of 
    space is not required on the node(s) running RLlib.

How to tune performance 
-----------------------

Because the different key layers in the offline RL API are managed by different modules and configurations scaling the layers is not straightforward and you need to understand which parameters have which leverage

Input API
---------

You can configure experience input for an agent using the following options:

.. code-block:: python

    def offline_data(
        self,
        *,
        # Specify how to generate experiences:
        # - A local directory or file glob expression (e.g., "/tmp/*.json").
        # - A cloud storage path or file glob expression (e.g., "gs://rl/").
        # - A list of individual file paths/URIs (e.g., ["/tmp/1.json",
        #   "s3://bucket/2.json"]).
        # - A file or directory path in a given `input_filesystem`.
        input_: Optional[Union[str, Callable[[IOContext], InputReader]]],
        # Read method for the `ray.data.Dataset` to read in the
        # offline data from `input_`. The default is `read_parquet` for Parquet
        # files. See https://docs.ray.io/en/latest/data/api/input_output.html for
        # more info about available read methods in `ray.data`.
        input_read_method: Optional[Union[str, Callable]],
        # Keyword args for `input_read_method`. These
        # are passed into the read method without checking. Use these
        # keyword args together with `map_batches_kwargs` and
        # `iter_batches_kwargs` to tune the performance of the data pipeline. It 
        # is strongly recommended to rely on Ray Data's automatic read performance 
        # tuning
        input_read_method_kwargs: Optional[Dict],
        # Table schema for converting offline data to episodes.
        # This schema maps the offline data columns to
        # `ray.rllib.core.columns.Columns`:
        # `{Columns.OBS: 'o_t', Columns.ACTIONS: 'a_t', ...}`. Columns in
        # the data set that are not mapped via this schema are sorted into
        # episodes' `extra_model_outputs`. If no schema is passed in the default
        # schema used is `ray.rllib.offline.offline_data.SCHEMA`. If your data set
        # contains already the names in this schema, no `input_read_schema` is
        # needed. The same applies, if the offline data is in RLlib's
        # `EpisodeType` or old `SampleBatch` format
        input_read_schema: Optional[Dict[str, str]],
        # Whether offline data is already stored in RLlib's
        # `EpisodeType` format, i.e. `ray.rllib.env.SingleAgentEpisode` (multi
        # -agent is planned but not supported, yet). Reading episodes directly
        # avoids additional transform steps and is usually faster and
        # therefore the recommended format when your application remains fully
        # inside of RLlib's schema. The other format is a columnar format and is
        # agnostic to the RL framework used. Use the latter format, if you are
        # unsure when to use the data or in which RL framework. The default is
        # to read column data, i.e. `False`. `input_read_episodes` and
        # `input_read_sample_batches` cannot be `True` at the same time. See
        # also `output_write_episodes` to define the output data format when
        # recording.
        input_read_episodes: Optional[bool],
        # Whether offline data is stored in RLlib's old
        # stack `SampleBatch` type. This is usually the case for older data
        # recorded with RLlib in JSON line format. Reading in `SampleBatch`
        # data needs extra transforms and might not concatenate episode chunks
        # contained in different `SampleBatch`es in the data. If possible avoid
        # to read `SampleBatch`es and convert them in a controlled form into
        # RLlib's `EpisodeType` (i.e. `SingleAgentEpisode`). The default is 
        # `False`. `input_read_episodes` and `input_read_sample_batches` cannot 
        # be True at the same time.
        input_read_sample_batches: Optional[bool],
        # Batch size to pull from the data set. This could
        # differ from the `train_batch_size_per_learner`, if a dataset holds
        # `EpisodeType` (i.e. `SingleAgentEpisode`) or `SampleBatch`, or any 
        # other data type that contains multiple timesteps in a single row of the
        # dataset. In such cases a single batch of size
        # `train_batch_size_per_learner` will potentially pull a multiple of
        # `train_batch_size_per_learner` timesteps from the offline dataset. The
        # default is `None` in which the `train_batch_size_per_learner` is pulled.
        input_read_batch_size: Optional[int],
        # A cloud filesystem to handle access to cloud storage when
        # reading experiences. Can be "gcs" for Google Cloud Storage, "s3" for AWS
        # S3 buckets, "abs" for Azure Blob Storage, or any filesystem supported 
        # by PyArrow. In general the file path is sufficient for accessing data
        # from public or local storage systems. See 
        # https://arrow.apache.org/docs/python/filesystems.html for details.
        input_filesystem: Optional[str],
        # A dictionary holding the kwargs for the filesystem
        # given by `input_filesystem`. See `gcsfs.GCSFilesystem` for GCS,
        # `pyarrow.fs.S3FileSystem`, for S3, and `ablfs.AzureBlobFilesystem` for
        # ABS filesystem arguments.
        input_filesystem_kwargs: Optional[Dict],
        # What input columns are compressed with LZ4 in the
        # input data. If data is stored in RLlib's `SingleAgentEpisode` (
        # `MultiAgentEpisode` not supported, yet). Note the providing
        # `rllib.core.columns.Columns.OBS` also tries to decompress
        # `rllib.core.columns.Columns.NEXT_OBS`.
        input_compress_columns: Optional[List[str]],
        # Whether the raw data should be materialized in memory.
        # This boosts performance, but requires enough memory to avoid an OOM, so
        # make sure that your cluster has the resources available. For very large
        # data you might want to switch to streaming mode by setting this to
        # `False` (default). If your algorithm does not need the RLModule in the
        # Learner connector pipeline or all (learner) connectors are stateless
        # you should consider setting `materialize_mapped_data` to `True`
        # instead (and set `materialize_data` to `False`). If your data does not
        # fit into memory and your Learner connector pipeline requires an RLModule
        # or is stateful, set both `materialize_data` and
        # `materialize_mapped_data` to `False`.
        materialize_data: Optional[bool],
        # Whether the data should be materialized after
        # running it through the Learner connector pipeline (i.e. after running
        # the `OfflinePreLearner`). This improves performance, but should only be
        # used in case the (learner) connector pipeline does not require an
        # RLModule and the (learner) connector pipeline is stateless. For example,
        # MARWIL's Learner connector pipeline requires the RLModule for value
        # function predictions and training batches would become stale after some
        # iterations causing learning degradation or divergence. Also ensure that
        # your cluster has enough memory available to avoid an OOM. If set to
        # `True`, make sure that `materialize_data` is set to `False` to
        # avoid materialization of two datasets. If your data does not fit into
        # memory and your Learner connector pipeline requires an RLModule or is
        # stateful, set both `materialize_data` and `materialize_mapped_data` to
        # `False`.
        materialize_mapped_data: Optional[bool],
        # Keyword args for the `map_batches` method. These are
        # passed into the `ray.data.Dataset.map_batches` method when sampling
        # without checking. If no arguments passed in the default arguments
        # `{'concurrency': max(2, num_learners), 'zero_copy_batch': True}` is
        # used. Use these keyword args together with `input_read_method_kwargs`
        # and `iter_batches_kwargs` to tune the performance of the data pipeline.
        map_batches_kwargs: Optional[Dict],
        # Keyword args for the `iter_batches` method. These are
        # passed into the `ray.data.Dataset.iter_batches` method when sampling
        # without checking. If no arguments are passed in, the default argument
        # `{'prefetch_batches': 2}` is used. Use these keyword args
        # together with `input_read_method_kwargs` and `map_batches_kwargs` to
        # tune the performance of the data pipeline.
        iter_batches_kwargs: Optional[Dict],
        # An optional `OfflinePreLearner` class that is used to
        # transform data batches in `ray.data.map_batches` used in the
        # `OfflineData` class to transform data from columns to batches that can
        # be used in the `Learner.update...()` methods. Override the
        # `OfflinePreLearner` class and pass your derived class in here, if you
        # need to make some further transformations specific for your data or
        # loss. The default is `None`` which uses the base `OfflinePreLearner`
        # defined in `ray.rllib.offline.offline_prelearner`. 
        prelearner_class: Optional[Type],
        # An optional `EpisodeReplayBuffer` class is
        # used to buffer experiences when data is in `EpisodeType` or
        # RLlib's previous `SampleBatch` type format. In this case, a single
        # data row may contain multiple timesteps and the buffer serves two
        # purposes: (a) to store intermediate data in memory, and (b) to ensure
        # that exactly `train_batch_size_per_learner` experiences are sampled
        # per batch. The default is RLlib's `EpisodeReplayBuffer`.
        prelearner_buffer_class: Optional[Type],
        # Optional keyword arguments for intializing the
        # `EpisodeReplayBuffer`. In most cases this is simply the `capacity`
        # for the default buffer used (`EpisodeReplayBuffer`), but it may
        # differ if the `prelearner_buffer_class` uses a custom buffer.
        prelearner_buffer_kwargs: Optional[Dict],
        # Number of updates to run in each learner
        # during a single training iteration. If None, each learner runs a
        # complete epoch over its data block (the dataset is partitioned into
        # at least as many blocks as there are learners). The default is `None`.
        # This must be set to `1`, if a single (local) learner is used.
        dataset_num_iters_per_learner: Optional[int],
    )

Example Custom Input API
------------------------

You can create a custom input reader like the following:

.. code-block:: python

    from ray.rllib.offline import InputReader, IOContext, ShuffledInput
    from ray.tune.registry import register_input

    class CustomInputReader(InputReader):
        def __init__(self, ioctx: IOContext): ...
        def next(self): ...

    def input_creator(ioctx: IOContext) -> InputReader:
        return ShuffledInput(CustomInputReader(ioctx))

    register_input("custom_input", input_creator)

    config = {
        "input": "custom_input",
        "input_config": {},
        ...
    }

You can pass arguments from the config to the custom input api through the
``input_config`` option which can be accessed with the ``IOContext``.
The interface for the ``IOContext`` is the following:

.. autoclass:: ray.rllib.offline.IOContext
    :members:
    :noindex:

See `custom_input_api.py <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/custom_input_api.py>`__ for a runnable example.

Output API
----------

You can configure experience output for an agent using the following options:

.. tip::
    Plain python config dicts will soon be replaced by :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
    objects, which have the advantage of being type safe, allowing users to set different config settings within
    meaningful sub-categories (e.g. ``my_config.offline_data(input_=[xyz])``), and offer the ability to
    construct an Algorithm instance from these config objects (via their ``.build()`` method).

.. code-block:: python

    # Specify where experiences should be saved:
    #  - None: don't save any experiences
    #  - "logdir" to save to the agent log dir
    #  - a path/URI to save to a custom output directory (e.g., "s3://bucket/")
    #  - a function that returns a rllib.offline.OutputWriter
    "output": None,
    # Arguments accessible from the IOContext for configuring custom output
    "output_config": {},
    # What sample batch columns to LZ4 compress in the output data.
    "output_compress_columns": ["obs", "new_obs"],
    # Max output file size (in bytes) before rolling over to a new file.
    "output_max_file_size": 64 * 1024 * 1024,

The interface for a custom output writer is as follows:

.. autoclass:: ray.rllib.offline.OutputWriter
    :members:
    :noindex:
