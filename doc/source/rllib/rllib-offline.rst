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

1. **Read Operations:** This layer handles data ingestion from files in a specified folder. It is automatically optimized by Ray Data and should not be manually scaled or adjusted.
2. **Post-processing:** In this stage, batches are converted, if necessary, into RLlib's ``SingleAgentEpisode`` format and passed through the *learner connector pipeline*. The processed data is then transformed into ``MultiAgentBatch`` objects for updating. This layer can be scaling the ``DataWorker`` instances.
3. **Updating:** This stage involves updating the policy and associated modules. Scalability is achieved by increasing the number of learners (``num_learners``), enabling parallel processing of batches during updates.

Ray Data Integration
--------------------

RLlib has experimental support for reading/writing training samples from/to large offline datasets using
:ref:`Ray Data <data>`.
We support JSON and Parquet files today. Other file formats supported by Ray Data can also be easily added.

Unlike JSON input, a single dataset can be automatically sharded and replayed by multiple rollout workers
by simply specifying the desired ``num_env_runners`` config.

To load sample data using Dataset, specify input and input_config keys like the following:

.. code-block:: python

    config = {
        ...
        "input"="dataset",
        "input_config"={
            "format": "json",  # json or parquet
	    # Path to data file or directory.
            "path": "/path/to/json_dir/",
	    # Num of tasks reading dataset in parallel, default is num_env_runners.
            "parallelism": 3,
	    # Dataset allocates 0.5 CPU for each reader by default.
	    # Adjust this value based on the size of your offline dataset.
            "num_cpus_per_read_task": 0.5,
        }
	...
    }



Writing Environment Data
--------------------------

To include environment data in the training sample datasets you can use the optional
``store_infos`` parameter that is part of the ``output_config`` dictionary. This parameter
ensures that the ``infos`` dictionary, as returned by the RL environment, is included in the output files.

.. note:: It is the responsibility of the user to ensure that the content of ``infos`` can be serialized to file.

.. note:: This setting is only relevant for the TensorFlow based agents, for PyTorch agents the ``infos`` data is always stored.

To write the ``infos`` data to JSON or Parquet files using Dataset, specify output and output_config keys like the following:

.. code-block:: python

    config = {
        "output": "dataset",
        "output_config": {
            "format": "json",  # json or parquet
            # Directory to write data files.
            "path": "/tmp/test_samples/",
            # Write the infos dict data
            "store_infos" : True,
        }
    }



Input Pipeline for Supervised Losses
------------------------------------

You can also define supervised model losses over offline data. This requires defining a `custom model loss <rllib-models.html#supervised-model-losses>`__. We provide a convenience function, ``InputReader.tf_input_ops()``, that can be used to convert any input reader to a TF input pipeline. For example:

.. code-block:: python

    def custom_loss(self, policy_loss):
        input_reader = JsonReader("/tmp/cartpole-out")
        # print(input_reader.next())  # if you want to access imperatively

        input_ops = input_reader.tf_input_ops()
        print(input_ops["obs"])  # -> output Tensor shape=[None, 4]
        print(input_ops["actions"])  # -> output Tensor shape=[None]

        supervised_loss = some_function_of(input_ops)
        return policy_loss + supervised_loss

See `custom_model_loss_and_metrics.py <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_model_loss_and_metrics.py>`__ for a runnable example of using these TF input ops in a custom loss.

Input API
---------

You can configure experience input for an agent using the following options:

.. tip::
    Plain python config dicts will soon be replaced by :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`
    objects, which have the advantage of being type safe, allowing users to set different config settings within
    meaningful sub-categories (e.g. ``my_config.offline_data(input_=[xyz])``), and offer the ability to
    construct an Algorithm instance from these config objects (via their ``.build()`` method).


.. code-block:: python

    # Specify how to generate experiences:
    #  - "sampler": Generate experiences via online (env) simulation (default).
    #  - A local directory or file glob expression (e.g., "/tmp/*.json").
    #  - A list of individual file paths/URIs (e.g., ["/tmp/1.json",
    #    "s3://bucket/2.json"]).
    #  - A dict with string keys and sampling probabilities as values (e.g.,
    #    {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
    #  - A callable that takes an `IOContext` object as only arg and returns a
    #    ray.rllib.offline.InputReader.
    #  - A string key that indexes a callable with tune.registry.register_input
    "input": "sampler",
    # Arguments accessible from the IOContext for configuring custom input
    "input_config": {},
    # True, if the actions in a given offline "input" are already normalized
    # (between -1.0 and 1.0). This is usually the case when the offline
    # file has been generated by another RLlib algorithm (e.g. PPO or SAC),
    # while "normalize_actions" was set to True.
    "actions_in_input_normalized": False,
    # Specify how to evaluate the current policy. This only has an effect when
    # reading offline experiences ("input" is not "sampler").
    # Available options:
    #  - "simulation": Run the environment in the background, but use
    #    this data for evaluation only and not for learning.
    #  - Any subclass of OffPolicyEstimator, e.g.
    #    ray.rllib.offline.estimators.is::ImportanceSampling or your own custom
    #    subclass.
    "off_policy_estimation_methods": {
        "is": {"type": ImportanceSampling},
        "wis": {"type": WeightedImportanceSampling}
    },
    # Whether to run postprocess_trajectory() on the trajectory fragments from
    # offline inputs. Note that postprocessing will be done using the *current*
    # policy, not the *behavior* policy, which is typically undesirable for
    # on-policy algorithms.
    "postprocess_inputs": False,
    # If positive, input batches will be shuffled via a sliding window buffer
    # of this number of batches. Use this if the input data isn't in random
    # enough order. Input is delayed until the shuffle buffer is filled.
    "shuffle_buffer_size": 0,

The interface for a custom input reader is as follows:

.. autoclass:: ray.rllib.offline.InputReader
    :members:
    :noindex:

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
