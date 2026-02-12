.. include:: /_includes/rllib/we_are_hiring.rst

Working with offline data
=========================

.. include:: /_includes/rllib/new_api_stack.rst

RLlib's offline RL API enables you to work with experiences read from offline storage (for example, disk, cloud storage,
streaming systems, Hadoop Distributed File System (HDFS). For example, you might want to read experiences saved from previous training runs, collected
from experts, or gathered from policies deployed in `web applications <https://arxiv.org/abs/1811.00260>`__. You can
also log new agent experiences produced during online training for future use.

RLlib represents trajectory sequences (for example, ``(s, a, r, s', ...)`` tuples) with :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`
objects (multi-agent offline training is currently not supported). Using this episode format
allows for efficient encoding and compression of experiences, rewriting trajectories, and user-friendly data access through getter methods.
During online training, RLlib uses :py:class:`~ray.rllib.env.single_agent_env_runner.SingleAgentEnvRunner` actors to generate episodes of experiences
in parallel using the current policy. However, RLlib uses this same episode format for reading experiences from and writing experiences to offline storage (see :py:class:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner`).

You can store experiences either directly in RLlib's episode format or in table (columns)
format. You should use the episode format when

#. You need experiences grouped by their trajectory and ordered in time (for example, to train stateful modules).
#. You want to use recorded experiences exclusively within RLlib (for example for offline RL or behavior cloning).

On the contrary, you should prefer the table (columns) format, if

#. You need to read the data easily with other data tools or ML libraries.

.. note:: RLlib's new API stack incorporates principles that support standalone applications. Consequently, the
    :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` class is usable outside of an RLlib context. To enable faster
    access through external data tools (for example, for data transformations), it's recommended to use the table record format.

Most importantly, RLlib's offline RL API builds on top of :ref:`Ray Data <data>` and therefore supports all of its read and
write methods (for example :py:class:`~ray.data.read_parquet`, :py:class:`~ray.data.read_json`, etc.) with
:py:class:`~ray.data.read_parquet` and :py:class:`~ray.data.Dataset.write_parquet` being the default read and write methods. A core design principle
of the API is to apply as many data transformations as possible on-the-fly prior to engaging the learner, allowing the latter to focus exclusively on model updates.

.. hint:: During the transition phase from old- to new API stack you can use the new offline RL API also with your
    :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` data recorded with the old API stack. To enable this feature set
    ``config.offline_data(input_read_sample_batches=True)``.

Example: Training an expert policy
----------------------------------
In this example you train a PPO agent on the ``CartPole-v1`` environment until it reaches an episode mean return of ``450.0``. You checkpoint
this agent and then use its policy to record expert data to local disk.

.. dropdown:: **Code example 1: Training an expert policy**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_1__begin__
        :end-before: __sphinx_doc_offline_api_1__end__

In this example, you saved a checkpoint from an agent that has become an expert at playing ``CartPole-v1``.  You use this checkpoint in the next
example to record expert data to disk, which is later utilized for offline training to clone another agent.

Example: Record expert data to local disk
-----------------------------------------
After you train an expert policy to play `CartPole-v1` you load its policy here to record expert data during evaluation. You use ``5``
:py:class:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner` instances to collect ``50`` complete episodes per `sample()` call. In this
example you store experiences directly in RLlib's :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects with no more than
``25`` episode objects per Parquet file. Altogether you run 10 evaluation runs, which should result in ``500`` recorded episodes from the expert policy.
You use these data in the next example to train a new policy through Offline RL that should reach a return of ``450.0`` when playing ``CartPole-v1``.

.. dropdown:: **Code example 2: Recording expert data to local disk**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_2__begin__
        :end-before: __sphinx_doc_offline_api_2__end__

.. note:: RLlib formats The stored episode data as ``binary``. Each episode is converted into its dictionary representation and serialized using ``msgpack-numpy``,
    ensuring version compatibility.

RLlib's  recording process is efficient because it utilizes multiple :py:class:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner` instances during
evaluation, enabling parallel data writing. You can explore the folder to review the stored Parquet data:

.. code-block:: text

    $ ls -la /tmp/docs_rllib_offline_recording/cartpole-v1

    drwxr-xr-x. 22 user user 440 21. Nov 17:23 .
    drwxr-xr-x.  3 user user  60 21. Nov 17:23 ..
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000001-00004
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000001-00009
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000001-00012
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000001-00016
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000002-00004
    drwxr-xr-x.  2 user user 540 21. Nov 17:23 run-000002-00007


.. hint:: RLlib stores records under a folder named by the RL environment. Therein, you see one folder of Parquet files for each :py:class:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner`
    and write operation. The write operation count is given in the second numbering. For example: above, env-runner 1 has sampled 25 episodes at
    its 4th :py:meth:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner.sample` call and writes then (because ``output_max_rows_per_file=25``) all sampled episodes
    to disk into file ``run-000001-00004``.

.. note:: The number of write operations per worker may vary because policy rollouts aren't evenly distributed. Faster workers collect more episodes,
    leading to differences in write operation counts. As a result, the second numbering may differ across files generated by different env-runner instances.

Example: Training on previously saved experiences
-------------------------------------------------
In this example you are using behavior cloning with the previously recorded Parquet data from your expert policy playing ``CartPole-v1``. The
data needs to be linked in the configuration of the algorithm (through the ``input_`` attribute).

.. dropdown:: **Code example 3: Training on previously saved experiences**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_3__begin__
        :end-before: __sphinx_doc_offline_api_3__end__

Behavior cloning in RLlib is highly performant, completing a single training iteration in approximately 2 milliseconds. The experiment's
results should resemble the following:

.. image:: images/offline/docs_rllib_offline_bc_episode_return_mean.svg
    :alt: Episode mean return over the course of BC training.
    :width: 500
    :align: left

It should take you around ``98`` seconds (``456`` iterations) to achieve the same episode return mean as the PPO agent. While this may not seem
impressive compared to the PPO training time, it's important to note that ``CartPole-v1`` is a very simple environment to learn. In more
complex environments, which require more sophisticated agents and significantly longer training times, pre-training through behavior cloning
can be highly beneficial. Combining behavior cloning with subsequent fine-tuning using a reinforcement learning algorithm can substantially
reduce training time, resource consumption, and associated costs.

Using external expert experiences
---------------------------------
Your expert data is often already available, either recorded from an operational system or directly provided by human experts. Typically,
you might store this data in a tabular (columnar) format. RLlib's new Offline RL API simplifies the use of such data by allowing direct ingestion
through a specified schema that organizes the expert data. The API default schema for reading data is provided in
:py:data:`~ray.rllib.offline.offline_prelearner.SCHEMA`.

.. tip::
    RLlib uses :py:class:`~ray.rllib.utils.replay_buffers.episode_replay_buffer.EpisodeReplayBuffer` to store the episodes we read. 
    This buffer accepts episodes without further transformations, making reading episodes the fastest operation.


    .. image:: images/offline/docs_rllib_offline_prelearner.svg
        :alt: The OfflinePreLearner converts and buffers episodes before sampling the batches used in learning.
        :width: 500
        :align: left

Lets consider a simple example in which your expert data is stored with the schema: ``(o_t, a_t, r_t, o_tp1, d_t, i_t, logprobs_t)``. In this case
you provide this schema as follows:

.. dropdown:: **Code: Using external expert experiences with custom schema**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_4__begin__
        :end-before: __sphinx_doc_offline_api_4__end__

.. note:: Internally, the legacy ``gym``'s ``done`` signals are mapped to ``gymnasium``'s ``terminated`` signals, with ``truncated`` values defaulting to
    ``False``. RLlib's :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` structures align with ``gymnasium``, adhering to the updated environment API standards in reinforcement learning.

Converting tabular data to RLlib's episode format
-------------------------------------------------
While the tabular format is widely compatible and seamlessly integrates with RLlib's new Offline RL API, there are cases where you may prefer to use RLlib's native episode format.
As briefly mentioned earlier, such scenarios typically arise when full expert trajectories are required.

.. note::
    RLlib processes tabular data in batches, converting each row into a *single-step episode*. This approach is primarily for procedural simplicity, as data can't
    generally be assumed to arrive in time-ordered rows grouped by episodes, though this may occasionally be the case (however knowledge of such a structure resides
    with the user as RLlib can't easily infer it automatically). While it's possible to concatenate consecutive :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`
    chunks, this can't be done with chunks arriving in some scrambled order.

If you require full trajectories you can transform your tabular data into :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects and store these in Parquet format. The next example shows
how to do this.
First, you store experiences of the preceding trained expert policy in tabular format (note the `output_write_episodes=False` setting below to activate tabular data output):

.. dropdown:: **Code: Recording expert data in tabular format**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_5__begin__
        :end-before: __sphinx_doc_offline_api_5__end__

You may have noticed that recording data in tabular format takes significantly longer than recording in episode format. This slower performance is due to the additional post-processing
required to convert episode data into a columnar format. To confirm that the recorded data is now in columnar format, you can print its schema:

.. dropdown:: **Code: Print tabular data schema**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_6__begin__
        :end-before: __sphinx_doc_offline_api_6__end__

.. note::
    ``infos`` aren't stored to disk when they're all empty.

If your expert data is given in columnar format and you need to train on full expert trajectories you can follow the code in the following example to convert
your own data into RLlib's :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects:

.. dropdown:: **Code: Converting tabular data to episode format**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_7__begin__
        :end-before: __sphinx_doc_offline_api_7__end__

.. note::
    For compatibility with RLlib's old "SampleBatch" objects, you can read them by setting ``input_read_sample_batches=True``.


Pre-processing, filtering and post-processing
---------------------------------------------

During recording, your expert policy may utilize pre-processing techniques for observations, such as *frame-stacking*, or filtering methods like *mean-std filtering*. Similarly, actions may undergo pre-processing, such as *action
sampling* or *scaling*. In its ``EnvRunner`` instances, RLlib applies such pre-processing and filtering (through the *env-to-module* connector pipeline) **before** observations are passed to the ``RLModule``. However, raw observations (as received
directly from the environment) are stored in the episodes. Likewise, actions are recorded in their raw form (as output directly from the ``RLModule``) while undergoing pre-processing (through RLlib's *module-to-env* connectors) before being
sent to the environment.

It's crucial to carefully consider the pre-processing and filtering applied during the recording of experiences, as they significantly influence how the expert policy learns and subsequently performs in the environment. For example, if
the expert policy uses *mean-std filtering* for observations, it learns a strategy based on the filtered observations, where the filter itself is highly dependent on the experiences collected during training. When deploying this expert
policy, it's essential to use the exact same filter during evaluation to avoid performance degradation. Similarly, a policy trained through behavior cloning may also require a *mean-std filter* for observations to accurately replicate the
behavior of the expert policy.

Scaling I/O throughput
----------------------

Similar to how we scale :py:class:`~ray.rllib.env.env_runner.EnvRunner` for online learnning, we can scale data ingestion in offline learning with ``num_env_runners``.
:py:class:`~ray.rllib.offline.offline_env_runner.OfflineSingleAgentEnvRunner` processes read and write experiences in parallel, enabling linear scaling of I/O throughput.

Offline RL training in RLlib is highly parallelized, encompassing data reading, post-processing, and, if applicable, updates. When training on offline data, scalability is achieved by increasing the number of ``DataWorker`` instances used to
transform offline experiences into a learner-compatible format (:py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch`). Ray Data optimizes reading operations under the hood by leveraging file metadata, predefined concurrency settings for batch post-processing, and available
system resources. It's strongly recommended not to override these defaults, as doing so may disrupt this optimization process.

Data processing in RLlib involves three key layers, all of which are highly scalable:

#. **Read Operations:** This layer handles data ingestion from files in a specified folder. It's automatically optimized by Ray Data and shouldn't be manually scaled or adjusted.
#. **Post-processing (PreLearner):** In this stage, batches are converted, if necessary, into RLlib's :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` format and passed through the *learner connector pipeline*. The processed data is then transformed into :py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch` objects for updating. This layer can be scaling the ``DataWorker`` instances.
#. **Updating (Learner):** This stage involves updating the policy and associated modules. Scalability is achieved by increasing the number of learners (``num_learners``), enabling parallel processing of batches during updates.

The diagram below illustrates the layers and their scalability:

.. image:: images/offline/key_layers.svg
    :width: 500
    :alt: Key layers of RLlib's fully scalable Offline RL API.

**Read operations** are executed exclusively on the CPU and are primarily scaled by allocating additional resources (see :ref:`How to tune performance <how-to-tune-performance>` for details), as they're fully managed by Ray Data. **Post-processing** can be scaled by increasing
the concurrency level specified in the keyword arguments for the mapping operation:

.. dropdown:: **Code: Scaling post-processing with actor pool**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_9__begin__
        :end-before: __sphinx_doc_offline_api_9__end__

This initiates an actor pool with 10 ``DataWorker`` instances, each running an instance of RLlib's callable :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` class to post-process batches for updating the
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.

.. note:: The ``num_cpus`` (and similarly the ``num_gpus``) attribute defines the resources **allocated to each** ``DataWorker`` not the full actor pool.

You scale the number of learners in RLlib's :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners` configuration block:

.. dropdown:: **Code: Scaling learners**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_10__begin__
        :end-before: __sphinx_doc_offline_api_10__end__

With this configuration you start an application with 4 (remote) :py:class:`~ray.rllib.core.learner.learner.Learner`s (see :ref:`Learner (Alpha)` for more details about RLlib's learners)
each of them using a single GPU.

Using cloud storage
-------------------
Unlike RLlib's previous stack, the new Offline RL API is cloud-agnostic and fully integrates with PyArrow. You can utilize any available cloud storage path or PyArrow-compatible filesystem. If
using a PyArrow or compatible filesystem, ensure that your ``input_`` path is a relative path within this filesystem. Similar to Ray Data, you can also use placeholders, lists of files
or folders, or simply specify a single folder to read recursively from.

For example, to read from a storage bucket in GCS, you can specify the folder location as follows:

.. dropdown:: **Code: Using cloud storage (GCS)**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_11__begin__
        :end-before: __sphinx_doc_offline_api_11__end__

This configuration allows RLlib to read data recursively from any folder beneath the specified path. If you are using a filesystem for GCS (for instance, due to authentication requirements),
use the following syntax:

.. dropdown:: **Code: Using cloud storage with PyArrow filesystem**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_12__begin__
        :end-before: __sphinx_doc_offline_api_12__end__

You can learn more about PyArrow's filesystems, particularly regarding cloud filesystems and required authentication, in `PyArrow Filesystem Interface <https://arrow.apache.org/docs/python/filesystems.html#filesystem-interface>`__.

Using cloud storage for recording
*********************************
You can use cloud storage in a similar way when recording experiences from an expert policy:

.. dropdown:: **Code: Using cloud storage for recording**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_13__begin__
        :end-before: __sphinx_doc_offline_api_13__end__

RLlib writes then directly into the folder in the cloud storage and creates it if not already existent in the bucket. The only difference to reading is that you can't use multiple paths for writing.
So something like

.. dropdown:: **Code: Invalid - multiple output paths (will not work)**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_14__begin__
        :end-before: __sphinx_doc_offline_api_14__end__

would not work. If the storage requires special permissions for creating folders and/or writing files, ensure that the cluster user is granted the necessary permissions. Failure to do so results
in denied write access, causing the recording process to stop.

.. note:: When using cloud storage, Ray Data typically streams data, meaning it's consumed in chunks. This allows postprocessing and training to begin after a brief warmup phase. More specifically, even if your cloud storage is large, the same amount of
    space isn't required on the nodes running RLlib.

.. _how-to-tune-performance:

How to tune performance
-----------------------

In RLlib's Offline RL API the various key layers are managed by distinct modules and configurations, making it non-trivial to scale these layers effectively. It's important to understand the specific parameters and their respective impact on system performance.

.. _how-to-tune-reading-operations:

How to tune reading operations
******************************
As noted earlier, the **Reading Operations** layer is automatically handled and dynamically optimized by :ref:`Ray Data <data>`. It's strongly recommended to avoid modifying this process. However, there are certain parameters that can enhance performance on this
layer to some extent, including:

#. Available resources (dedicated to the job).
#. Data locality.
#. Data sharding.
#. Data pruning.

Available resources
~~~~~~~~~~~~~~~~~~~
The scheduling strategy employed by :ref:`Ray Data <data>` operates independently of any existing placement group, scheduling tasks and actors separately. Consequently, it's essential to reserve adequate resources for other tasks and actors within your job. To
optimize :ref:`Ray Data <data>`'s scalability for read operations and improve reading performance, consider increasing the available resources in your cluster while preserving the resource allocation for existing tasks and actors. The key resources to monitor and
provision are CPUs and object store memory. Insufficient object store memory, especially under heavy backpressure, may lead to objects being spilled to disk, which can severely impact application performance.

Bandwidth is a crucial factor influencing the throughput within your cluster. In some cases, scaling the number of nodes can increase bandwidth, thereby enhancing the flow of data from storage to consuming processes. Scenarios where this approach is beneficial
include:

- Independent connections to the network backbone: Nodes utilize dedicated bandwidth, avoiding shared up-links and potential bottlenecks (see for ex. `here <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html>`__ for AWS and `here <https://cloud.google.com/compute/docs/network-bandwidth?hl=en>`__ for GCP network bandwidth documentations).
- Optimized cloud access: Employing features like `S3 Transfer Acceleration <https://aws.amazon.com/s3/transfer-acceleration/>`__, `Google Cloud Storage FUSE <https://cloud.google.com/storage/docs/cloud-storage-fuse/file-caching#configure-parallel-downloads>`__ , or parallel and accelerated data transfer methods to enhance performance.

Data locality
~~~~~~~~~~~~~
Data locality is a critical factor in achieving fast data processing. For instance, if your data resides on GCP, running a Ray cluster on AWS S3 or a local machine inevitably results in low transfer rates and slow data processing. To ensure optimal performance, storing data within the same region, same zone and cloud provider as the Ray cluster is generally
sufficient to enable efficient streaming for RLlib's Offline RL API. Additional adjustments to consider include:

- Multi-Region Buckets: Use multi-region storage to improve data availability and potentially enhance access speeds for distributed systems.
- Storage class optimization within buckets: Use **standard storage** for frequent access and low-latency streaming. Avoid archival storage classes like AWS Glacier or GCP Archive for streaming workloads due to high retrieval times.

Data sharding
~~~~~~~~~~~~~
Data sharding improves the efficiency of fetching, transferring, and reading data by balancing chunk sizes. If chunks are too large, they can cause delays during transfer and processing, leading to bottlenecks. Conversely, chunks that are too small can result in high metadata fetching overhead, slowing down overall performance. Finding an optimal chunk size is
critical for balancing these trade-offs and maximizing throughput.

- As a rule-of-thumb keep data file sizes in between 64MiB to 256MiB.

Data pruning
~~~~~~~~~~~~
If your data is in **Parquet** format (the recommended offline data format for RLlib), you can leverage data pruning to optimize performance. :ref:`Ray Data <data>` supports pruning in its :py:meth:`~ray.data.read_parquet` method through projection pushdown (column filtering) and filter pushdown (row filtering). These filters are applied directly during file
scans, reducing the amount of unnecessary data loaded into memory.

For instance, if you only require specific columns from your offline data (for example, to avoid loading the ``infos`` column):

.. dropdown:: **Code: Data pruning - column projection**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_15__begin__
        :end-before: __sphinx_doc_offline_api_15__end__

Similarly, if you only require specific rows from your dataset, you can apply pushdown filters as shown below:

.. dropdown:: **Code: Data pruning - filter pushdown**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_16__begin__
        :end-before: __sphinx_doc_offline_api_16__end__

How to tune post-processing (PreLearner)
****************************************
When enabling high throughput in Read Operations, it's essential to ensure sufficient processing capacity in the Post-Processing (Pre-Learner) stage. Insufficient capacity in this stage can cause backpressure, leading to increased memory usage and, in severe cases,
object spilling to disk or even Out-Of-Memory (see :ref:`Out-Of-Memory Prevention <ray-oom-prevention>`) errors.

Tuning the **Post-Processing (Pre-Learner)** layer is generally more straightforward than optimizing the **Read Operations** layer. Tuning the Post-Processing (Pre-Learner) layer is generally more straightforward than optimizing the Read Operations layer. The following parameters can be adjusted to optimize its performance:

- Actor Pool Size
- Allocated Resources
- Read Batch and Buffer Sizes.

Actor pool size
~~~~~~~~~~~~~~~
Internally, the **Post-Processing (PreLearner)** layer is defined by a :py:meth:`~ray.data.Dataset.map_batches` operation that starts an :py:class:`~ray.data._internal.execution.operators.actor_pool_map_operator._ActorPool`. Each actor in this pool runs an :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner`
instances to transform batches on their way from disk to RLlib's :py:class:`~ray.rllib.core.learner.learner.Learner`. Obviously, the size of this :py:class:`~ray.data._internal.execution.operators.actor_pool_map_operator._ActorPool` defines the throughput of this layer and needs to be fine-tuned in regard to the previous layer's
throughput to avoid backpressure. You can use the ``concurrency`` in RLlib's ``map_batches_kwargs`` parameter to define this pool size:

.. dropdown:: **Code: Actor pool size configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_17__begin__
        :end-before: __sphinx_doc_offline_api_17__end__

With the preceding code you would enable :ref:`Ray Data <data>` to start up to ``4`` parallel :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` actors that can post-process your data for training.

.. note:: :ref:`Ray Data <data>` dynamically adjusts its read operations based on the parallelism of your **Post-Processing (Pre-Learner)** layer. It scales read operations up or down depending on the backpressure
    in the **Post-Processing (Pre-Learner)** stage. This means the throughput of your entire streaming pipeline is determined by the performance of the downstream tasks and the resources allocated to the
    **Reading Operations** layer (see :ref:`How to tune reading operations <how-to-tune-reading-operations>`). However, due to the overhead associated with scaling reading operations up or down, backpressure - and
    in severe cases, object spilling or Out-Of-Memory (OOM) errors - can't always be entirely avoided.

You can also enable auto-scaling in your **Post-Processing (PreLearner)** by providing an interval instead of a straight number:

.. dropdown:: **Code: Actor pool auto-scaling**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_18__begin__
        :end-before: __sphinx_doc_offline_api_18__end__

This allows :ref:`Ray Data <data>` to start up to ``8`` post-processing actors to downstream data faster, for example in case of backpressure.

.. note:: Implementing an autoscaled actor pool in the **Post-Processing (Pre-Learner)** layer doesn't guarantee you the elimination of backpressure. Adding more :py:class:`~ray.rllib.offline.offline_prelearner.OffLinePreLearner` instances introduces additional overhead to the system. RLlib's offline RL pipeline is
    optimized for streaming data, which typically exhibits stable throughput and resource usage, except in cases of imbalances between upstream and downstream tasks. As a rule of thumb, consider using autoscaling only under the following conditions: (1) throughput is expected to be highly variable, (2) Cluster resources
    are subject to fluctuations (for example, in shared or dynamic environments), and/or (3) workload characteristics are highly unpredictable.

Allocated resources
~~~~~~~~~~~~~~~~~~~
Other than the number of post-processing actors you can tune performance on the **Post-Processing (PreLearner)** layer through defining resources to be allocated to each :py:class:`~ray.rllib.offline.offline_prelearner.OffLinePreLearner` in the actor pool. Such resources can be defined either through ``num_cpus`` and ``num_gpus``
or in the ``ray_remote_args``.

.. note:: Typically, increasing the number of CPUs is sufficient for performance tuning in the post-processing stage of your pipeline. GPUs are only needed in specialized cases, such as in customized pipelines. For example, RLlib’s :py:class:`~ray.rllib.algorithms.marwil.marwil.MARWIL` implementation uses the
    :py:class:`~ray.rllib.connectors.learner.general_advantage_estimation.GeneralAdvantageEstimation` connector in its :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2` to apply `General Advantage Estimation <https://arxiv.org/abs/1506.02438>`__ on experience batches. In these calculations, the value model of the algorithm's
    :py:class:`~ray.rllib.core.rl_module.RLModule` is applied, which you can accelerate by running on a GPU.

As an example, to provide each of your ``4`` :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` in the **Post-Processing (PreLearner)** ``2`` CPUs you can use the following syntax:

.. dropdown:: **Code: Allocating resources to workers**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_19__begin__
        :end-before: __sphinx_doc_offline_api_19__end__

.. warning:: Don't override the ``batch_size`` in RLlib's ``map_batches_kwargs``. This usually leads to high performance degradations. Note, this ``batch_size`` differs from the `train_batch_size_per_learner`: the former specifies the batch size in transformations of
    the streaming pipeline, while the latter defines the batch size used for training within each :py:class:`~ray.rllib.core.learner.learner.Learner` (the batch size of the actual model forward- and backward passes performed for training).

Choosing batch sizes and buffer size
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tuning the `input_read_batch_size` parameter and the `prelearner_buffer_kwargs["capacity"]` parameter provides additional optimization opportunities. 
`input_read_batch_size` controls the size of batches retrieved from data files. 
`prelearner_buffer_kwargs["capacity"]` controls the size of the buffer that stores the episodes.
Consider the following example 


You can balance ingestion throughput and training sample variation by tuning the `input_read_batch_size` and `prelearner_buffer_kwargs["capacity"]` parameters. Consider the following example: Suppose each :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` has a length of ``100`` timesteps, and your `train_batch_size_per_learner` is configured to be ``1000``.
Each :py:class:`~ray.rllib.utils.replay_buffers.episode_replay_buffer.EpisodeReplayBuffer` instance is set with a capacity of ``1000``:

.. dropdown:: **Code: Buffer size configuration (capacity=1000)**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_20__begin__
        :end-before: __sphinx_doc_offline_api_20__end__

If you configure `input_read_batch_size` to ``10`` as shown in the code, each of the ``10`` :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` fit into the buffer, enabling sampling across a wide variety of timesteps from multiple episodes. This results in high sampling variation. Now, consider the case where the buffer
capacity is reduced to ``500``:

.. dropdown:: **Code: Buffer size configuration (capacity=500)**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_21__begin__
        :end-before: __sphinx_doc_offline_api_21__end__

With the same `input_read_batch_size`, only ``5`` :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` can be buffered at a time, causing inefficiencies as more data is read than can be retained for sampling.

.. tip:: To choose an adequate `input_read_batch_size` take a look at the length of your recorded episodes. In some cases each single episode is long enough to fulfill the `train_batch_size_per_learner` and you could choose a `input_read_batch_size` of ``1``. Most times it's not and you need to consider how many episodes should be buffered to balance
    the amount of data digested from read input and the variation of data sampled from the :py:class:`~ray.rllib.utils.replay_buffers.episode_replay_buffer.EpisodeReplayBuffer` instances in the :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner`.


How to tune updating (Learner)
******************************

**Updating (Learner)**  is the final downstream task in RLlib's Offline RL pipeline, and its consumption speed determines the overall throughput of the data pipeline. If the learning process is slow, it can cause backpressure in upstream layers, potentially leading to object spilling or Out-Of-Memory (OOM) errors. Therefore, it's essential to fine-tune this
layer in coordination with the upstream components. Several parameters can be adjusted to optimize the learning speed in your Offline algorithm:

- Allocated Resources
- Batch Prefetching
- Learner Iterations.


Allocated resources
~~~~~~~~~~~~~~~~~~~
Since :py:class:`~ray.rllib.offline.offline_data.OfflineData` owns the Ray Data pipeline that reads data and inserts it into the replay buffer, it can have multiple sources of bottlenecks.
You can address such bottlenecks by allocating more resources to the respective operations that happen inside :py:class:`~ray.rllib.offline.offline_data.OfflineData`.
Take a look at :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.offline_data` for more details.

If the bottleneck is not in your ingestion pipeline, it is likely in your training pipeline.
Your main levers are then to use GPUs for training and increasing the number of :py:class:`~ray.rllib.core.learner.learner.Learner` instances.

Batch prefetching
~~~~~~~~~~~~~~~~~
Batch prefetching allows you to control data consumption on the downstream side of your offline data pipeline. The primary goal is to ensure that learners remain active, maintaining a continuous flow of data. This is achieved by preparing the next batch while the learner processes the current one. Prefetching determines how many batches are kept ready for learners
and should be tuned based on the time required to produce the next batch and the learner's update speed. Prefetching too many batches can lead to memory inefficiencies and, in some cases, backpressure in upstream tasks.

.. tip::The default in RLlib's Offline RL API is to prefetch ``2`` batches per learner instance, which works well with most tested applications.

You can configure batch prefetching in the `iter_batches_kwargs`:

.. dropdown:: **Code: Batch prefetching**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_27__begin__
        :end-before: __sphinx_doc_offline_api_27__end__

.. warning:: Don't override the ``batch_size`` in RLlib's `map_batches_kwargs`. This usually leads to high performance degradations. Note, this ``batch_size`` differs from the `train_batch_size_per_learner`: the former specifies the batch size
    in iterating over data output of the streaming pipeline, while the latter defines the batch size used for training within each :py:class:`~ray.rllib.core.learner.learner.Learner`.

Learner iterations
~~~~~~~~~~~~~~~~~~
This tuning parameter is available only when using multiple instances of ::py:class:`~ray.rllib.core.learner.learner.Learner`. In distributed learning, each :py:class:`~ray.rllib.core.learner.learner.Learner` instance processes a sub-stream of the offline streaming pipeline, iterating over batches from that sub-stream. You can control the number of iterations each
:py:class:`~ray.rllib.core.learner.learner.Learner` instance runs per RLlib training iteration. Result reporting occurs after each RLlib training iteration. Setting this parameter too low results in inefficiencies, while excessively high values can hinder training monitoring and, in some cases - such as in RLlib's :py:class:`~ray.rllib.algorithms.marwil.marwil.MARWIL`
implementation - lead to stale training data. This happens because some data transformations rely on the same :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` that the :py:class:`~ray.rllib.core.learner.learner.Learner` instances are training on. The number of iterations per sub-stream is controlled by the attribute
:py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.dataset_num_iters_per_learner`, which has a default value of ``None``, meaning it runs one epoch on the sub-stream.

You can modify this value as follows:

.. dropdown:: **Code: Learner iterations**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_28__begin__
        :end-before: __sphinx_doc_offline_api_28__end__

.. note::The default value of :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.dataset_num_iters_per_learner` is None, which allows each :py:class:`~ray.rllib.core.learner.learner.Learner` instance to process a full epoch on its data substream. While this setting works well for small datasets, it may not be suitable for larger datasets. It's important
    to tune this parameter according to the size of your dataset to ensure optimal performance.

Customization
-------------

Customization of the Offline RL components in RLlib, such as the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`, :py:class:`~ray.rllib.core.learner.learner.Learner`, or :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, follows a similar process to that of their Online RL counterparts. For detailed guidance, refer to the documentation on :ref:`Algorithms <rllib-algorithms-doc>`,
:ref:`Learners <learner-guide>`, and RLlib's :ref:`RLModule <rlmodule-guide>`. The new stack Offline RL streaming pipeline in RLlib supports customization at various levels and locations within the dataflow, allowing for tailored solutions to meet the specific requirements of your offline RL algorithm.

- Connector Level
- PreLearner Level
- Pipeline Level.

Connector level
***************
Small data transformations on instances of :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` can be easily implemented by modifying the :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2`, which is part of the :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` and prepares episodes for training. You can leverage any connector from
RLlib's library (see `RLlib's default connectors <https://github.com/ray-project/ray/tree/master/rllib/connectors>`__) or create a custom connector (see `RLlib's ConnectorV2 examples <https://github.com/ray-project/ray/tree/master/rllib/examples/connectors>`__) to integrate into the :py:class:`~ray.rllib.core.learner.learner.Learner`'s :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2`.
Careful consideration must be given to the order in which :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` instances are applied, as demonstrated in the implementation of `RLlib's MARWIL algorithm <https://github.com/ray-project/ray/tree/master/rllib/algorithms/marwil>`__ (see the `MARWIL paper <https://www.nematilab.info/bmijc/assets/012819_paper.pdf>`__).

The `MARWIL algorithm <https://github.com/ray-project/ray/tree/master/rllib/algorithms/marwil>`__ computes a loss that extends beyond behavior cloning by improving the expert's strategy during training using advantages. These advantages are calculated through `General Advantage Estimation (GAE) <https://arxiv.org/abs/1506.02438>`__ using a value model. GAE is computed on-the-fly through the
:py:class:`~ray.rllib.connectors.learner.general_advantage_estimation.GeneralAdvantageEstimation` connector. This connector has specific requirements: it processes a list of :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` instances and must be one of the final components in the :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2`. This is because
it relies on fully prepared batches containing `OBS`, `REWARDS`, `NEXT_OBS`, `TERMINATED`, and `TRUNCATED` fields. Additionally, the incoming :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` instances must already include one artificially elongated timestep.

To meet these requirements, the pipeline must include the following sequence of :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` instances:

1. :py:class:`ray.rllib.connectors.learner.add_one_ts_to_episodes_and_truncate.AddOneTsToEpisodesAndTruncate` ensures the :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects are elongated by one timestep.
2. :py:class:`ray.rllib.connectors.common.add_observations_from_episodes_to_batch.AddObservationsFromEpisodesToBatch` incorporates the observations (`OBS`) into the batch.
3. :py:class:`ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch.AddNextObservationsFromEpisodesToTrainBatch` adds the next observations (`NEXT_OBS`).
4. Finally, the :py:class:`ray.rllib.connectors.learner.general_advantage_estimation.GeneralAdvantageEstimation` connector piece is applied.

Below is the example code snippet from `RLlib's MARWIL algorithm <https://github.com/ray-project/ray/tree/master/rllib/algorithms/marwil>`__ demonstrating this setup:

.. dropdown:: **Code: MARWIL connector pipeline**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_29__begin__
        :end-before: __sphinx_doc_offline_api_29__end__

Define a primer LearnerConnector pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are multiple ways to customize the :py:class:`~ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline`. One approach, as demonstrated above, is to override the `build_learner_connector` method in the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`. Alternatively, you can directly define a custom :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` piece to the
:py:class:`~ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline` by utilizing the `learner_connector` attribute:

.. dropdown:: **Code: Custom learner connector**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_30__begin__
        :end-before: __sphinx_doc_offline_api_30__end__

As noted in the comments, this approach to adding a :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` piece to the :py:class:`~ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline` is suitable only if you intend to manipulate raw episodes, as your :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` piece serves as the foundation for building the remainder of the pipeline (including batching and other processing
steps). If your goal is to modify data further along in the :py:class:`~ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline`, you should either override the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`'s `build_learner_connector` method or consider the third option: overriding the entire :py:class:`~ray.rllib.offline.offline_prelearner.PreLearner`.

PreLearner level
****************
If you need to perform data transformations at a deeper level - before your data reaches the :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` stage - consider overriding the :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner`. This class orchestrates the complete data transformation pipeline, converting raw input data into
:py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch` objects ready for training. For instance, if your data is stored in specialized formats requiring pre-parsing and restructuring (for example, XML, HTML, Protobuf, images, or videos), you may need to handle these custom formats directly. You can leverage tools such as `Ray Data's custom datasources <custom_datasource>` (for example, :py:meth:`~ray.data.read_binary_files`) to manage the ingestion process. To ensure
this data is appropriately structured and sorted into :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects, you can override the :py:meth:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner._map_to_episodes` static method.

For more extensive customization, you can rewrite the `__call__` method to define custom transformation steps, implement a unique :py:class:`~ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline`, and construct :py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch` instances for the :py:class:`~ray.rllib.core.learner.learner.Learner`.

The following example demonstrates how to use a custom :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` to process text data and construct training batches:

.. dropdown:: **Code: Custom OfflinePreLearner with _map_to_episodes**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_32__begin__
        :end-before: __sphinx_doc_offline_api_32__end__

The preceding example illustrates the flexibility of RLlib's Offline RL API for custom data transformation. In this case, a customized :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` processes a batch of text data - organized as sentences - and converts each sentence into a :py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`. The static method returns a dictionary containing a list of these
:py:class:`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` instances. Similarly, you can extend this functionality by overriding the :py:meth:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner.__call__` method. For instance, you could implement a :py:class:`ray.rllib.connectors.learner.learner_connector_pipeline.LearnerConnectorPipeline` that stacks multiple observations (for example, tokens) together. This can be achieved using RLlib's
:py:class:`~ray.rllib.connectors.learner.frame_stacking.FrameStackingLearner` and is shown in the example below.

.. dropdown:: **Code: Full custom OfflinePreLearner with __init__ and __call__**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_33__begin__
        :end-before: __sphinx_doc_offline_api_33__end__

The ability to fully customize the :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` empowers you to design tailored data transformation workflows. This includes defining a specific learner connector pipeline and implementing raw data mapping, enabling multi-step processing of text data from its raw format to a :py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch`.

To integrate your custom :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner`, simply specify it within your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`:

.. dropdown:: **Code: Custom OfflinePreLearner configuration**
    :animate: fade-in-slide-down

    .. literalinclude:: doc_code/offline_api.py
        :language: python
        :start-after: __sphinx_doc_offline_api_31__begin__
        :end-before: __sphinx_doc_offline_api_31__end__

If these customization capabilities still don't meet your requirements, consider moving to the **Pipeline Level** for even greater flexibility.

Pipeline level
~~~~~~~~~~~~~~
On this level of RLlib's Offline RL API you can redefine your complete pipeline from data reading to batch iteration by overriding the :py:class:`~®ay.rllib.offline.offline_data.OfflineData` class. In most cases however the other two levels should be sufficient for your requirements. Manipulating the complete pipeline needs sensible handling because it could degrade performance of your
pipeline to a high degree. Study carefully the :py:class:`~ray.rllib.offline.offline_data.OfflineData` class to reach a good understanding of how the default pipeline works before going over to program your own one. There are mainly two methods that define this pipeline:

- The :py:meth:`~ray.rllib.offline.offline_data.OfflineData.__init__` method that defines the data reading process.
- The :py:meth:`~ray.rllib.offline.offline_data.OfflineData.sample` method that defines the data mapping and batch iteration.

For example consider overriding the :py:meth:`~ray.rllib.offline.offline_data.OfflineData.__init__` method, if you have some foundational data transformations as for example transforming image files into numpy arrays.

.. literalinclude:: ../../../rllib/examples/offline_rl/classes/image_offline_data.py
    :language: python

In the code example provided, you define a custom :py:class:`~ray.rllib.offline.offline_data.OfflineData` class to handle the reading and preprocessing of image data, converting it from a binary encoding format into `numpy` arrays. Additionally, you implement a custom :py:class:`~ray.rllib.offline.offline_prelearner.OfflinePreLearner` to process this data further, transforming it into a learner-ready :py:class:`~ray.rllib.policy.sample_batch.MultiAgentBatch` format.

.. literalinclude:: ../../../rllib/examples/offline_rl/classes/image_offline_prelearner.py
    :language: python

This demonstrates how the entire Offline Data Pipeline can be customized with your own logic. You can run the example by using the following code:

.. literalinclude:: ../../../rllib/examples/offline_rl/offline_rl_with_image_data.py
    :language: python

.. tip:: Consider this approach carefully: in many cases, fully transforming your data into a suitable format before engaging RLlib's offline RL API can be more efficient. For instance, in the example above, you could preprocess the entire image dataset into `numpy` arrays beforehand and utilize RLlib's default :py:class:`~ray.rllib.offline.offline_data.OfflineData` class for subsequent steps.

Monitoring
----------
To effectively monitor your offline data pipeline, leverage :ref:`Ray Data's built-in monitoring capacities <monitoring-your-workload>`. Focus on ensuring that all stages of your offline data streaming pipeline are actively processing data. Additionally, keep an eye on the Learner instance, particularly the `learner_update_timer`, which should maintain low values - around `0.02` for small models - to indicate efficient data processing and model updates.

.. note:: RLlib doesn't include :ref:`Ray Data <data>`  metrics in its results or display them in `Tensorboard` through :ref:`Ray Tune <tune-main>`'s :py:class:`~ray.tune.logger.tensorboardx.TBXLoggerCallback`. It's strongly recommended to enable the :ref:`Ray Dashboard <observability-getting-started>`, accessible at `127.0.0.1:8265`, for comprehensive monitoring and insights.

