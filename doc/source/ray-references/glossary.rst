.. _ray_glossary:

Ray Glossary
============

On this page you find a list of important terminology used throughout the Ray
documentation, sorted alphabetically.

.. glossary::

    Action space
        Property of an RL environment. The shape(s) and datatype(s) that actions within
        an RL environment are allowed to have.
        Examples: An RL environment, in which an agent can move up, down, left,
        or right might have an action space of ``Discrete(4)`` (integer values
        of 0, 1, 2, or 3).
        An RL environment, in which an agent can apply a torque between
        -1.0 and 1.0 to a joint, the action space might be
        ``Box(-1.0, 1.0, (1,), float32)`` (single float values between -1.0 and 1.0).

    Actor
        A Ray actor is a remote instance of a class, which is
        essentially a stateful service. :ref:`Learn more about Ray actors<actor-guide>`.

    Actor task
        An invocation of an Ray actor method. Sometimes we just call it a task.

    Ray Agent
        Daemon process running on each Ray node. It has several functionalities like
        collecting metrics on the local node and installing runtime environments.

    Agent
        An acting entity inside an RL environment. One RL environment might contain
        one (single-agent RL) or more (multi-agent RL) acting agents. Different agents
        within the same environment might have different observation- and action-spaces,
        different reward functions, and act at different time-steps.

    Algorithm
        A class that holds the who/when/where/how for training one or more RL agent(s).
        The user interacts with an Algorithm instance directly to train their agents
        (it is the top-most user facing API or RLlib).

    Asynchronous execution
        An execution model where a later task can begin executing in parallel,
        without waiting for an earlier task to finish.
        Ray tasks and actor tasks are all executed asynchronously.

    Asynchronous sampling
        Sampling is the process of rolling out (playing) episodes within an RL
        environment and thereby collecting the training data (observations, actions
        and rewards). In an asynchronous sampling setup, Ray actors run sampling in the
        background and send collected samples back to a main driver script. The driver
        checks for such “ready” data frequently and then triggers central model
        learning updates. Hence, sampling and learning happen at the same time.
        Note that because of this, the policy/ies used for creating the samples
        (action computations) might be slightly behind the centrally learned policy
        model(s), even in an on-policy Algorithm.

    Autoscaler
        A Ray component that scales up and down the Ray cluster by adding and removing
        Ray nodes according to the resources requested by applications running on
        the cluster.

    Autoscaling
        The process of scaling up and down the Ray cluster automatically.

    Backend
        A class containing the initialization and teardown logic for a specific deep
        learning framework (eg. Torch, TensorFlow), used to set up distributed
        data-parallel training for :ref:`Ray Train’s built-in trainers<train-api>`.

    Batch format
        The way Ray Data represents batches of data.

        Set ``batch_format`` in methods like
        :meth:`Dataset.iter_batches() <ray.data.Dataset.iter_batches>` and
        :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>` to specify the
        batch type.

        .. doctest::

            >>> import ray
            >>> dataset = ray.data.range(10)
            >>> next(iter(dataset.iter_batches(batch_format="numpy", batch_size=5)))
            {'id': array([0, 1, 2, 3, 4])}
            >>> next(iter(dataset.iter_batches(batch_format="pandas", batch_size=5)))
               id
            0   0
            1   1
            2   2
            3   3
            4   4

        To learn more about batch formats, read
        :ref:`Configuring batch formats <configure_batch_format>`.

    Batch size
        A batch size in the context of model training is the number of data points used
        to compute and apply one gradient update to the model weights.

    Block
        A processing unit of data. A :class:`~ray.data.Dataset` consists of a
        collection of blocks.

        Under the hood, Ray Data partitions rows into a set of distributed data blocks.
        This allows it to perform operations in parallel.

        Unlike a batch, which is a user-facing object, a block is an internal abstraction.

    Placement Group Bundle
        A collection of resources that must be reserved on a single Ray node.
        :ref:`Learn more<ray-placement-group-doc-ref>`.

    Checkpoint
        A Ray Train Checkpoint is a common interface for accessing data and models across
        different Ray components and libraries. A Checkpoint can have its data
        represented as a directory on local (on-disk) storage, as a directory on an
        external storage (e.g., cloud storage), and as an in-memory dictionary.
        :ref:`Learn more<checkpoint-api-ref>`,

        .. TODO: How does this relate to RLlib checkpoints etc.? Be clear here

    Ray Client
        The Ray Client is an API that connects a Python script to a remote Ray cluster.
        Effectively, it allows you to leverage a remote Ray cluster just like you would
        with Ray running on your local machine.
        :ref:`Learn more<ray-client-ref>`.

    Ray Cluster
        A Ray cluster is a set of worker nodes connected to a common Ray head node.
        Ray clusters can be fixed-size, or they can autoscale up and down according to
        the resources requested by applications running on the cluster.

    .. TODO: Add "Concurrency" here, or try to avoid this in docs.

    Connector
        A connector performs transformations on data that comes out of a dataset or an
        RL environment and is about to be passed to a model. Connectors are flexible
        components and can be swapped out such that models are easily reusable and do
        not have to be retrained for different data transformations.

    Tune Config
        This is the set of hyperparameters corresponding to a Tune trial.
        Sampling from a hyperparameter search space will produce a config.

    .. TODO: DAG

    Ray Dashboard
        Ray’s built-in dashboard is a web interface that provides metrics, charts,
        and other features that help Ray users to understand and debug Ray applications.

    .. TODO: Data Shuffling

    Dataset (object)
        A class that produces a sequence of distributed data blocks.

        :class:`~ray.data.Dataset` exposes methods to read, transform, and consume data at scale.

        To learn more about Datasets and the operations they support, read the :ref:`Datasets API Reference <data-api>`.

    Deployment
        A deployment is a group of actors that can handle traffic in Ray Serve.
        Deployments are defined as a single class with a number of options, including
        the number of “replicas” of the deployment, each of which will map to a Ray
        actor at runtime. Requests to a deployment are load balanced across its replicas.

    .. TODO: Deployment pipeline

    Deployment graph
        A deployment graph is a group of Ray Serve deployments that are bound together
        into a directed acyclic graph (DAG) to handle requests. This enables model
        composition. Each request will be passed through the graph, allowing multiple
        stages of processing. For example, there might be a different deployment for
        preprocessing, inference, and postprocessing.

    Ingress Deployment
        The “ingress” deployment is the one that receives and responds to inbound user
        traffic. It handles HTTP parsing and response formatting. In the case of a
        deployment graph, it would also fan out requests to other deployments to do
        things like a forward pass of an ML model.

    Driver
        "Driver" is the name of the process running the main script that starts all
        other processes. For Python, this is usually the script you start with
        ``python ...``.

    Tune Driver
        The Tune driver is the main event loop that’s happening on the node that
        launched the Tune experiment. This event loop schedules trials given the
        cluster resources, executes training on remote Trainable actors, and processes
        results and checkpoints from those actors.

    Distributed Data-Parallel
        A distributed data-parallel (DDP) training job scales machine learning training
        to happen on multiple nodes, where each node processes one shard of the full
        dataset. Every worker holds a copy of the model weights, and a common strategy
        for updating weights is a “mirrored strategy”, where each worker will hold the
        exact same weights at all times, and computed gradients are averaged then
        applied across all workers.

        With N worker nodes and a dataset of size D, each worker is responsible for
        only ``D / N`` datapoints. If each worker node computes the gradient on a batch
        of size ``B``, then the effective batch size of the DDP training is ``N * B``.

    .. TODO: Entrypoint

    Environment
        The world or simulation, in which one or more reinforcement learning agents
        have to learn to behave optimally in wrt. a given reward function. An
        environment consists of an observation space, a reward function, an action
        space, a state transition function, and a distribution over initial states
        (after a reset).

        Episodes consisting of one or more time-steps are played through an
        environment in order to generate and collect samples for learning.
        These samples contain one 4-tuple of
        ``[observation, action, reward, next observation]`` per timestep.

    Episode
        A series of subsequent RL environment timesteps, each of which is a
        4-tuple: ``[observation, action, reward, next observation]``.
        Episodes can end with the terminated- or truncated-flags being True.
        An episode generally spans multiple time-steps for one or more agents.
        The Episode is an important concept in RL as "optimal agent behavior" is
        defined as choosing actions that maximize the sum of individual rewards
        over the course of an episode.

    Trial Executor
        An internal :ref:`Ray Tune component<raytrialexecutor-docstring>` that manages
        the resource management and execution of each trial’s corresponding remote
        Trainable actor. The trial  executor’s responsibilities include launching
        training, checkpointing, and restoring remote tasks.

    Experiment
        A Ray Tune or Ray Train experiment is a collection of one or more training jobs
        that may correspond to different hyperparameter configurations. These
        experiments are launched via the
        :ref:`Tuner API<tune-run-ref>` and the :ref:`Trainer API<train-api>`.

    .. TODO: Event

    Fault tolerance
        Fault tolerance in Ray Train and Tune consists of experiment-level and trial-level
        restoration. Experiment-level restoration refers to resuming all trials,
        in the event that an experiment is interrupted in the middle of training due
        to a cluster-level failure. Trial-level restoration refers to resuming
        individual trials, in the event that a trial encounters a runtime
        error such as OOM.

        .. TODO: more on fault tolerance in Core

    Framework
        The deep-learning framework used for the model(s), loss(es), and optimizer(s)
        inside an RLlib Algorithm. RLlib currently supports PyTorch and TensorFlow.

    GCS / Global Control Service
        Centralized metadata server for a Ray cluster. It runs on the Ray head node
        and has functions like managing node membership and actor directory.
        It’s also known as the Global Control Store.

    Head node
        A node that runs extra cluster-level processes like GCS and API server in
        addition to those processes running on a worker node. A Ray cluster only has
        one head node.

    HPO
        Hyperparameter optimization (HPO) is the process of choosing a set of optimal
        hyperparameters for a learning algorithm. A hyperparameter can be a parameter
        whose value is used to control the learning process (e.g., learning rate),
        define the model architecture (e.g, number of hidden layers), or influence data
        pre-processing. In the case of Ray AIR, hyperparameters can also include
        compute processing scale-out parameters such as the number of distributed
        training workers.

    .. TODO: Inference

    Job
        A ray job is a packaged ray application that can be executed on a
        (remote) Ray cluster. :ref:`Learn more<jobs-overview>`.

    Lineage
        For Ray objects, this is the set of tasks that was originally executed to
        produce the object. If an object’s value is lost due to node failure,
        Ray may attempt to recover the value by re-executing the object’s lineage.

    .. TODO: Logs

    .. TODO: Metrics

    Model
        A function approximator with trainable parameters (e.g. a neural network) that
        can be trained by an algorithm on available data or collected data from an RL
        environment. The parameters are usually initialized at random (unlearned state).
        During the training process, checkpoints of the model can be created such that -
        after the learning process is shut down or crashes - training can resume from
        the latest weights rather than having to re-learn from scratch.
        After the training process is completed, models can be deployed into production
        for inference using Ray Serve.

    Multi-agent
        Denotes an RL environment setup, in which several (more than one) agents act
        in the same environment and learn either the same or different optimal
        behaviors. The relationship between the different agents in a multi-agent setup
        might be adversarial (playing against each other), cooperative (trying to reach
        a common goal) or neutral (the agents don’t really care about other agents’
        actions). The NN model architectures that can be used for multi-agent training
        range from "independent" (each agent trains its own separate model), over
        "partially shared" (i.e. some agents might share their value function, because
        they have a common goal), to "identical" (all agents train on the same model).

    Namespace
        A namespace is a logical grouping of jobs and named actors. When an actor is
        named, its name must be unique within the namespace.
        When a namespace is not specified, Ray will place your job in an anonymous
        namespace.

    Node
        A Ray node is a physical or virtual machine that is part of a Ray cluster.
        See also :term:`Head node`.

    Object
        An application value. These are values that are returned by a task or
        created through ``ray.put``.

    Object ownership
        Ownership is the concept used to decide where metadata for a certain
        ``ObjectRef`` (and the task that creates the value) should be stored.
        If a worker calls ``foo.remote()`` or ``ray.put()``, it owns the metadata for
        the returned ``ObjectRef``, e.g., ref count and location information. If an
        object’s owner dies and another worker tries to get the value,
        it will receive an ``OwnerDiedError`` exception.

    Object reference
        A pointer to an application value, which can be stored anywhere in the cluster.
        Can be created by calling ``foo.remote()`` or ``ray.put()``.
        If using ``foo.remote()``, then the returned ``ObjectRef`` is also a future.

    Object store
        A distributed in-memory data store for storing Ray objects.

    Object spilling
        Objects in the object store are spilled to external storage once the capacity
        of the object store is used up. This enables out-of-core data processing for
        memory-intensive distributed applications. It comes with a performance penalty
        since data needs to be written to disk.

    .. TODO: Observability

    Observation
        The full or partial state of an RL environment, which an agent sees
        (has access to) at each timestep. A fully observable environment produces
        observations that contain all the information to sufficiently infer the current
        underlying state of the environment. Such states are also called “Markovian”.
        Examples for environments with Markovian observations are chess or 2D games,
        in which the player can see with each frame the entirety of the game’s state.
        A partially observable (or non-Markovian) environment produces observations
        that do not contain sufficient information to infer the exact underlying state.
        An example here would be a robot with a camera on its head facing forward.
        The robot walks around in a maze, but from a single camera frame might not know
        what’s currently behind it.

    Offline data
        Data collected in an RL environment up-front and stored in some data format
        (e.g. JSON). Offline data can be used to train an RL agent. The data might have
        been generated by a non-RL/ML system, such as a simple decision making script.
        Also, when training from offline data, the RL algorithm will not be able to
        explore new actions in new situations as all interactions with the environment
        already happened in the past (were recorded prior to training).

    Offline RL
        A sub-field of reinforcement learning (RL), in which specialized offline
        RL Algorithms learn how to compute optimal actions for an agent inside an
        environment without the ability to interact live with that environment.
        Instead, the data used for training has already been collected up-front
        (maybe even by a non-RL/ML system). This is very similar to a supervised
        learning setup. Examples for offline RL algorithms are MARWIL, CQL, and CRR.

    Off-Policy
        A type of RL Algorithm. In an off-policy algorithm, the policy used to compute
        the actions inside an RL environment (to generate the training data) might be
        different from the one that is being optimized. Examples for off-policy
        Algorithms are DQN, SAC, and DDPG.

    On-Policy
        A type of RL Algorithm. In an on-policy algorithm, the policy used to compute
        the actions inside an RL environment (to generate the training data) must be the
        exact same (matching NN weights at all times) than the one that is being
        optimized. Examples for on-policy Algorithms are PPO, APPO, and IMPALA.

    OOM (Out of Memory)
        Ray may run out of memory if the application is using too much memory on a
        single node. In this case the :ref:`Ray OOM killer<oom-questions>` will kick
        in and kill worker processes to free up memory.

    Placement group
        Placement groups allow users to atomically reserve groups of resources across
        multiple nodes (i.e., gang scheduling). They can be then used to schedule Ray
        tasks and actors packed as close as possible for locality (PACK), or spread
        apart (SPREAD). Placement groups are generally used for gang-scheduling actors,
        but also support tasks.
        :ref:`Learn more<ray-placement-group-doc-ref>`.

    Policy
        A (neural network) model that maps an RL environment observation of some agent
        to its next action inside an RL environment.

    .. TODO: Policy evaluation

    Predictor
        :class:`An interface for performing inference<ray.train.predictor.Predictor>` (prediction)
        on input data with a trained model.

    Preprocessor
        :ref:`An interface used to preprocess a Dataset<air-preprocessor-ref>` for
        training and inference (prediction). Preprocessors
        can be stateful, as they can be fitted on the training dataset before being
        used to transform the training and evaluation datasets.

    .. TODO: Process

    Ray application
        A collection of Ray tasks, actors, and objects that originate from the
        same script.

    .. TODO: Ray Timeline

    Raylet
        A system process that runs on each Ray node. It’s responsible for scheduling
        and object management.

    Replica
        A replica is a single actor that handles requests to a given Serve deployment.
        A deployment may consist of many replicas, either statically-configured via
        ``num_replicas`` or dynamically configured using auto-scaling.

    Resource (logical and physical)
        Ray resources are logical resources (e.g. CPU, GPU) used by tasks and actors.
        It doesn't necessarily map 1-to-1 to physical resources of machines on which
        Ray cluster runs. :ref:`Learn more<core-resources>`.

    Reward
        A single floating point value that each agent within an RL environment receives
        after each action taken. An agent is defined to be acting optimally inside the
        RL environment when the sum over all received rewards within an episode is
        maximized.

        Note that rewards might be delayed (not immediately telling the agent, whether
        an action was good or bad) or sparse (often have a value of zero) making it
        harder for the agent to learn.

    Rollout
        The process of advancing through an episode in an RL environment (with one or
        more RL agents) by taking sequential actions. During rollouts, the algorithm
        should collect the environment produced 4-tuples [observations, actions,
        rewards, next observations] in order to (later or simultaneously) learn how to
        behave more optimally from this data.

    Rollout Worker
        Component within a RLlib Algorithm responsible for advancing and collecting
        observations and rewards in an RL environment. Actions for the different
        agent(s) within the environment are computed by the Algorithms’ policy models.
        A distributed algorithm might have several replicas of Rollout Workers running
        as Ray actors in order to scale the data collection process for faster RL
        training.

        .. START ROLLOUT WORKER

        RolloutWorkers are used as ``@ray.remote`` actors to collect and return samples
        from environments or offline files in parallel. An RLlib
        :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` usually has
        ``num_workers`` :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s plus a
        single "local" :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` (not ``@ray.remote``) in
        its :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` under ``self.workers``.

        Depending on its evaluation config settings, an additional
        :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` with
        :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s for evaluation may be present in the
        :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
        under ``self.evaluation_workers``.

        .. END ROLLOUT WORKER

    .. TODO: Runtime

    Runtime environment
        A runtime environment defines dependencies such as files, packages, environment
        variables needed for a Python script to run. It is installed dynamically on the
        cluster at runtime, and can be specified for a Ray job, or for specific actors
        and tasks. :ref:`Learn more<handling_dependencies>`.

    Remote Function
        See :term:`Task`.

    Remote Class
        See :term:`Actor`.

    (Ray) Scheduler
        A Ray component that assigns execution units (Task/Actor) to Ray nodes.

    Search Space
        The definition of the possible values for hyperparameters. Can be composed out
        of constants, discrete values, distributions of functions. This is also
        referred to as the “parameter space” (``param_space`` in the ``Tuner``).

    Search algorithm
        Search algorithms suggest new hyperparameter configurations to be evaluated
        by Tune. The default search algorithm is random search, where each new
        configuration is independent from the previous one. More sophisticated search
        algorithms such as ones using Bayesian optimization will fit a model to predict
        the hyperparameter configuration that will produce the best model, while also
        exploring the space of possible hyperparameters. Many popular search algorithms
        are built into Tune, most of which are integrations with other libraries.

    Serve application
        An application is the unit of upgrade in a Serve cluster.

        An application consists of one or more deployments. One of these deployments
        is considered the “ingress” deployment, which is where all inbound
        traffic is handled.

        Applications can be called via HTTP at their configured ``route_prefix``.

    ServeHandle
        ServeHandle is the Python API for making requests to Serve deployments. A
        handle is defined by passing one bound Serve deployment to the constructor of
        another. Then at runtime that reference can be used to make requests. This is
        used to combine multiple deployments into “deployment graphs.”

    Session
        - A Ray Train/Tune session: Tune session at the experiment execution layer
          and Train session at the Data Parallel training layer
          if running data-parallel distributed training with Ray Train.

          The session allows access to metadata, such as which trial is being run,
          information about the total number of workers, as well as the rank of the
          current worker. The session is also the interface through which an individual
          Trainable can interact with the Tune experiment as a whole. This includes uses
          such as reporting an individual trial’s metrics, saving/loading checkpoints,
          and retrieving the corresponding dataset shards for each Train worker.

        - A Ray cluster: in some cases the session also means a :term:`Ray Cluster`.
          For example, logs of a Ray cluster are stored under ``session_xxx/logs/``.

    Spillback
        A task caller schedules a task by first sending a resource request to the
        preferred raylet for that request. If the preferred raylet chooses not to grant
        the resources locally, it may also “Spillback” and respond to the caller with
        the address of a remote raylet at which the caller should retry the resource
        request.

    State
        State of the environment an RL agent interacts with.

    Synchronous execution
        Two tasks A and B are executed synchronously if A must finish before B can
        start. For example, if you call ``ray.get`` immediately after launching a remote
        task with ``task.remote()``, you’ll be running with synchronous execution,
        since this will wait for the task to finish before the program continues.

    Synchronous sampling
        Sampling workers work in synchronous steps. All of them must finish collecting
        a new batch of samples before training can proceed to the next iteration.

    Task
        A remote function invocation. This is a single function invocation that
        executes on a process different from the caller, and potentially on a different
        machine. A task can be stateless (a ``@ray.remote`` function) or stateful (a
        method of a ``@ray.remote`` class - see Actor below). A task is executed
        asynchronously with the caller: the ``.remote()`` call immediately returns
        one or more ``ObjectRefs`` (futures) that can be used to retrieve the
        return value(s). See :term:`Actor task`.

    Trainable
        A :ref:`Trainable<trainable-docs>` is the interface that Ray Tune uses to
        perform custom training
        logic. User-defined Trainables take in a configuration as an input and can
        run user-defined training code as well as custom metric reporting and
        checkpointing.

        There are many types of trainables. Most commonly used is the function
        trainable API, which is simply a Python function that contains model training
        logic and metric reporting. Tune also exposes a class trainable API, which
        allows you to implement training, checkpointing, and restoring as different
        methods.

        Ray Tune associates each trial with its own Trainable – the Trainable is the
        one actually doing training. The Trainable is a remote actor that can be placed
        on any node in a Ray cluster.

    Trainer
        A Trainer is the top-level API to configure a single distributed training job.
        :ref:`There are built-in Trainers for different frameworks<air-trainer-ref>`,
        like PyTorch, Tensorflow, and XGBoost. Each trainer shares a common interface
        and otherwise defines framework-specific configurations and entrypoints. The
        main job of a trainer is to coordinate N distributed training workers and set
        up the communication backends necessary for these workers to communicate
        (e.g., for sharing computed gradients).

    Trainer configuration
        A Trainer can be configured in various ways. Some
        configurations are shared across all trainers, like the RunConfig, which
        configures things like the experiment storage, and ScalingConfig, which
        configures the number of training workers as well as resources needed per
        worker. Other configurations are specific to the trainer framework.

    Training iteration
        A partial training pass of input data up to pre-defined yield point
        (e.g., time or data consumed) for checkpointing of long running training jobs.
        A full training epoch can consist of multiple training iterations.
        .. TODO: RLlib

    Training epoch
        A full training pass of the input dataset. Typically, model training iterates
        through the full dataset in batches of size B, where gradients are calculated
        on each batch and then applied as an update to the model weights. Training
        jobs can consist of multiple epochs by training through the same dataset
        multiple times.

    Training step
        An RLlib-specific method of the Algorithm class which includes the core logic
        of an RL algorithm. Commonly includes gathering of experiences (either through
        sampling or from offline data), optimization steps, redistribution of learnt
        model weights. The particularities of this method are specific to algorithms
        and configurations.

    Transition
        A tuple of (observation, action, reward, next observation). A transition
        represents one step of an agent in an environment.

    Trial
        One training run within a Ray Tune experiment. If you run multiple trials,
        each trial usually corresponds to a different config (a set of hyperparameters).

    Trial scheduler
        When running a Ray Tune job, the scheduler will decide how to allocate
        resources to trials. In the most common case, this resource is time - the trial
        scheduler decides which trials to run at what time. Certain built-in schedulers
        like Asynchronous Hyperband (ASHA) perform early stopping of under-performing
        trials, while others like Population Based Training (PBT) will make
        under-performing trials copy the hyperparameter config and model weights of
        top performing trials and continue training.

    Tuner
        The Tuner is the top level Ray Tune API used to configure and run an
        experiment with many trials.

    .. TODO: Tunable

    .. TODO: (Ray) Workflow

    .. TODO: WorkerGroup

    .. TODO: Worker heap

    .. TODO: Worker node / worker node pod

    Worker process / worker
        The process that runs user defined tasks and actors.
