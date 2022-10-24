Ray Glossary
============

On this page you find a list of important terminology used throughout the Ray 
documentation.

.. glossary::

    Action space
        TODO

    Actor
        TODO

    Actor pool
        TODO

    Actor task
        TODO

    Agent
        TODO

    Algorithm
        TODO

    Asynchronous execution
        TODO

    Autoscaler / Autoscaling
        TODO

    Backend
        TODO

    Batch predictor
        TODO

    (Placement Group) Bundle
        TODO

    Checkpoint
        TODO

    (Ray) Client
        TODO

    (Ray) Cluster
        TODO

    Concurrency
        TODO

    DAG
        TODO

    (Ray) Dashboard
        TODO

    Data Shuffling
        TODO

    Datasets
        TODO

    Dataset pipeline
        TODO

    Deployment
        TODO

    Deployment pipeline
        TODO

    Deployment graph
        TODO

    Driver
        TODO

    (Ray) Ecosystem
        TODO

    Entrypoint
        TODO

    Environment
        TODO

    Episode
        TODO

    Executor
        TODO

    Experiment
        TODO

    Event
        TODO

    Fault tolerance
        TODO

    GCS / Global control store
        TODO

    Generator
        TODO

    Head node / head node pod
        TODO

    Host
        TODO

    HPO
        TODO

    (Ray) Integration
        TODO

    Job
        TODO

    Lineage
        TODO

    Logs
        TODO

    Namespace
        TODO

    Node
        TODO

    Object
        TODO

    Object ownership
        TODO

    Object reference
        TODO

    Object store / Plasma store
        TODO

    Object spilling
        TODO

    Observability
        TODO

    Observation
        TODO

    OOM / Out of memory
        TODO

    Parallelism
        TODO

    Pattern and anti-pattern
        TODO

    Pipeline/pipelining
        TODO

    Placement group
        TODO

    Policy
        TODO

    Policy evaluation
        TODO

    Predictor
        TODO

    Preprocessor
        TODO

    Process
        TODO

    Ray application
        TODO

    Ray Timeline
        TODO

    Raylet
        TODO

    Replica
        TODO

    Resource / logical resource / physical resources (CPU, GPU, etc.)
        TODO

    Reward
        TODO

    Rollout
        TODO



    Rollout Worker
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

    Runtime
        TODO

    Runtime environment
        TODO

    (Ray) Scheduler
        TODO

    Search Space
        TODO

    Search algorithm
        TODO

    Serve application
        TODO

    ServeHandle
        TODO

    Session
        TODO

    State
        TODO

    Synchronous execution
        TODO

    Task
        TODO

    Trainable
        TODO

    Trainer
        TODO

    Trainer configuration
        TODO

    Training iteration
        TODO

    Training step
        TODO

    Trial
        TODO

    Trial scheduler
        TODO

    Tuner
        TODO

    Tunable
        TODO

    UDF
        TODO

    (Ray) Workflow
        TODO

    WorkerGroup
        TODO

    Worker heap
        TODO

    Worker node / worker node pod
        TODO

    Worker process / worker
        TODO
