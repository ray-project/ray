.. _executing_datasets:

==================
Executing Datasets
==================

The Datasets execution is :ref::`lazy <datasets_lazy_execution>` and is invoked in
two ways:

- :ref::`Consume the Dataset <consuming_datasets>`.
- Materialize the ``Dataset`` by calling :meth:`ds.cache() <ray.data.Dataset.cache>`.

In either case, the Dataset is executed in a streaming way. This means that Dataset
transformations will be executed incrementally on the base data, instead of on
all of the data at once. This can be used for streaming data loading into ML
training to overlap the data preprocessing and model training, or to execute
batch transformations on large datasets without needing to load the entire
dataset into cluster memory.

The following code is a hello world example which invokes the execution with
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` consumption:

.. code-block::

   import ray
   import time
        
   def sleep(x):
       time.sleep(0.1)
       return x
   
   for _ in (
       ray.data.range_tensor(5000, shape=(80, 80, 3), parallelism=200)
       .map_batches(sleep, num_cpus=2)
       .map_batches(sleep, compute=ray.data.ActorPoolStrategy(2, 4))
       .map_batches(sleep, num_cpus=1)
       .iter_batches()
   ):
       pass

This launches a simple 4-stage pipeline. We use different compute args for each stage, which forces them to be run as separate operators instead of getting fused together. You should see a log message indicating streaming execution is being used:

.. code-block::

   2023-03-30 16:40:10,076	INFO streaming_executor.py:83 -- Executing DAG InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange] -> TaskPoolMapOperator[MapBatches(sleep)] -> ActorPoolMapOperator[MapBatches(sleep)] -> TaskPoolMapOperator[MapBatches(sleep)]

The next few lines will show execution progress. Here is how to interpret the output:

.. code-block::

   Resource usage vs limits: 7.0/16.0 CPU, 0.0/0.0 GPU, 76.91 MiB/2.25 GiB object_store_memory

This line tells you how many resources are currently being used by the streaming executor out of the limits. The streaming executor will attempt to keep resource usage under the printed limits by throttling task executions.

.. code-block::

   ReadRange: 2 active, 37 queued, 7.32 MiB objects 1:  80%|████████▊  | 161/200 [00:08<00:02, 17.81it/s]
   MapBatches(sleep): 5 active, 5 queued, 18.31 MiB objects 2:  76%|██▎| 151/200 [00:08<00:02, 19.93it/s]
   MapBatches(sleep): 7 active, 2 queued, 25.64 MiB objects, 2 actors [all objects local] 3:  71%|▋| 142/
   MapBatches(sleep): 2 active, 0 queued, 7.32 MiB objects 4:  70%|██▊ | 139/200 [00:08<00:02, 23.16it/s]
   output: 2 queued 5:  70%|█████████████████████████████▉             | 139/200 [00:08<00:02, 22.76it/s]

Lines like the above show progress for each stage. The `active` count indicates the number of running tasks for the operator. The `queued` count is the number of input blocks for the operator that are computed but are not yet submitted for execution. For operators that use actor-pool execution, the number of running actors is shown as `actors`.

The final line shows how much of the stream output has been consumed by the driver program. This value can fall behind the stream execution if your program doesn't pull data from `iter_batches()` fast enough, which may lead to execution throttling.

.. tip::

    Avoid returning large outputs from the final operation of a pipeline you are iterating over, since the consumer process will be a serial bottleneck.

Configuring Resources and Locality
==================================

By default, the CPU and GPU limits are set to the cluster size, and the object store memory limit conservatively to 1/4 of the total object store size to avoid the possibility of disk spilling.

You may want to customize these limits in the following scenarios:
- If running multiple concurrent jobs on the cluster, setting lower limits can avoid resource contention between the jobs.
- If you want to fine-tune the memory limit to maximize performance.
- For data loading into training jobs, you may want to set the object store memory to a low value (e.g., 2GB) to limit resource usage.

Execution options can be configured via the global DatasetContext. The options will be applied for future jobs launched in the process:

.. code-block::

   ctx = ray.data.context.DatasetContext.get_current()
   ctx.execution_options.resource_limits.cpu = 10
   ctx.execution_options.resource_limits.gpu = 5
   ctx.execution_options.resource_limits.object_store_memory = 10e9

Deterministic Execution
=======================

.. code-block::

   ctx.execution_options.preserve_order = True

To enable deterministic execution, set the above to True. This may decrease performance, but will ensure block ordering is preserved through execution. This flag defaults to True in Ray 2.3 but False in the nightly builds

Actor Locality Optimization (ML inference use case)
===================================================

.. code-block::

   ctx.execution_options.actor_locality_enabled = True

The actor locality optimization (if you're using actor pools) tries to schedule objects that are already local to an actor's node to the same actor. This reduces network traffic across nodes. When actor locality is enabled, you'll see a report in the progress output of the hit rate:

.. code-block::

   MapBatches(Model): 0 active, 0 queued, 0 actors [992 locality hits, 8 misses]: 100%|██████████| 1000/1000 [00:59<00:00, 16.84it/s]

Locality with Output (ML ingest use case)
=========================================

.. code-block::

   ctx.execution_options.locality_with_output = True

Setting this to True tells Datasets to prefer placing operator tasks onto the consumer node in the cluster, rather than spreading them evenly across the cluster. This can be useful if you know you'll be consuming the output data directly on the consumer node (i.e., for ML training ingest). However, this may incur a performance penalty for other use cases.


Scalability
===========
We expect the data streaming backend to scale to tens of thousands of files / blocks and up to hundreds of terabytes of data. Please report if you experience performance degradation at these scales, we would be very interested to investigate!
