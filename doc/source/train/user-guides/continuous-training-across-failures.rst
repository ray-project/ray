.. _train-continuous-training-across-failures:

Continuous Training Across Failures
===================================

Standard data-parallel training uses an allreduce across every worker on each
step. If a single worker dies, the collective hangs and the rest of the workers
are stuck. The naive recovery strategy is to tear down the entire worker group,
bring up a fresh one, and resume from the latest on-disk checkpoint — paying
the full cost of a restart and reload for every failure.

`torchft <https://github.com/meta-pytorch/torchft>`_ is PyTorch's open-source
implementation of per-step fault tolerance. Instead of restarting the world on
failure, torchft uses **quorum-based** training coordinated by a *lighthouse*
service: whenever membership changes (a worker dies, or new workers join for
elastic training), torchft forms a new distributed group **in place** without
restarting any of the surviving workers. Newly-joined workers recover their
model state through a direct memory transfer from a peer instead of loading
from disk.

This approach saves time along three dimensions:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Dimension
     - Benefit
   * - Checkpoint frequency
     - You only need on-disk checkpoints to survive the case where *every*
       replica dies simultaneously. Peer recovery covers the common case, so
       checkpoints can be far less frequent.
   * - Checkpoint load time
     - Recovery happens via in-memory peer transfer rather than reading a
       checkpoint from disk or object storage.
   * - Recovery latency
     - With elastic training the run can continue down a worker while a
       replacement is brought up. With fixed-size training only the failed
       worker is restarted, not the whole group.

For background, see the public
`torchft design doc <https://docs.google.com/document/d/1phjJanIPv5JSD-_B-iPp4Afa4cRV_sEBLeW6q_Z26Xc>`_,
which covers the initial design.

Why Ray Train + Ray Data + torchft
----------------------------------

**TL;DR:** Ray Train + Ray Data + torchft works well today for DDP with a
fixed number of workers. Other configurations (elastic worker counts, Ray Data
with model parallelism) are in progress.

Compared to running torchft under ``torchrun`` with a plain PyTorch
``DataLoader``, the Ray Train integration adds:

- **Fast catch-up on data state.** torchft recovers *model* state from a peer
  quickly, but it does not checkpoint the data iterator. With a plain
  ``DataLoader`` you would need to checkpoint and reload data iterator state
  every step to match. With Ray Data, the current data state lives in memory
  on a single controller, so a replacement worker can resume from the right
  position without loading any data checkpoint.
- **Single-controller ergonomics.** You declare ``num_workers`` and pass a
  ``TorchftConfig``; Ray Train owns
  the rest — placing workers, starting the lighthouse, managing restarts.

Support matrix
~~~~~~~~~~~~~~

The matrix below tracks which combinations of worker-count policy and
parallelism are supported today. Anything marked *unsupported* is planned but
not yet shipped.

.. list-table:: Worker-count policy
   :header-rows: 1
   :widths: 35 25 40

   * - Mode
     - PyTorch DataLoader
     - Ray Data
   * - **Fixed** (exactly ``N`` workers; failed workers are replaced one-for-one)
     - Supported (well-tested happy path)
     - Supported
   * - **Elastic, shrink-only** (``min_workers >= 1``, ``max_workers = N``;
       up to ``N-1`` can die and training continues)
     - Supported
     - Unsupported
   * - **Elastic, grow** (``min_workers = N``, ``max_workers = N+D``;
       when new workers join, the training group is rebuilt before resuming)
     - Supported
     - Unsupported

.. list-table:: Parallelism
   :header-rows: 1
   :widths: 35 25 40

   * - Strategy
     - PyTorch DataLoader
     - Ray Data
   * - **DDP**
     - Supported
     - Supported
   * - **Model parallelism** (e.g. tensor parallelism)
     - Supported (tensor parallelism tested; other strategies expected to work)
     - Unsupported

torchft components
------------------

To use torchft you replace several of your usual PyTorch primitives with
torchft equivalents. This section names the concepts and the user-facing
classes that implement them.

High-level concepts
~~~~~~~~~~~~~~~~~~~

The following definitions are from the torchft design doc:

- **Lighthouse** — a service that tracks the active replica groups and
  determines quorum at each step. It also lets replica groups find each
  other (hence the name). The term is borrowed from Nebula's Lighthouse.
- **Manager** — a service that runs once per replica group. It manages all
  workers in that replica group and coordinates with the lighthouse to
  decide whether the group is part of the quorum.
- **Replica group** — a group of worker nodes that makes up one slice of the
  job. Training is data-parallel across replica groups, so every rank ``n``
  holds the same slice of the model in every replica group.

.. _train-torchft-code-abstractions:

Code abstractions
~~~~~~~~~~~~~~~~~

You'll see the following classes in user code. They map onto the concepts
above:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Class
     - Role
   * - ``ProcessGroupWrapper`` (e.g. ``ProcessGroupNCCL``, ``ProcessGroupGloo``)
     - Wraps a regular process group so it can be reconfigured when the
       quorum changes. Pick the wrapper that matches your collective backend.
   * - ``Manager``
     - Creates the replica-group manager described above. Because torchft is
       SPMD, only rank 0 starts the manager *server*; the other ranks act as
       clients of that server. ``Manager`` takes a process group and
       reconfigures it on quorum changes.
   * - ``PGTransport``
     - Takes a ``ProcessGroupWrapper`` and uses it to transfer model state
       between workers when a replica needs to catch up.
   * - ``Optimizer``
     - Wraps a regular ``torch.optim`` optimizer plus a ``Manager`` to add
       fault tolerance to optimizer steps.
   * - ``DistributedDataParallel``
     - Same idea — wraps your model plus a ``Manager`` to add fault
       tolerance to the forward/backward pass.

Usage
-----

Data-parallel training with a PyTorch DataLoader (well-tested)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run DDP with Ray Train + torchft + a PyTorch ``DataLoader``, make two changes:

1. Replace your DDP primitives (model wrapper, optimizer, process group,
   sampler) with the torchft equivalents listed under
   :ref:`train-torchft-code-abstractions` above. For data loading, torchft
   adds ``DistributedSampler``, an extension of
   ``torch.utils.data.distributed.DistributedSampler`` that shards data
   across fault-tolerant replica groups.
2. Pass a ``TorchftConfig`` (from ``ray.train.v2.torch.torchft_config``)
   as the trainer's ``torch_config``. A minimal config looks like:

**TODO:** replace the inline class reference once ``TorchftConfig`` is in
the public API reference, and link to it from here.

.. literalinclude:: ../doc_code/torchft.py
    :language: python
    :start-after: __torchft_config_start__
    :end-before: __torchft_config_end__
    :dedent:

End-to-end example
^^^^^^^^^^^^^^^^^^

**TODO:** decide whether to highlight only the torchft-specific lines (via
diff or ``:emphasize-lines:``) versus the full DDP version, or show both
side-by-side.

.. literalinclude:: ../doc_code/torchft.py
    :language: python
    :start-after: __torchft_ddp_torch_dataloader_start__
    :end-before: __torchft_ddp_torch_dataloader_end__

Data-parallel training with Ray Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**TODO:** document the Ray Data + torchft DDP path, including the dataset
construction, the per-worker iterator pattern, and any ``TorchftConfig``
differences.

Benchmarking
~~~~~~~~~~~~

**TODO:** add benchmark results comparing torchft recovery time vs. a
checkpoint-restart baseline, across fixed and elastic worker counts.

Code links
~~~~~~~~~~

**TODO:** clean up and verify the runnable code links (Ray Train torchft
example, torchft repo examples, end-to-end notebooks).
