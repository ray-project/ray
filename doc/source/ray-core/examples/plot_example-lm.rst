Fault-Tolerant Fairseq Training
===============================

.. note::
    For an overview of Ray's distributed training library,
    see :ref:`Ray Train <train-docs>`.


This document provides a walkthrough of adapting the `Fairseq library <https://github.com/pytorch/fairseq>`__ to perform fault-tolerant distributed training on AWS.
As an example, we use the WikiText-103 dataset to pretrain the RoBERTa model following `this tutorial <https://github.com/pytorch/fairseq/blob/master/examples/roberta/README.pretraining.md>`__. The pipeline and configurations in this document will work for other models supported by Fairseq, such as sequence-to-sequence machine translation models.

To run this example, you will need to install Ray on your local machine to use the Ray cluster launcher.

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/source/ray-core/examples/lm


To use Ray cluster launcher on AWS, install boto (``pip install boto3``) and configure your AWS credentials in ``~/.aws/credentials`` as described on the  :ref:`Automatic Cluster Setup page <cluster-cloud>`.
We provide an `example config file <https://github.com/ray-project/ray/tree/master/doc/source/ray-core/examples/lm/lm-cluster.yaml>`__ (``lm-cluster.yaml``).

In the example config file, we use an ``m5.xlarge`` on-demand instance as the head node, and use ``p3.2xlarge`` GPU spot instances as the worker nodes. We set the minimal number of workers to 1 and maximum workers to 2 in the config, which can be modified according to your own demand.

We also mount :ref:`Amazon EFS <aws-cluster-efs>` to store code, data and checkpoints.

.. note::

  The ``{{SecurityGroupId}}`` and ``{{FileSystemId}}`` fields in the config file should be replaced by your own IDs.


In ``setup_commands``, we use the PyTorch environment in the Deep Learning AMI, and install Ray and Fairseq:

.. code-block:: yaml

    setup_commands:
        - echo 'export PATH="$HOME/anaconda3/envs/pytorch_p36/bin:$PATH"' >> ~/.bashrc;
            source ~/.bashrc;
            pip install -U ray;
            pip install -U fairseq==0.8.0;

Run the following command on your local machine to start the Ray cluster:

.. code-block:: bash

  ray up lm-cluster.yaml

``ray_train.sh`` also assumes that all of the ``lm/`` files are in ``$HOME/efs``.
You can move these files manually, or use the following command to upload
files from a local path:

.. code-block:: bash

  ray rsync-up lm-cluster.yaml PATH/TO/LM '~/efs/lm'

Preprocessing Data
------------------

Once the cluster is started, you can then SSH into the head node using ``ray attach lm-cluster.yaml`` and download or preprocess the data on EFS for training. We can run ``preprocess.sh`` (`code <https://github.com/ray-project/ray/tree/master/doc/source/ray-core/examples/lm/preprocess.sh>`_) to do this, which adapts instructions from `the RoBERTa tutorial <https://github.com/pytorch/fairseq/blob/master/examples/roberta/README.pretraining.md>`__.

Training
--------

We provide ``ray_train.py`` (`code <https://github.com/ray-project/ray/tree/master/doc/source/ray-core/examples/lm/ray_train.py>`__) as an entrypoint to the Fairseq library. Since we are training the model on spot instances, we provide fault-tolerance in ``ray_train.py`` by checkpointing and restarting when a node fails. The code will also check whether there are new resources available after checkpointing. If so, the program will make use of them by restarting and resizing.

Two main components of ``ray_train.py`` are a ``RayDistributedActor`` class and a function ``run_fault_tolerant_loop()``. The ``RayDistributedActor`` sets proper arguments for different ray actor processes, adds a checkpoint hook to enable the process to make use of new available GPUs, and calls the ``main`` of Fairseq:

.. code-block:: python

  import math
  import copy
  import socket
  import time

  import ray

  import fairseq
  from fairseq import options
  from fairseq_cli.train import main
  from contextlib import closing

  _original_save_checkpoint = fairseq.checkpoint_utils.save_checkpoint


  class RayDistributedActor:
      """Actor to perform distributed training."""

      def run(self, url, world_rank, args):
          """Runs the fairseq training.

          We set args for different ray actors for communication,
          add a checkpoint hook, and call the main function of fairseq.
          """

          # Set the init_method and rank of the process for distributed training.
          print("Ray worker at {url} rank {rank}".format(
              url=url, rank=world_rank))
          self.url = url
          self.world_rank = world_rank
          args.distributed_rank = world_rank
          args.distributed_init_method = url

          # Add a checkpoint hook to make use of new resources.
          self.add_checkpoint_hook(args)

          # Call the original main function of fairseq.
          main(args, init_distributed=(args.distributed_world_size > 1))

      def add_checkpoint_hook(self, args):
          """Add a hook to the original save_checkpoint function.

          This checks if there are new computational resources available.
          If so, raise exception to restart the training process and
          make use of the new resources.
          """

          if args.cpu:
              original_n_cpus = args.distributed_world_size

              def _new_save_checkpoint(*args, **kwargs):
                  _original_save_checkpoint(*args, **kwargs)
                  n_cpus = int(ray.cluster_resources()["CPU"])
                  if n_cpus > original_n_cpus:
                      raise Exception(
                          "New CPUs find (original %d CPUs, now %d CPUs)" %
                          (original_n_cpus, n_cpus))
          else:
              original_n_gpus = args.distributed_world_size

              def _new_save_checkpoint(*args, **kwargs):
                  _original_save_checkpoint(*args, **kwargs)
                  n_gpus = int(ray.cluster_resources().get("GPU", 0))
                  if n_gpus > original_n_gpus:
                      raise Exception(
                          "New GPUs find (original %d GPUs, now %d GPUs)" %
                          (original_n_gpus, n_gpus))

          fairseq.checkpoint_utils.save_checkpoint = _new_save_checkpoint

      def get_node_ip(self):
          """Returns the IP address of the current node."""
          return ray._private.services.get_node_ip_address()

      def find_free_port(self):
          """Finds a free port on the current node."""
          with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
              s.bind(("", 0))
              s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
              return s.getsockname()[1]


The function ``run_fault_tolerant_loop()`` provides fault-tolerance by catching failure and restart the computation:

.. code-block:: python

  def run_fault_tolerant_loop():
      """Entrance function to the fairseq library, providing fault-tolerance."""

      # Parse the command line arguments.
      parser = options.get_training_parser()
      add_ray_args(parser)
      args = options.parse_args_and_arch(parser)
      original_args = copy.deepcopy(args)

      # Main loop for fault-tolerant training.
      retry = True
      while retry:
          args = copy.deepcopy(original_args)

          # Initialize Ray.
          ray.init(address=args.ray_address)

          set_num_resources(args)
          set_batch_size(args)

          # Set up Ray distributed actors.
          Actor = ray.remote(
              num_cpus=1, num_gpus=int(not args.cpu))(RayDistributedActor)
          workers = [Actor.remote() for i in range(args.distributed_world_size)]

          # Get the IP address and a free port of actor 0, which is used for
          # fairseq distributed training.
          ip = ray.get(workers[0].get_node_ip.remote())
          port = ray.get(workers[0].find_free_port.remote())
          address = "tcp://{ip}:{port}".format(ip=ip, port=port)

          # Start the remote processes, and check whether their are any process
          # fails. If so, restart all the processes.
          unfinished = [
              worker.run.remote(address, i, args)
              for i, worker in enumerate(workers)
          ]
          try:
              while len(unfinished) > 0:
                  finished, unfinished = ray.wait(unfinished)
                  finished = ray.get(finished)
              retry = False
          except Exception as inst:
              print("Ray restart because following error occurs:")
              print(inst)
              retry = True
          ray.shutdown()

In ``ray_train.py``, we also define a set of helper functions. ``add_ray_args()`` adds Ray and fault-tolerant training related arguments to the argument parser:

.. code-block:: python

  def add_ray_args(parser):
      """Add ray and fault-tolerance related parser arguments to the parser."""
      group = parser.add_argument_group("Ray related arguments")
      group.add_argument(
          "--ray-address",
          default="auto",
          type=str,
          help="address for ray initialization")
      group.add_argument(
          "--fix-batch-size",
          default=None,
          metavar="B1,B2,...,B_N",
          type=lambda uf: options.eval_str_list(uf, type=int),
          help="fix the actual batch size (max_sentences * update_freq "
              "* n_GPUs) to be the fixed input values by adjusting update_freq "
              "accroding to actual n_GPUs; the batch size is fixed to B_i for "
              "epoch i; all epochs >N are fixed to B_N")
      return group


``set_num_resources()`` sets the distributed world size to be the number of resources. Also if we want to use GPUs but the current number of GPUs is 0, the function will wait until there is GPU available:

.. code-block:: python


  def set_num_resources(args):
      """Get the number of resources and set the corresponding fields."""
      if args.cpu:
          args.distributed_world_size = int(ray.cluster_resources()["CPU"])
      else:
          n_gpus = int(ray.cluster_resources().get("GPU", 0))
          while n_gpus == 0:
              print("No GPUs available, wait 10 seconds")
              time.sleep(10)
              n_gpus = int(ray.cluster_resources().get("GPU", 0))
          args.distributed_world_size = n_gpus



``set_batch_size()`` keeps the effective batch size to be relatively the same given different number of GPUs:

.. code-block:: python

  def set_batch_size(args):
      """Fixes the total batch_size to be agnostic to the GPU count."""
      if args.fix_batch_size is not None:
          args.update_freq = [
              math.ceil(batch_size /
                        (args.max_sentences * args.distributed_world_size))
              for batch_size in args.fix_batch_size
          ]
          print("Training on %d GPUs, max_sentences=%d, update_freq=%s" %
                (args.distributed_world_size, args.max_sentences,
                  repr(args.update_freq)))



To start training, run `following commands <https://github.com/ray-project/ray/tree/master/doc/source/ray-core/examples/lm/ray_train.sh>`__ (``ray_train.sh``) on the head machine:

.. code-block:: bash

  cd ~/efs/lm

  TOTAL_UPDATES=125000       # Total number of training steps
  WARMUP_UPDATES=10000       # Warmup the learning rate over this many updates
  PEAK_LR=0.0005             # Peak learning rate, adjust as needed
  TOKENS_PER_SAMPLE=512      # Max sequence length
  #MAX_POSITIONS=512         # Num. positional embeddings (usually same as above)
  MAX_SENTENCES=8            # Number of sequences per batch on one GPU (batch size)
  FIX_BATCH_SIZE=2048        # Number of batch size in total (max_sentences * update_freq * n_gpus)
  SAVE_INTERVAL_UPDATES=1000 # save a checkpoint every N updates

  LOG_DIR=$HOME/efs/lm/log/
  DATA_DIR=$HOME/efs/lm/data-bin/wikitext-103/
  mkdir -p $LOG_DIR

  python $HOME/efs/lm/ray_train.py --fp16 $DATA_DIR \
      --task masked_lm --criterion masked_lm \
      --arch roberta_base --sample-break-mode complete --tokens-per-sample $TOKENS_PER_SAMPLE \
      --optimizer adam --adam-betas '(0.9, 0.98)' --adam-eps 1e-6 --clip-norm 0.0 \
      --lr-scheduler polynomial_decay --lr $PEAK_LR --warmup-updates $WARMUP_UPDATES --total-num-update $TOTAL_UPDATES \
      --dropout 0.1 --attention-dropout 0.1 --weight-decay 0.01 \
      --max-sentences $MAX_SENTENCES \
      --fix-batch-size $FIX_BATCH_SIZE \
      --max-update $TOTAL_UPDATES --log-format simple --log-interval 1 \
      --save-interval-updates $SAVE_INTERVAL_UPDATES \
      --save-dir $LOG_DIR --ddp-backend=no_c10d

``SAVE_INTERVAL_UPDATES`` controls how often to save a checkpoint, which can be tuned based on the `stability of chosen instances <https://aws.amazon.com/ec2/spot/instance-advisor/>`__. ``FIX_BATCH_SIZE`` controls the total batch size to be a roughly fixed number.

Helpful Ray Commands
--------------------

To let Ray automatically stop the cluster after the training finished, you can download the ``ray_train.sh`` to ``~/efs`` of the remote machine, and run the following command on your local machine:

.. code-block:: bash

  ray exec --stop lm-cluster.yaml 'bash $HOME/efs/lm/ray_train.sh'

or run the following command on the remote head node:

.. code-block:: bash

  ray exec --stop ~/ray_bootstrap_config.yaml 'bash $HOME/efs/lm/ray_train.sh'

To test the fault-tolerance, you can run the following command on your local machine to randomly kill one node:

.. code-block:: bash

  ray kill-random-node lm-cluster.yaml
