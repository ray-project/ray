Training Language Model on Cloud with Fairseq and Ray Autoscaler
================================================================

This document provides a walkthrough of using Ray Autoscaler with the `Fairseq library <https://github.com/pytorch/fairseq>`__ to train language models on AWS spot instances. As an example, we use the WikiText-103 dataset to pretrain the RoBERTa model following `this tutorial <https://github.com/pytorch/fairseq/blob/master/examples/roberta/README.pretraining.md>`__. The pipeline and configurations in this document will work for other models supported by Fairseq, such as sequence-to-sequence machine translation models.

To run this example, you will need to install Ray on your local machine to use Ray Autoscaler. 

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/examples/lm


To use Ray Autoscaler on AWS, install boto (``pip install boto3``) and configure your AWS credentials in ``~/.aws/credentials`` as described on  `Automatic Cluster Setup page <autoscaling.html>`__. We provide an `example config file <https://github.com/ray-project/ray/tree/master/doc/examples/lm/lm-cluster.yaml>`__ (``lm-cluster.yaml``).

In this config file, we use an ``m5.xlarge`` on-demand instance as the head node, and use ``p3.2xlarge`` GPU spot instances as the worker nodes. We set the minimal number of workers to 1 and maximum workers to 2 in the config, which can be modified according to your own demand.

We also mount an `Amazon EFS <https://aws.amazon.com/efs/>`__ to store code, data and checkpoints. 

.. note::

  The ``{{SecurityGroupId}}`` and ``{{FileSystemId}}`` fileds in the config file should be replaced by your own IDs.


In ``setup_commands``, we use the PyTorch environment in the Deep Learning AMI, and install Ray and Fairseq with

.. code-block:: bash

  echo 'export PATH="$HOME/anaconda3/envs/pytorch_p36/bin:$PATH"' >> ~/.bashrc;
  source ~/.bashrc;
  pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev3-cp36-cp36m-manylinux1_x86_64.whl;
  pip install -U fairseq==0.8.0;

If you want to build Fairseq from the source, replace the ``pip install -U fairseq==0.8.0`` with

.. code-block:: bash

  git clone https://github.com/pytorch/fairseq;
  cd fairseq;
  pip install --editable .;

We also install EFS utilities and mount the EFS in ``setup_commands``:

.. code-block:: bash

  sudo kill -9 `sudo lsof /var/lib/dpkg/lock-frontend | awk '{print $2}' | tail -n 1`; 
  sudo pkill -9 apt-get; 
  sudo pkill -9 dpkg; 
  sudo dpkg --configure -a; 
  sudo apt-get -y install binutils; 
  cd $HOME; 
  git clone https://github.com/aws/efs-utils; 
  cd $HOME/efs-utils; 
  ./build-deb.sh; 
  sudo apt-get -y install ./build/amazon-efs-utils*deb; 
  cd $HOME; 
  mkdir efs; 
  sudo mount -t efs {{FileSystemId}}:/ efs; 
  sudo chmod 777 efs;


.. note::

  You need to replace the ``{{FileSystemId}}`` to your own EFS ID (and correspondingly set the ``{{SecurityGroupId}}``) before using the conifg.

The following parts of config will also start Ray server on the head node.

Running the following command on your local machine to start the Ray cluster:

.. code-block:: bash

  ray up lm-cluster.yaml

Once the cluster is started, you can then SSH into the head node using ``ray attach lm-cluster.yaml`` and download or preprocess the data on EFS for training. Following `the RoBERTa tutorial <https://github.com/pytorch/fairseq/blob/master/examples/roberta/README.pretraining.md>`__, we run the following commands to preprocess the dataset:

.. code-block:: bash

  cd ~/efs

  # download the dataset
  wget https://s3.amazonaws.com/research.metamind.io/wikitext/wikitext-103-raw-v1.zip
  unzip wikitext-103-raw-v1.zip
  
  # encode it with the GPT-2 BPE
  mkdir -p gpt2_bpe
  wget -O gpt2_bpe/encoder.json https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/encoder.json
  wget -O gpt2_bpe/vocab.bpe https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/vocab.bpe
  wget https://raw.githubusercontent.com/pytorch/fairseq/master/examples/roberta/multiprocessing_bpe_encoder.py
  for SPLIT in train valid test; do \
      python multiprocessing_bpe_encoder.py \
          --encoder-json gpt2_bpe/encoder.json \
          --vocab-bpe gpt2_bpe/vocab.bpe \
          --inputs wikitext-103-raw/wiki.${SPLIT}.raw \
          --outputs wikitext-103-raw/wiki.${SPLIT}.bpe \
          --keep-empty \
          --workers 60; \
  done
  
  # preprocess/binarize the data using the GPT-2 fairseq dictionary
  wget -O gpt2_bpe/dict.txt https://dl.fbaipublicfiles.com/fairseq/gpt2_bpe/dict.txt
  fairseq-preprocess \
      --only-source \
      --srcdict gpt2_bpe/dict.txt \
      --trainpref wikitext-103-raw/wiki.train.bpe \
      --validpref wikitext-103-raw/wiki.valid.bpe \
      --testpref wikitext-103-raw/wiki.test.bpe \
      --destdir data-bin/wikitext-103 \
      --workers 60

We provide ``ray_train.py`` as an entrence to the fairseq library. The code is cloned from ``train.py`` in the Fairseq library. Since we are training the model on spot instances, we provide fault-tolerance in ``ray_train.py`` by checkpointing and restarting when a node fails. The code will also check whether there are new resources available after checkpointing. If so, the program will make use them by restarting. See `the code <https://github.com/ray-project/ray/tree/master/doc/examples/lm/ray_train.py>`__ for more details.

To start training, run `following commands <https://github.com/ray-project/ray/tree/master/doc/examples/lm/ray_train.sh>`__ (``ray_train.sh``) on the head machine:

.. code-block:: bash

  cd ~/efs

  TOTAL_UPDATES=125000       # Total number of training steps
  WARMUP_UPDATES=10000       # Warmup the learning rate over this many updates
  PEAK_LR=0.0005             # Peak learning rate, adjust as needed
  TOKENS_PER_SAMPLE=512      # Max sequence length
  MAX_POSITIONS=512          # Num. positional embeddings (usually same as above)
  MAX_SENTENCES=16           # Number of sequences per batch on one GPU (batch size)
  FIX_BATCH_SZIE=2048        # Number of batch size in total (max_sentences * update_freq * n_gpus)
  SAVE_INTERVAL_UPDATES=1000 # save a checkpoint every N updates
  
  LOG_DIR=log/
  DATA_DIR=data-bin/wikitext-103  
  mkdir -p $LOG_DIR

  python ray_train.py --fp16 $DATA_DIR \
      --task masked_lm --criterion masked_lm \
      --arch roberta_base --sample-break-mode complete --tokens-per-sample $TOKENS_PER_SAMPLE \
      --optimizer adam --adam-betas '(0.9, 0.98)' --adam-eps 1e-6 --clip-norm 0.0 \
      --lr-scheduler polynomial_decay --lr $PEAK_LR --warmup-updates $WARMUP_UPDATES --total-num-update $TOTAL_UPDATES \
      --dropout 0.1 --attention-dropout 0.1 --weight-decay 0.01 \
      --max-sentences $MAX_SENTENCES \
      --fix-batch-size $FIX_BATCH_SZIE \
      --max-update $TOTAL_UPDATES --log-format simple --log-interval 1 \
      --save-interval-updates $SAVE_INTERVAL_UPDATES \
      --save-dir $LOG_DIR --ddp-backend=no_c10d

``SAVE_INTERVAL_UPDATES`` controls how often to save a checkpoint, which can be tuned based on the `stability of chosed instances <https://aws.amazon.com/ec2/spot/instance-advisor/>`__. ``FIX_BATCH_SZIE`` controls the total batch size to be a roughly fixed number.

To let Ray automatically stop the cluster after the training finished, you can download the ``ray_train.sh`` to ``~/efs`` of the remote machine, and run the following command on your local machine:

.. code-block:: bash

  ray exec --stop lm-cluster.yaml 'bash $HOME/efs/ray_train.sh'

or run the following command on the remote head node:

.. code-block:: bash

  ray exec --stop ~/ray_bootstrap_config.yaml 'bash $HOME/efs/ray_train.sh'

To test the fault-tolerance, you can run the following command on your local machine to randomly kill one node:

.. code-block:: bash

  ray kill-random-node lm-cluster.yaml

