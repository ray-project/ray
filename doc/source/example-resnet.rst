ResNet
======

This code uses ResNet to do data parallel training
across multiple GPUs using Ray. View the `code for this example`_.

To run the example, you will need to install `TensorFlow`_ (at
least version ``1.0.0``). Then you can run the example as follows.

First download the CIFAR-10 or CIFAR-100 dataset.

.. code-block:: bash

  # Get the CIFAR-10 dataset.
  curl -o cifar-10-binary.tar.gz https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz
  tar -xvf cifar-10-binary.tar.gz

  # Get the CIFAR-100 dataset.
  curl -o cifar-100-binary.tar.gz https://www.cs.toronto.edu/~kriz/cifar-100-binary.tar.gz
  tar -xvf cifar-100-binary.tar.gz

Then run the training script that matches the dataset you downloaded.

.. code-block:: bash

  # Train Resnet on CIFAR-10.
  python ray/doc/examples/resnet/resnet_main.py \
      --eval_dir=/tmp/resnet-model/eval \
      --train_data_path=cifar-10-batches-bin/data_batch* \
      --eval_data_path=cifar-10-batches-bin/test_batch.bin \
      --dataset=cifar10 \
      --num_gpus=1

  # Train Resnet on CIFAR-100.
  python ray/doc/examples/resnet/resnet_main.py \
      --eval_dir=/tmp/resnet-model/eval \
      --train_data_path=cifar-100-binary/train.bin \
      --eval_data_path=cifar-100-binary/test.bin \
      --dataset=cifar100 \
      --num_gpus=1

To run the training script on a cluster with multiple machines, you will need
to also pass in the flag ``--address=<address>``, where
``<address>`` is the address of the Redis server on the head node.

The script will print out the IP address that the log files are stored on. In
the single-node case, you can ignore this and run tensorboard on the current
machine.

.. code-block:: bash

  python -m tensorflow.tensorboard --logdir=/tmp/resnet-model

If you are running Ray on multiple nodes, you will need to go to the node at the
IP address printed, and run the command.

The core of the script is the actor definition.

.. code-block:: python

  @ray.remote(num_gpus=1)
  class ResNetTrainActor(object):
      def __init__(self, data, dataset, num_gpus):
          # data is the preprocessed images and labels extracted from the dataset.
          # Thus, every actor has its own copy of the data.
          # Set the CUDA_VISIBLE_DEVICES environment variable in order to restrict
          # which GPUs TensorFlow uses. Note that this only works if it is done before
          # the call to tf.Session.
          os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in ray.get_gpu_ids()])
          with tf.Graph().as_default():
              with tf.device('/gpu:0'):
                  # We omit the code here that actually constructs the residual network
                  # and initializes it. Uses the definition in the Tensorflow Resnet Example.

      def compute_steps(self, weights):
          # This method sets the weights in the network, runs some training steps,
          # and returns the new weights. self.model.variables is a TensorFlowVariables
          # class that we pass the train operation into.
          self.model.variables.set_weights(weights)
          for i in range(self.steps):
              self.model.variables.sess.run(self.model.train_op)
          return self.model.variables.get_weights()

The main script first creates one actor for each GPU, or a single actor if
``num_gpus`` is zero.

.. code-block:: python

  train_actors = [ResNetTrainActor.remote(train_data, dataset, num_gpus) for _ in range(num_gpus)]

Then the main loop passes the same weights to every model, performs
updates on each model, averages the updates, and puts the new weights in the
object store.

.. code-block:: python

  while True:
      all_weights = ray.get([actor.compute_steps.remote(weight_id) for actor in train_actors])
      mean_weights = {k: sum([weights[k] for weights in all_weights]) / num_gpus for k in all_weights[0]}
      weight_id = ray.put(mean_weights)

.. _`TensorFlow`: https://www.tensorflow.org/install/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/examples/resnet
