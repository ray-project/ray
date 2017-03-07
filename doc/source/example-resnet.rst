ResNet
======

This code adapts the `TensorFlow ResNet example`_ to do data parallel training
across multiple GPUs using Ray. View the `code for this example`_.

To run the example, you will need to install `TensorFlow with GPU support`_ (at
least version ``1.0.0``). Then you can run the example as follows.

First download the CIFAR-10 dataset.

.. code-block:: bash

  curl -o cifar-10-binary.tar.gz https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz

  tar -xvf cifar-10-binary.tar.gz


Then run the training script.

.. code-block:: bash

  python ray/examples/resnet/resnet_main.py \
      --train_data_path=cifar-10-batches-bin/data_batch* \
      --eval_data_path=cifar-10-batches-bin/test_batch.bin \
      --num_gpus=1

The core of the script is the actor definition.

.. code-block:: python

  @ray.actor(num_gpus=1)
  class ResNetTrainActor(object):
    def __init__(self, path, num_gpus):
      # Set the CUDA_VISIBLE_DEVICES environment variable in order to restrict
      # which GPUs TensorFlow uses. Note that this only works if it is done before
      # the call to tf.Session.
      os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in ray.get_gpu_ids()])
      with tf.Graph().as_default():
        with tf.device('/gpu:0'):
          # We omit the code here that actually constructs the residual network
          # and initializes it.

    def compute_steps(self, weights):
      # This method sets the weights in the network, runs some training steps,
      # and returns the new weights.
      steps = 10
      self.model.variables.set_weights(weights)
      for i in range(steps):
        self.model.variables.sess.run(self.model.train_op)
      return self.model.variables.get_weights()

The main script first creates one actor for each GPU.

.. code-block:: python

  train_actors = [ResNetTrainActor(train_data, num_gpus) for _ in range(num_gpus)]

Then after initializing the actors with the same weights, the main loop performs
updates on each model, averages the updates, and puts the new weights in the
object store.

.. code-block:: python

  while True:
    all_weights = ray.get([actor.compute_steps(weight_id) for actor in train_actors])
    mean_weights = {k: sum([weights[k] for weights in all_weights]) / num_gpus for k in all_weights[0]}
    weight_id = ray.put(mean_weights)

.. _`TensorFlow ResNet example`: https://github.com/tensorflow/models/tree/master/resnet
.. _`TensorFlow with GPU support`: https://www.tensorflow.org/install/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/resnet
