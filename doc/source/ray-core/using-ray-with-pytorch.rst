Best Practices: Ray with PyTorch
================================

This document describes best practices for using Ray with PyTorch. Feel free to contribute if you think this document is missing anything.

Downloading Data
----------------

It is very common for multiple Ray actors running PyTorch to have code that downloads the dataset for training and testing.

.. code-block:: python

    # This is running inside a Ray actor
    # ...
    torch.utils.data.DataLoader(
        datasets.MNIST(
            "../data", train=True, download=True,
            transform=transforms.Compose([
                transforms.ToTensor(),
                transforms.Normalize((0.1307,), (0.3081,))
            ])),
        128, shuffle=True, **kwargs)
    # ...

This may cause different processes to simultaneously download the data and cause data corruption. One easy workaround for this is to use ``Filelock``:

.. code-block:: python

    from filelock import FileLock

    with FileLock("./data.lock"):
        torch.utils.data.DataLoader(
            datasets.MNIST(
                "./data", train=True, download=True,
                transform=transforms.Compose([
                    transforms.ToTensor(),
                    transforms.Normalize((0.1307,), (0.3081,))
                ])),
            128, shuffle=True, **kwargs)


Use Actors for Parallel Models
------------------------------

One common use case for using Ray with PyTorch is to parallelize the training of multiple models.

.. tip::
  Avoid sending the PyTorch model directly. Send ``model.state_dict()``, as
  PyTorch tensors are natively supported by the Plasma Object Store.


Suppose we have a simple network definition (this one is modified from the
PyTorch documentation).

.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __torch_model_start__
   :end-before: __torch_model_end__

Along with these helper training functions:

.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __torch_helper_start__
   :end-before: __torch_helper_end

Let's now define a class that captures the training process.

.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __torch_net_start__
   :end-before: __torch_net_end


To train multiple models, you can convert the above class into a Ray Actor class.

.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __torch_ray_start__
   :end-before: __torch_ray_end__


Then, we can instantiate multiple copies of the Model, each running on different processes. If GPU is enabled, each copy runs on a different GPU. See the `GPU guide <using-ray-with-gpus.html>`_ for more information.

.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __torch_actor_start__
   :end-before: __torch_actor_end__


We can then use ``set_weights`` and ``get_weights`` to move the weights of the neural network around. The below example averages the weights of the two networks and sends them back to update the original actors.


.. literalinclude:: /ray-core/_examples/doc_code/torch_example.py
   :language: python
   :start-after: __weight_average_start__
