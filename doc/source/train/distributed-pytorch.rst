.. _train-pytorch-overview:

Distributed PyTorch
===================
`PyTorch <https://pytorch.org/>`__ is one of the most popular deep learning
frameworks. Ray Train's PyTorch integration enables you to scale your PyTorch
training up.

Ray Train natively supports libraries built on top of PyTorch, such as
PytorchLightning, Huggingface transformers, and accelerate.

On a technical level, Ray Train schedules your training workers and sets up
the distributed process group, allowing
you to run your ``DistributedDataParallel`` training script.

See `PyTorch
Distributed Overview <https://pytorch.org/tutorials/beginner/dist_overview.html>`_
for more information.

.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/quickstart

            Quick start example

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/converting-existing-training-loop

            Convert an existing training loop
