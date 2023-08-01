.. _train-pytorch-overview:

Distributed PyTorch
===================
Ray Train's `PyTorch <https://pytorch.org/>`__  integration
makes it easy to scale your PyTorch-based training to many nodes
and GPUs. This includes training loops for libraries built on top of PyTorch, such as
`PyTorch Lightning <https://www.pytorchlightning.ai/>`_,
`Hugging Face Transformers <https://huggingface.co/docs/transformers/index>`_,
and `Hugging Face Accelerate <https://huggingface.co/docs/accelerate/index>`_.

On a technical level, Ray Train schedules your training workers and sets up
the distributed process group, allowing
you to run your ``DistributedDataParallel`` training script.

For more information on the technical details, see the `PyTorch
Distributed Overview <https://pytorch.org/tutorials/beginner/dist_overview.html>`_
on their official documentation for reference.

.. grid:: 1 2 3 4

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/converting-existing-training-loop

            Convert an existing training loop
