Distributed PyTorch
===================

Ray integrates with PyTorch as a training backend.

Ray Train initializes your distributed process group, allowing you to run your ``DistributedDataParallel`` training script.

Ray Train also supports PyTorch's more advanced communication patterns.

A number of libraries for machine learning have been built on top of PyTorch,
such as PyTorch Lightning or Huggingface Transformers / Accelerate.
All these libraries are compatible with Ray Train as well.


.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/native-pytorch

            Ray Train with native PyTorch

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/pytorch-lightning

            Ray Train with PyTorch Lightning

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: distributed-pytorch/huggingface-transformers-accelerate

            Ray Train with Huggingface Transformers / Accelerate
