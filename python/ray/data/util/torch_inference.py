"""The user-facing base class for Torch batch inference with ``map_batches``."""
from typing import TYPE_CHECKING, Any, Tuple, Union

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch

    from ray.data.block import DataBatch
    from ray.data.collate_fn import TensorBatchType


@PublicAPI(stability="alpha")
class TorchInference:
    """Base class for PyTorch batch inference with
    :meth:`~ray.data.Dataset.map_batches`.

    Subclass this and implement :meth:`process_on_device`. For every batch,
    Ray Data runs:

    1. :meth:`collate` — convert the batch into CPU torch tensors.
    2. Ray Data moves the tensors to the :meth:`get_device` device.
    3. :meth:`process_on_device` — compute on the device torch tensors.
    4. Ray Data moves the resulting torch tensors back to the CPU.
    5. :meth:`finalize` — convert them into the output batch.

    Override :meth:`collate`, :meth:`finalize`, or :meth:`get_device` only if
    the defaults don't fit your data.

    **Passing non-tensor data between steps.** Only tensors go through the
    managed device transfers, but :meth:`collate` and
    :meth:`process_on_device` can each return a ``(tensors, other)`` tuple to
    hand small side values (original shapes, padding lengths, strings, etc)
    directly to the next step: ``collate``'s ``other`` arrives as
    ``collated_other`` in :meth:`process_on_device`, and
    ``process_on_device``'s ``other`` arrives as ``output_other`` in
    :meth:`finalize`. Return a bare ``TensorBatchType`` and the next step
    receives ``None`` instead. Ray Data passes ``other`` values through
    untouched.

    Examples:

        Minimal — only :meth:`process_on_device`, everything else default:

        .. testcode::
            :skipif: True

            import numpy as np
            import torch
            import ray
            from ray.data.util.torch_inference import TorchInference

            class MyInferenceActor(TorchInference):

                def initialize(self):
                    self.model = torch.nn.Identity().cuda().eval()

                def process_on_device(
                    self, input_batch, collated_tensors, collated_other
                ):
                    out = self.model(collated_tensors["data"])
                    return {"mean": out.float().mean(dim=1)}

            ds = (
                ray.data.from_numpy(np.ones((32, 100), dtype=np.float32))
                .map_batches(
                    MyInferenceActor,
                    batch_size=4,
                    compute=ray.data.ActorPoolStrategy(size=1),
                    num_gpus=1,
                )
            )

    .. note::
        Don't define ``__init__`` in your subclass — put setup code in
        :meth:`initialize`, which Ray Data calls once when the actor starts.

    .. note::
        With the default :meth:`collate`, the ``batch_format`` given to
        ``map_batches`` must be ``"default"`` or ``"numpy"``.

    The per-batch methods run under ``torch.no_grad()``; gradients are not
    recorded.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        """Forward the constructor arguments to :meth:`initialize`.

        Args:
            *args: Forwarded to :meth:`initialize`.
            **kwargs: Forwarded to :meth:`initialize`.
        """
        self.initialize(*args, **kwargs)

    def initialize(self, *args: Any, **kwargs: Any) -> None:
        """Initialize actor state, such as the model.

        Called once when the actor starts. Override this instead of defining
        ``__init__``.

        Args:
            *args: The ``fn_constructor_args`` given to
                :meth:`~ray.data.Dataset.map_batches`.
            **kwargs: The ``fn_constructor_kwargs`` given to
                :meth:`~ray.data.Dataset.map_batches`.
        """
        pass

    def get_device(self) -> "torch.device":
        """Return the device batches are processed on.

        Called once when the actor starts. The default returns
        ``torch.device("cuda")``.

        Returns:
            The device that :meth:`collate` outputs are moved to and that
            :meth:`process_on_device` runs on. Must be a CUDA device.
        """
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError(
                "CUDA is not available on this system. The default TorchInference "
                "flow is GPU-only and requires a CUDA-capable device."
            )
        return torch.device("cuda")

    def collate(
        self, input_batch: "DataBatch"
    ) -> Union["TensorBatchType", Tuple["TensorBatchType", Any]]:
        """Convert an input batch into CPU tensors.

        Nested tensor sequences (e.g. ``Dict[str, List[Tensor]]``) are treated
        as chunks of one logical tensor and concatenated along the batch
        dimension during the device transfer. Use flat structures to preserve
        tensor shapes as-is.

        Args:
            input_batch: The batch to convert, in the ``batch_format`` given
                to :meth:`~ray.data.Dataset.map_batches`. The default
                implementation only supports NumPy batches
                (``Dict[str, np.ndarray]``) and raises ``TypeError`` for
                other batch formats.

        Returns:
            The batch as Torch tensors, optionally with side data:

            - ``tensors``: the tensors must be on the CPU; Ray Data moves
              them to the :meth:`get_device` device before calling
              :meth:`process_on_device`.
            - ``(tensors, other)``: additionally hand ``other`` — any
              non-tensor side value, passed through untouched — to
              :meth:`process_on_device` as ``collated_other``. When only
              ``tensors`` is returned, ``collated_other`` is ``None``.
        """
        import numpy as np

        from ray.data.util.torch_utils import (
            _get_type_str,
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        if not isinstance(input_batch, dict) or not all(
            isinstance(column, np.ndarray) for column in input_batch.values()
        ):
            raise TypeError(
                "The default `collate` only supports NumPy batches "
                f"(`Dict[str, np.ndarray]`); got {_get_type_str(input_batch)}. "
                'Use `batch_format="numpy"` in `map_batches`, or override '
                "`collate` to convert the batch yourself."
            )
        return convert_ndarray_batch_to_torch_tensor_batch(input_batch)

    def process_on_device(
        self,
        input_batch: "DataBatch",
        collated_tensors: "TensorBatchType",
        collated_other: Any,
    ) -> Union["TensorBatchType", Tuple["TensorBatchType", Any]]:
        """Process a batch of device tensors and return device tensors.

        This is the only method a subclass must implement.

        Args:
            input_batch: The untouched input batch (pre-:meth:`collate`), for
                reading fields that don't go through the tensor path.
            collated_tensors: The tensors returned by :meth:`collate`, moved
                to the :meth:`get_device` device.
            collated_other: The side data returned by :meth:`collate`, or
                ``None`` if :meth:`collate` returned only tensors.

        Returns:
            The resulting Torch tensors, optionally with side data:

            - ``tensors``: the tensors must be on the device; Ray Data moves
              them back to the CPU before calling :meth:`finalize`.
            - ``(tensors, other)``: additionally hand ``other`` — any
              non-tensor side value, passed through untouched — to
              :meth:`finalize` as ``output_other``. When only ``tensors`` is
              returned, ``output_other`` is ``None``.
        """
        raise NotImplementedError(
            f"`{type(self).__name__}` must implement `process_on_device`."
        )

    def finalize(
        self,
        input_batch: "DataBatch",
        output_tensors: "TensorBatchType",
        output_other: Any,
    ) -> "DataBatch":
        """Convert the output tensors into the batch ``map_batches`` returns.

        Args:
            input_batch: The untouched input batch (pre-:meth:`collate`), for
                passing fields through to the output.
            output_tensors: The tensors returned by :meth:`process_on_device`,
                already moved back to the CPU.
            output_other: The side data returned by :meth:`process_on_device`,
                or ``None`` if it returned only tensors.

        Returns:
            The output batch, in a format
            :meth:`~ray.data.Dataset.map_batches` accepts. The default
            implementation recursively converts every Torch tensor into a
            NumPy array, preserving the surrounding dict/sequence structure
            (e.g. ``Dict[str, torch.Tensor]`` becomes
            ``Dict[str, np.ndarray]``), and raises ``TypeError`` for
            non-tensor values — or for a non-``None`` ``output_other``, which
            it doesn't know how to fold into the batch; override ``finalize``
            to consume it.
        """
        from ray.data.util.torch_utils import convert_tensors_to_numpy

        if output_other is not None:
            raise TypeError(
                "The default `finalize` doesn't know how to fold "
                "`output_other` into the output batch. Override `finalize` "
                "to consume the side data returned by `process_on_device`."
            )
        return convert_tensors_to_numpy(output_tensors)
