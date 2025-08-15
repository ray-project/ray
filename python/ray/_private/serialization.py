import io
import logging
import threading
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    import torch

import google.protobuf.message

import ray._private.utils
import ray.cloudpickle as pickle
import ray.exceptions
from ray._private import ray_constants
from ray._raylet import (
    DynamicObjectRefGenerator,
    MessagePackSerializedObject,
    MessagePackSerializer,
    Pickle5SerializedObject,
    Pickle5Writer,
    RawSerializedObject,
    SerializedRayObject,
    split_buffer,
    unpack_pickle5_buffers,
)
from ray.core.generated.common_pb2 import ErrorType, RayErrorInfo
from ray.exceptions import (
    ActorDiedError,
    ActorPlacementGroupRemoved,
    ActorUnavailableError,
    ActorUnschedulableError,
    LocalRayletDiedError,
    NodeDiedError,
    ObjectFetchTimedOutError,
    ObjectFreedError,
    ObjectLostError,
    ObjectReconstructionFailedError,
    ObjectReconstructionFailedLineageEvictedError,
    ObjectReconstructionFailedMaxAttemptsExceededError,
    ObjectRefStreamEndOfStreamError,
    OutOfDiskError,
    OutOfMemoryError,
    OwnerDiedError,
    PlasmaObjectNotAvailable,
    RayError,
    RaySystemError,
    RayTaskError,
    ReferenceCountingAssertionError,
    RuntimeEnvSetupError,
    TaskCancelledError,
    TaskPlacementGroupRemoved,
    TaskUnschedulableError,
    WorkerCrashedError,
)
from ray.experimental.compiled_dag_ref import CompiledDAGRef
from ray.util import inspect_serializability, serialization_addons

logger = logging.getLogger(__name__)
ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = ray_constants.env_bool(
    "RAY_allow_out_of_band_object_ref_serialization", True
)


class DeserializationError(Exception):
    pass


def pickle_dumps(obj: Any, error_msg: str):
    """Wrap cloudpickle.dumps to provide better error message
    when the object is not serializable.
    """
    try:
        return pickle.dumps(obj)
    except (TypeError, ray.exceptions.OufOfBandObjectRefSerializationException) as e:
        sio = io.StringIO()
        inspect_serializability(obj, print_file=sio)
        msg = f"{error_msg}:\n{sio.getvalue()}"
        if isinstance(e, TypeError):
            raise TypeError(msg) from e
        else:
            raise ray.exceptions.OufOfBandObjectRefSerializationException(msg)


def _object_ref_deserializer(binary, call_site, owner_address, object_status):
    # NOTE(suquark): This function should be a global function so
    # cloudpickle can access it directly. Otherwise cloudpickle
    # has to dump the whole function definition, which is inefficient.

    # NOTE(swang): Must deserialize the object first before asking
    # the core worker to resolve the value. This is to make sure
    # that the ref count for the ObjectRef is greater than 0 by the
    # time the core worker resolves the value of the object.
    obj_ref = ray.ObjectRef(binary, owner_address, call_site)

    # TODO(edoakes): we should be able to just capture a reference
    # to 'self' here instead, but this function is itself pickled
    # somewhere, which causes an error.
    if owner_address:
        worker = ray._private.worker.global_worker
        worker.check_connected()
        context = worker.get_serialization_context()
        outer_id = context.get_outer_object_ref()
        # outer_id is None in the case that this ObjectRef was closed
        # over in a function or pickled directly using pickle.dumps().
        if outer_id is None:
            outer_id = ray.ObjectRef.nil()
        worker.core_worker.deserialize_and_register_object_ref(
            obj_ref.binary(), outer_id, owner_address, object_status
        )
    return obj_ref


def _actor_handle_deserializer(serialized_obj, weak_ref):
    # If this actor handle was stored in another object, then tell the
    # core worker.
    context = ray._private.worker.global_worker.get_serialization_context()
    outer_id = context.get_outer_object_ref()
    return ray.actor.ActorHandle._deserialization_helper(
        serialized_obj, weak_ref, outer_id
    )


class SerializationContext:
    """Initialize the serialization library.

    This defines a custom serializer for object refs and also tells ray to
    serialize several exception classes that we define for error handling.
    """

    def __init__(self, worker):
        self.worker = worker
        self._thread_local = threading.local()
        # This flag is to mark whether the custom serializer for torch.Tensor has
        # been registered. If the method is decorated with
        # `@ray.method(tensor_transport="xxx")`, it will use external transport
        # (e.g. gloo, nccl, etc.) for tensor communication between actors,
        # instead of the normal serialize -> object store -> deserialize codepath.
        self._torch_custom_serializer_registered = False

        def actor_handle_reducer(obj):
            ray._private.worker.global_worker.check_connected()
            serialized, actor_handle_id, weak_ref = obj._serialization_helper()
            # Update ref counting for the actor handle
            if not weak_ref:
                self.add_contained_object_ref(
                    actor_handle_id,
                    # Right now, so many tests are failing when this is set.
                    # Allow it for now, but we should eventually disallow it here.
                    allow_out_of_band_serialization=True,
                )
            return _actor_handle_deserializer, (serialized, weak_ref)

        self._register_cloudpickle_reducer(ray.actor.ActorHandle, actor_handle_reducer)

        def compiled_dag_ref_reducer(obj):
            raise TypeError("Serialization of CompiledDAGRef is not supported.")

        self._register_cloudpickle_reducer(CompiledDAGRef, compiled_dag_ref_reducer)

        def object_ref_reducer(obj):
            worker = ray._private.worker.global_worker
            worker.check_connected()
            self.add_contained_object_ref(
                obj,
                allow_out_of_band_serialization=(
                    ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION
                ),
                call_site=obj.call_site(),
            )
            obj, owner_address, object_status = worker.core_worker.serialize_object_ref(
                obj
            )
            return _object_ref_deserializer, (
                obj.binary(),
                obj.call_site(),
                owner_address,
                object_status,
            )

        self._register_cloudpickle_reducer(ray.ObjectRef, object_ref_reducer)

        def object_ref_generator_reducer(obj):
            return DynamicObjectRefGenerator, (obj._refs,)

        self._register_cloudpickle_reducer(
            DynamicObjectRefGenerator, object_ref_generator_reducer
        )

        serialization_addons.apply(self)

    def _register_cloudpickle_reducer(self, cls, reducer):
        pickle.CloudPickler.dispatch[cls] = reducer

    def _unregister_cloudpickle_reducer(self, cls):
        pickle.CloudPickler.dispatch.pop(cls, None)

    def _register_cloudpickle_serializer(
        self, cls, custom_serializer, custom_deserializer
    ):
        def _CloudPicklerReducer(obj):
            return custom_deserializer, (custom_serializer(obj),)

        # construct a reducer
        pickle.CloudPickler.dispatch[cls] = _CloudPicklerReducer

    def is_in_band_serialization(self):
        return getattr(self._thread_local, "in_band", False)

    def set_in_band_serialization(self):
        self._thread_local.in_band = True

    def set_out_of_band_serialization(self):
        self._thread_local.in_band = False

    def get_outer_object_ref(self):
        stack = getattr(self._thread_local, "object_ref_stack", [])
        return stack[-1] if stack else None

    def get_and_clear_contained_object_refs(self):
        if not hasattr(self._thread_local, "object_refs"):
            self._thread_local.object_refs = set()
            return set()

        object_refs = self._thread_local.object_refs
        self._thread_local.object_refs = set()
        return object_refs

    def add_contained_object_ref(
        self,
        object_ref: "ray.ObjectRef",
        *,
        allow_out_of_band_serialization: bool,
        call_site: Optional[str] = None,
    ):
        if self.is_in_band_serialization():
            # This object ref is being stored in an object. Add the ID to the
            # list of IDs contained in the object so that we keep the inner
            # object value alive as long as the outer object is in scope.
            if not hasattr(self._thread_local, "object_refs"):
                self._thread_local.object_refs = set()
            self._thread_local.object_refs.add(object_ref)
        else:
            if not allow_out_of_band_serialization:
                raise ray.exceptions.OufOfBandObjectRefSerializationException(
                    f"It is not allowed to serialize ray.ObjectRef {object_ref.hex()}. "
                    "If you want to allow serialization, "
                    "set `RAY_allow_out_of_band_object_ref_serialization=1.` "
                    "If you set the env var, the object is pinned forever in the "
                    "lifetime of the worker process and can cause Ray object leaks. "
                    "See the callsite and trace to find where the serialization "
                    "occurs.\nCallsite: "
                    f"{call_site or 'Disabled. Set RAY_record_ref_creation_sites=1'}"
                )
            else:
                # If this serialization is out-of-band (e.g., from a call to
                # cloudpickle directly or captured in a remote function/actor),
                # then pin the object for the lifetime of this worker by adding
                # a local reference that won't ever be removed.
                ray._private.worker.global_worker.core_worker.add_object_ref_reference(
                    object_ref
                )

    def _deserialize_pickle5_data(
        self,
        data: Any,
        out_of_band_tensors: Optional[List["torch.Tensor"]],
    ) -> Any:
        """

        Args:
            data: The data to deserialize.
            out_of_band_tensors: Tensors that were sent out-of-band. If this is
                not None, then the serialized data will contain placeholders
                that need to be replaced with these tensors.

        Returns:
            Any: The deserialized object.
        """
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current().serialization_context
        enable_gpu_objects = out_of_band_tensors is not None
        if enable_gpu_objects:
            ctx.reset_out_of_band_tensors(out_of_band_tensors)

        try:
            in_band, buffers = unpack_pickle5_buffers(data)
            if len(buffers) > 0:
                obj = pickle.loads(in_band, buffers=buffers)
            else:
                obj = pickle.loads(in_band)
        # cloudpickle does not provide error types
        except pickle.pickle.PicklingError:
            raise DeserializationError()
        finally:
            if enable_gpu_objects:
                ctx.reset_out_of_band_tensors([])
        return obj

    def _deserialize_msgpack_data(
        self,
        data,
        metadata_fields,
        out_of_band_tensors: Optional[List["torch.Tensor"]] = None,
    ):
        msgpack_data, pickle5_data = split_buffer(data)

        if metadata_fields[0] == ray_constants.OBJECT_METADATA_TYPE_PYTHON:
            python_objects = self._deserialize_pickle5_data(
                pickle5_data, out_of_band_tensors
            )
        else:
            python_objects = []

        try:

            def _python_deserializer(index):
                return python_objects[index]

            obj = MessagePackSerializer.loads(msgpack_data, _python_deserializer)
        except Exception:
            raise DeserializationError()
        return obj

    def _deserialize_error_info(self, data, metadata_fields):
        assert data
        pb_bytes = self._deserialize_msgpack_data(data, metadata_fields)
        assert pb_bytes

        ray_error_info = RayErrorInfo()
        ray_error_info.ParseFromString(pb_bytes)
        return ray_error_info

    def _deserialize_actor_died_error(self, data, metadata_fields):
        if not data:
            return ActorDiedError()
        ray_error_info = self._deserialize_error_info(data, metadata_fields)
        assert ray_error_info.HasField("actor_died_error")
        if ray_error_info.actor_died_error.HasField("creation_task_failure_context"):
            return RayError.from_ray_exception(
                ray_error_info.actor_died_error.creation_task_failure_context
            )
        else:
            assert ray_error_info.actor_died_error.HasField("actor_died_error_context")
            return ActorDiedError(
                cause=ray_error_info.actor_died_error.actor_died_error_context
            )

    def _deserialize_object(
        self,
        data,
        metadata,
        object_ref,
        out_of_band_tensors: Optional[List["torch.Tensor"]],
    ):
        if metadata:
            metadata_fields = metadata.split(b",")
            if metadata_fields[0] in [
                ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
                ray_constants.OBJECT_METADATA_TYPE_PYTHON,
            ]:
                return self._deserialize_msgpack_data(
                    data, metadata_fields, out_of_band_tensors
                )
            # Check if the object should be returned as raw bytes.
            if metadata_fields[0] == ray_constants.OBJECT_METADATA_TYPE_RAW:
                if data is None:
                    return b""
                return data.to_pybytes()
            elif metadata_fields[0] == ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE:
                obj = self._deserialize_msgpack_data(
                    data, metadata_fields, out_of_band_tensors
                )
                # The last character is a 1 if weak_ref=True and 0 else.
                serialized, weak_ref = obj[:-1], obj[-1:] == b"1"
                return _actor_handle_deserializer(serialized, weak_ref)
            # Otherwise, return an exception object based on
            # the error type.
            try:
                error_type = int(metadata_fields[0])
            except Exception:
                raise Exception(
                    f"Can't deserialize object: {object_ref}, " f"metadata: {metadata}"
                )

            # RayTaskError is serialized with pickle5 in the data field.
            # TODO (kfstorm): exception serialization should be language
            # independent.
            if error_type == ErrorType.Value("TASK_EXECUTION_EXCEPTION"):
                obj = self._deserialize_msgpack_data(
                    data, metadata_fields, out_of_band_tensors
                )
                return RayError.from_bytes(obj)
            elif error_type == ErrorType.Value("WORKER_DIED"):
                return WorkerCrashedError()
            elif error_type == ErrorType.Value("ACTOR_DIED"):
                return self._deserialize_actor_died_error(data, metadata_fields)
            elif error_type == ErrorType.Value("LOCAL_RAYLET_DIED"):
                return LocalRayletDiedError()
            elif error_type == ErrorType.Value("TASK_CANCELLED"):
                # Task cancellations are serialized in two ways, so check both
                # deserialization paths.
                # TODO(swang): We should only have one serialization path.
                try:
                    # Deserialization from C++ (the CoreWorker task submitter).
                    # The error info will be stored as a RayErrorInfo.
                    error_message = ""
                    if data:
                        error_info = self._deserialize_error_info(data, metadata_fields)
                        error_message = error_info.error_message
                    return TaskCancelledError(error_message=error_message)
                except google.protobuf.message.DecodeError:
                    # Deserialization from Python. The TaskCancelledError is
                    # serialized and returned directly.
                    obj = self._deserialize_msgpack_data(
                        data, metadata_fields, out_of_band_tensors
                    )
                    return RayError.from_bytes(obj)
            elif error_type == ErrorType.Value("OBJECT_LOST"):
                return ObjectLostError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OBJECT_FETCH_TIMED_OUT"):
                return ObjectFetchTimedOutError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OUT_OF_DISK_ERROR"):
                return OutOfDiskError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OUT_OF_MEMORY"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                return OutOfMemoryError(error_info.error_message)
            elif error_type == ErrorType.Value("NODE_DIED"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                return NodeDiedError(error_info.error_message)
            elif error_type == ErrorType.Value("OBJECT_DELETED"):
                return ReferenceCountingAssertionError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OBJECT_FREED"):
                return ObjectFreedError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OWNER_DIED"):
                return OwnerDiedError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("OBJECT_UNRECONSTRUCTABLE"):
                return ObjectReconstructionFailedError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value(
                "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED"
            ):
                return ObjectReconstructionFailedMaxAttemptsExceededError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value(
                "OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED"
            ):
                return ObjectReconstructionFailedLineageEvictedError(
                    object_ref.hex(), object_ref.owner_address(), object_ref.call_site()
                )
            elif error_type == ErrorType.Value("RUNTIME_ENV_SETUP_FAILED"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                # TODO(sang): Assert instead once actor also reports error messages.
                error_msg = ""
                if error_info.HasField("runtime_env_setup_failed_error"):
                    error_msg = error_info.runtime_env_setup_failed_error.error_message
                return RuntimeEnvSetupError(error_message=error_msg)
            elif error_type == ErrorType.Value("TASK_PLACEMENT_GROUP_REMOVED"):
                return TaskPlacementGroupRemoved()
            elif error_type == ErrorType.Value("ACTOR_PLACEMENT_GROUP_REMOVED"):
                return ActorPlacementGroupRemoved()
            elif error_type == ErrorType.Value("TASK_UNSCHEDULABLE_ERROR"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                return TaskUnschedulableError(error_info.error_message)
            elif error_type == ErrorType.Value("ACTOR_UNSCHEDULABLE_ERROR"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                return ActorUnschedulableError(error_info.error_message)
            elif error_type == ErrorType.Value("END_OF_STREAMING_GENERATOR"):
                return ObjectRefStreamEndOfStreamError()
            elif error_type == ErrorType.Value("ACTOR_UNAVAILABLE"):
                error_info = self._deserialize_error_info(data, metadata_fields)
                if error_info.HasField("actor_unavailable_error"):
                    actor_id = error_info.actor_unavailable_error.actor_id
                else:
                    actor_id = None
                return ActorUnavailableError(error_info.error_message, actor_id)
            else:
                return RaySystemError("Unrecognized error type " + str(error_type))
        elif data:
            raise ValueError("non-null object should always have metadata")
        else:
            # Object isn't available in plasma. This should never be returned
            # to the user. We should only reach this line if this object was
            # deserialized as part of a list, and another object in the list
            # throws an exception.
            return PlasmaObjectNotAvailable

    def deserialize_objects(
        self,
        serialized_ray_objects: List[SerializedRayObject],
        object_refs,
        gpu_objects: Dict[str, List["torch.Tensor"]],
    ):
        assert len(serialized_ray_objects) == len(object_refs)
        # initialize the thread-local field
        if not hasattr(self._thread_local, "object_ref_stack"):
            self._thread_local.object_ref_stack = []
        results = []
        for object_ref, (data, metadata, transport) in zip(
            object_refs, serialized_ray_objects
        ):
            try:
                # Push the object ref to the stack, so the object under
                # the object ref knows where it comes from.
                self._thread_local.object_ref_stack.append(object_ref)
                object_tensors = None
                if object_ref is not None:
                    object_id = object_ref.hex()
                    if object_id in gpu_objects:
                        object_tensors = gpu_objects[object_id]
                obj = self._deserialize_object(
                    data,
                    metadata,
                    object_ref,
                    object_tensors,
                )
            except Exception as e:
                logger.exception(e)
                obj = RaySystemError(e, traceback.format_exc())
            finally:
                # Must clear ObjectRef to not hold a reference.
                if self._thread_local.object_ref_stack:
                    self._thread_local.object_ref_stack.pop()
            results.append(obj)
        return results

    def _serialize_to_pickle5(self, metadata, value):
        writer = Pickle5Writer()
        # TODO(swang): Check that contained_object_refs is empty.
        try:
            self.set_in_band_serialization()
            inband = pickle.dumps(
                value, protocol=5, buffer_callback=writer.buffer_callback
            )
        except Exception as e:
            self.get_and_clear_contained_object_refs()
            raise e
        finally:
            self.set_out_of_band_serialization()

        return Pickle5SerializedObject(
            metadata, inband, writer, self.get_and_clear_contained_object_refs()
        )

    def _serialize_to_msgpack(self, value):
        # Only RayTaskError is possible to be serialized here. We don't
        # need to deal with other exception types here.
        contained_object_refs = []

        if isinstance(value, RayTaskError):
            if issubclass(value.cause.__class__, TaskCancelledError):
                # Handle task cancellation errors separately because we never
                # want to warn about tasks that were intentionally cancelled by
                # the user.
                metadata = str(ErrorType.Value("TASK_CANCELLED")).encode("ascii")
                value = value.to_bytes()
            else:
                metadata = str(ErrorType.Value("TASK_EXECUTION_EXCEPTION")).encode(
                    "ascii"
                )
                value = value.to_bytes()
        elif isinstance(value, ray.actor.ActorHandle):
            # TODO(fyresone): ActorHandle should be serialized via the
            # custom type feature of cross-language.
            serialized, actor_handle_id, weak_ref = value._serialization_helper()
            if not weak_ref:
                contained_object_refs.append(actor_handle_id)
            # Update ref counting for the actor handle
            metadata = ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE
            # Append a 1 to mean weak ref or 0 for strong ref.
            # We do this here instead of in the main serialization helper
            # because msgpack expects a bytes object. We cannot serialize
            # `weak_ref` in the C++ code because the weak_ref property is only
            # available in the Python ActorHandle instance.
            value = serialized + (b"1" if weak_ref else b"0")
        else:
            metadata = ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE

        python_objects = []

        def _python_serializer(o):
            index = len(python_objects)
            python_objects.append(o)
            return index

        msgpack_data = MessagePackSerializer.dumps(value, _python_serializer)

        if python_objects:
            metadata = ray_constants.OBJECT_METADATA_TYPE_PYTHON
            pickle5_serialized_object = self._serialize_to_pickle5(
                metadata, python_objects
            )
        else:
            pickle5_serialized_object = None

        return MessagePackSerializedObject(
            metadata, msgpack_data, contained_object_refs, pickle5_serialized_object
        )

    def serialize_and_store_gpu_objects(
        self,
        value: Any,
        obj_id: bytes,
    ) -> MessagePackSerializedObject:
        """Retrieve GPU data from `value` and store it in the GPU object store. Then, return the serialized value.

        Args:
            value: The value to serialize.
            obj_id: The object ID of the value. `obj_id` is required, and the GPU data (e.g. tensors) in `value`
                will be stored in the GPU object store with the key `obj_id`.

        Returns:
            Serialized value.
        """
        assert (
            obj_id is not None
        ), "`obj_id` is required, and it is the key to retrieve corresponding tensors from the GPU object store."
        if not self._torch_custom_serializer_registered:
            # Register a custom serializer for torch.Tensor. If the method is
            # decorated with `@ray.method(tensor_transport="xxx")`, it will
            # use external transport (e.g. gloo, nccl, etc.) for tensor
            # communication between actors, instead of the normal serialize ->
            # object store -> deserialize codepath.
            from ray.experimental.channel.torch_tensor_type import TorchTensorType

            TorchTensorType().register_custom_serializer()
            self._torch_custom_serializer_registered = True

        serialized_val, tensors = self._serialize_and_retrieve_tensors(value)
        # Regardless of whether `tensors` is empty, we always store the GPU object
        # in the GPU object store. This ensures that `_get_tensor_meta` is not
        # blocked indefinitely.
        obj_id = obj_id.decode("ascii")
        worker = ray._private.worker.global_worker
        gpu_object_manager = worker.gpu_object_manager
        gpu_object_manager.gpu_object_store.add_object(obj_id, tensors, is_primary=True)

        return serialized_val

    def serialize(
        self, value: Any
    ) -> Union[RawSerializedObject, MessagePackSerializedObject]:
        """Serialize an object.

        Args:
            value: The value to serialize.

        Returns:
            Serialized value.
        """
        if isinstance(value, bytes):
            # If the object is a byte array, skip serializing it and
            # use a special metadata to indicate it's raw binary. So
            # that this object can also be read by Java.
            return RawSerializedObject(value)
        else:
            return self._serialize_to_msgpack(value)

    def _serialize_and_retrieve_tensors(
        self, value: Any
    ) -> Tuple[MessagePackSerializedObject, List["torch.Tensor"]]:
        """
        Serialize `value` and return the serialized value and any tensors retrieved from `value`.
        This is only used for GPU objects.
        """
        from ray.experimental.channel import ChannelContext

        ctx = ChannelContext.get_current().serialization_context
        prev_use_external_transport = ctx.use_external_transport
        ctx.set_use_external_transport(True)
        try:
            serialized_val = self._serialize_to_msgpack(value)
        finally:
            ctx.set_use_external_transport(prev_use_external_transport)

        tensors, _ = ctx.reset_out_of_band_tensors([])
        return serialized_val, tensors
