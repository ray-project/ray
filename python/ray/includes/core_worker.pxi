from libc.stdint cimport int32_t, int64_t, uint64_t
from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport make_unique, shared_ptr, unique_ptr
from libcpp.pair cimport pair as c_pair
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from libcpp.vector cimport vector as c_vector

from cython.operator import dereference, postincrement

cimport cpython

from ray.includes.optional cimport optional, nullopt, make_optional
from ray.includes.ray_config cimport RayConfig
from ray.includes.unique_ids cimport (
    CActorID,
    CClusterID,
    CNodeID,
    CObjectID,
    CPlacementGroupID,
    ObjectIDIndexType,
)
from ray.includes.common cimport (
    CAddress,
    CBuffer,
    CConcurrencyGroup,
    CFallbackOption,
    CJobConfig,
    CLabelIn,
    CLabelMatchExpression,
    CLabelMatchExpressions,
    CLabelNotIn,
    CLabelSelector,
    CLineageReconstructionTask,
    CNodeAffinitySchedulingStrategy,
    CNodeLabelSchedulingStrategy,
    CObjectReference,
    CPlacementGroupSchedulingStrategy,
    CPlacementStrategy,
    CRayFunction,
    CRayObject,
    CRayStatus,
    CSchedulingStrategy,
    CTaskArg,
    CWorkerExitType,
    LANGUAGE_PYTHON,
    LocalMemoryBuffer,
    PLACEMENT_STRATEGY_PACK,
    PLACEMENT_STRATEGY_SPREAD,
    PLACEMENT_STRATEGY_STRICT_PACK,
    PLACEMENT_STRATEGY_STRICT_SPREAD,
    WORKER_EXIT_TYPE_INTENTIONAL_SYSTEM_ERROR,
    WORKER_EXIT_TYPE_SYSTEM_ERROR,
    WORKER_EXIT_TYPE_USER_ERROR,
    WORKER_TYPE_DRIVER,
    WORKER_TYPE_RESTORE_WORKER,
    WORKER_TYPE_SPILL_WORKER,
    WORKER_TYPE_WORKER,
    move,
)
from ray.includes.libcoreworker cimport (
    ActorHandleSharedPtr,
    CActorCreationOptions,
    CCoreWorkerOptions,
    CCoreWorkerProcess,
    CFiberEvent,
    CPlacementGroupCreationOptions,
    CReaderRefInfo,
    CTaskOptions,
    ResourceMappingType,
)

import asyncio
import concurrent.futures
import inspect
import threading
import time
import traceback
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union

import ray
import ray._private.ray_constants as ray_constants


cdef class CoreWorker:
    """Adapter class for calling C++ CoreWorker from Python.
    """

    def __cinit__(self, worker_type, store_socket, raylet_socket,
                  JobID job_id, GcsClientOptions gcs_options, log_dir,
                  node_ip_address, node_manager_port,
                  local_mode, driver_name,
                  serialized_job_config, metrics_agent_port, runtime_env_hash,
                  WorkerID worker_id, session_name, cluster_id, entrypoint,
                  worker_launch_time_ms, worker_launched_time_ms, debug_source):
        self.is_local_mode = local_mode

        cdef CCoreWorkerOptions options = CCoreWorkerOptions()
        if worker_type in (ray.LOCAL_MODE, ray.SCRIPT_MODE):
            self.is_driver = True
            options.worker_type = WORKER_TYPE_DRIVER
        elif worker_type == ray.WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_WORKER
        elif worker_type == ray.SPILL_WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_SPILL_WORKER
        elif worker_type == ray.RESTORE_WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_RESTORE_WORKER
        else:
            raise ValueError(f"Unknown worker type: {worker_type}")
        options.language = LANGUAGE_PYTHON
        options.store_socket = store_socket.encode("ascii")
        options.raylet_socket = raylet_socket.encode("ascii")
        options.job_id = job_id.native()
        options.gcs_options = gcs_options.native()[0]
        options.enable_logging = True
        options.log_dir = log_dir.encode("utf-8")
        options.install_failure_signal_handler = (
            not ray_constants.RAY_DISABLE_FAILURE_SIGNAL_HANDLER
        )
        # https://stackoverflow.com/questions/2356399/tell-if-python-is-in-interactive-mode
        options.interactive = hasattr(sys, "ps1")
        options.node_ip_address = node_ip_address.encode("utf-8")
        options.node_manager_port = node_manager_port
        options.driver_name = driver_name
        options.initialize_thread_callback = initialize_pygilstate_for_thread
        options.task_execution_callback = task_execution_handler
        options.free_actor_object_callback = free_actor_object_callback
        options.set_direct_transport_metadata = set_direct_transport_metadata
        options.check_signals = check_signals
        options.gc_collect = gc_collect
        options.spill_objects = spill_objects_handler
        options.restore_spilled_objects = restore_spilled_objects_handler
        options.delete_spilled_objects = delete_spilled_objects_handler
        options.unhandled_exception_handler = unhandled_exception_handler
        options.cancel_async_actor_task = cancel_async_actor_task
        options.get_lang_stack = get_py_stack
        options.is_local_mode = local_mode
        options.kill_main = kill_main_task
        options.actor_shutdown_callback = call_actor_shutdown
        options.serialized_job_config = serialized_job_config
        options.metrics_agent_port = metrics_agent_port
        options.runtime_env_hash = runtime_env_hash
        options.worker_id = worker_id.native()
        options.session_name = session_name
        options.cluster_id = CClusterID.FromHex(cluster_id)
        options.entrypoint = entrypoint
        options.worker_launch_time_ms = worker_launch_time_ms
        options.worker_launched_time_ms = worker_launched_time_ms
        options.debug_source = debug_source
        CCoreWorkerProcess.Initialize(options)

        self.cgname_to_eventloop_dict = None
        self.fd_to_cgname_dict = None
        self.eventloop_for_default_cg = None
        self.current_runtime_env = None
        self._task_id_to_future_lock = threading.Lock()
        self._task_id_to_future = {}
        self.event_loop_executor = None

        self._gc_thread = None
        if RayConfig.instance().start_python_gc_manager_thread():
            self._gc_thread = PythonGCThread()
            self._gc_thread.start()

    def shutdown_driver(self):
        # If it's a worker, the core worker process should have been
        # shutdown. So we can't call
        # `CCoreWorkerProcess.GetCoreWorker().GetWorkerType()` here.
        # Instead, we use the cached `is_driver` flag to test if it's a
        # driver.
        assert self.is_driver
        if self._gc_thread is not None:
            self._gc_thread.stop()
            self._gc_thread = None
        with nogil:
            CCoreWorkerProcess.Shutdown()

    def run_task_loop(self):
        with nogil:
            CCoreWorkerProcess.RunTaskExecutionLoop()

    def drain_and_exit_worker(self, exit_type: str, c_string detail):
        """
        Exit the current worker process. This API should only be used by
        a worker. If this API is called, the worker will wait to finish
        currently executing task, initiate the shutdown, and stop
        itself gracefully. The given exit_type and detail will be
        reported to GCS, and any worker failure error will contain them.

        The behavior of this API while a task is running is undefined.
        Avoid using the API when a task is still running.
        """
        cdef:
            CWorkerExitType c_exit_type
            cdef const shared_ptr[LocalMemoryBuffer] null_ptr

        if exit_type == "user":
            c_exit_type = WORKER_EXIT_TYPE_USER_ERROR
        elif exit_type == "system":
            c_exit_type = WORKER_EXIT_TYPE_SYSTEM_ERROR
        elif exit_type == "intentional_system_exit":
            c_exit_type = WORKER_EXIT_TYPE_INTENTIONAL_SYSTEM_ERROR
        else:
            raise ValueError(f"Invalid exit type: {exit_type}")
        assert not self.is_driver
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().Exit(c_exit_type, detail, null_ptr)

    def get_current_task_name(self) -> str:
        """Return the current task name.

        If it is a normal task, it returns the task name from the main thread.
        If it is a threaded actor, it returns the task name for the current thread.
        If it is async actor, it returns the task name stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task name within asyncio task
        # via async_task_name contextvar. We try this first.
        # It is needed because the core worker's GetCurrentTask API
        # doesn't have asyncio context, thus it cannot return the
        # correct task name.
        task_name = async_task_name.get()
        if task_name is None:
            # if it is not within asyncio context, fallback to TaskName
            # obtainable from core worker.
            task_name = CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskName() \
                .decode("utf-8")
        return task_name

    def get_current_task_function_name(self) -> str:
        """Return the current task function.

        If it is a normal task, it returns the task function from the main thread.
        If it is a threaded actor, it returns the task function for the current thread.
        If it is async actor, it returns the task function stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task function within asyncio task
        # via async_task_function_name contextvar. We try this first.
        # It is needed because the core Worker's GetCurrentTask API
        # doesn't have asyncio context, thus it cannot return the
        # correct task function.
        task_function_name = async_task_function_name.get()
        if task_function_name is None:
            # if it is not within asyncio context, fallback to TaskName
            # obtainable from core worker.
            task_function_name = CCoreWorkerProcess.GetCoreWorker() \
                .GetCurrentTaskFunctionName().decode("utf-8")
        return task_function_name

    def get_current_task_id(self) -> TaskID:
        """Return the current task ID.

        If it is a normal task, it returns the TaskID from the main thread.
        If it is a threaded actor, it returns the TaskID for the current thread.
        If it is async actor, it returns the TaskID stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task ID within asyncio task
        # via async_task_id contextvar. We try this first.
        # It is needed because the core Worker's GetCurrentTaskId API
        # doesn't have asyncio context, thus it cannot return the
        # correct TaskID.
        task_id = async_task_id.get()
        if task_id is None:
            # if it is not within asyncio context, fallback to TaskID
            # obtainable from core worker.
            task_id = TaskID(
                CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskId().Binary())
        return task_id

    def get_current_task_attempt_number(self):
        return CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskAttemptNumber()

    def get_task_depth(self):
        return CCoreWorkerProcess.GetCoreWorker().GetTaskDepth()

    def get_current_job_id(self):
        return JobID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentJobId().Binary())

    def get_current_node_id(self):
        return NodeID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentNodeId().Binary())

    def get_actor_id(self):
        return ActorID(
            CCoreWorkerProcess.GetCoreWorker().GetActorId().Binary())

    def get_actor_name(self):
        return CCoreWorkerProcess.GetCoreWorker().GetActorName()

    def get_placement_group_id(self):
        return PlacementGroupID(
            CCoreWorkerProcess.GetCoreWorker()
            .GetCurrentPlacementGroupId().Binary())

    def get_worker_id(self):
        return WorkerID(
            CCoreWorkerProcess.GetCoreWorker().GetWorkerID().Binary())

    def should_capture_child_tasks_in_placement_group(self):
        return CCoreWorkerProcess.GetCoreWorker(
            ).ShouldCaptureChildTasksInPlacementGroup()

    def update_task_is_debugger_paused(self, TaskID task_id, is_debugger_paused):
        cdef:
            CTaskID c_task_id = task_id.native()

        return CCoreWorkerProcess.GetCoreWorker(
            ).UpdateTaskIsDebuggerPaused(c_task_id, is_debugger_paused)

    def get_objects(self, object_refs, int64_t timeout_ms=-1):
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = CCoreWorkerProcess.GetCoreWorker().Get(
                c_object_ids, timeout_ms, results)
        check_status(op_status)

        return RayObjectsToSerializedRayObjects(results, object_refs)

    def get_if_local(self, object_refs):
        """Get objects from local plasma store directly
        without a fetch request to raylet."""
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetIfLocal(
                    c_object_ids, &results))
        return RayObjectsToSerializedRayObjects(results, object_refs)

    def object_exists(self, ObjectRef object_ref, memory_store_only=False):
        cdef:
            c_bool has_object
            c_bool is_in_plasma
            CObjectID c_object_id = object_ref.native()

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Contains(
                c_object_id, &has_object, &is_in_plasma))

        return has_object and (not memory_store_only or not is_in_plasma)

    cdef unique_ptr[CAddress] _convert_python_address(self, address=None):
        """ convert python address to `CAddress`, If not provided,
        return nullptr.

        Args:
            address: worker address.
        """
        cdef:
            unique_ptr[CAddress] c_address

        if address is not None:
            c_address = make_unique[CAddress]()
            dereference(c_address).ParseFromString(address)
        return move(c_address)

    def put_file_like_object(
            self, metadata, data_size, file_like, ObjectRef object_ref,
            owner_address):
        """Directly create a new Plasma Store object from a file like
        object. This avoids extra memory copy.

        Args:
            metadata (bytes): The metadata of the object.
            data_size (int): The size of the data buffer.
            file_like: A python file object that provides the `readinto`
                interface.
            object_ref: The new ObjectRef.
            owner_address: Owner address for this object ref.
        """
        cdef:
            CObjectID c_object_id = object_ref.native()
            shared_ptr[CBuffer] data_buf
            shared_ptr[CBuffer] metadata_buf
            unique_ptr[CAddress] c_owner_address = self._convert_python_address(
                    object_ref.owner_address())

        # TODO(suquark): This method does not support put objects to
        # in memory store currently.
        metadata_buf = string_to_buffer(metadata)

        status = CCoreWorkerProcess.GetCoreWorker().CreateExisting(
                    metadata_buf, data_size, object_ref.native(),
                    dereference(c_owner_address), &data_buf,
                    False)
        if not status.ok():
            logger.debug("Error putting restored object into plasma.")
            return
        if data_buf == NULL:
            logger.debug("Object already exists in 'put_file_like_object'.")
            return
        data = Buffer.make(data_buf)
        view = memoryview(data)
        index = 0
        while index < data_size:
            bytes_read = file_like.readinto(view[index:])
            index += bytes_read
        with nogil:
            # Using custom object refs is not supported because we
            # can't track their lifecycle, so we don't pin the object
            # in this case.
            check_status(
                CCoreWorkerProcess.GetCoreWorker().SealExisting(
                            c_object_id, pin_object=False,
                            generator_id=CObjectID.Nil(),
                            owner_address=c_owner_address))

    def experimental_channel_put_serialized(self, serialized_object,
                                            ObjectRef object_ref,
                                            num_readers,
                                            timeout_ms):
        cdef:
            CObjectID c_object_id = object_ref.native()
            shared_ptr[CBuffer] data
            uint64_t data_size = serialized_object.total_bytes
            int64_t c_num_readers = num_readers
            int64_t c_timeout_ms = timeout_ms

        metadata = string_to_buffer(serialized_object.metadata)
        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalChannelWriteAcquire(
                             c_object_id,
                             metadata,
                             data_size,
                             c_num_readers,
                             c_timeout_ms,
                             &data,
                             ))
        if data_size > 0:
            (<SerializedObject>serialized_object).write_to(
                Buffer.make(data))

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalChannelWriteRelease(
                             c_object_id,
                             ))

    def experimental_channel_set_error(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CRayStatus status

        with nogil:
            status = (CCoreWorkerProcess.GetCoreWorker()
                      .ExperimentalChannelSetError(c_object_id))
        return status.ok()

    def experimental_channel_register_writer(self,
                                             ObjectRef writer_ref,
                                             remote_reader_ref_info):
        cdef:
            CObjectID c_writer_ref = writer_ref.native()
            c_vector[CNodeID] c_remote_reader_nodes
            c_vector[CReaderRefInfo] c_remote_reader_ref_info
            CReaderRefInfo c_reader_ref_info

        for node_id, reader_ref_info in remote_reader_ref_info.items():
            c_reader_ref_info = CReaderRefInfo()
            c_reader_ref_info.reader_ref_id = (
                <ObjectRef>reader_ref_info.reader_ref).native()
            c_reader_ref_info.owner_reader_actor_id = (
                <ActorID>reader_ref_info.ref_owner_actor_id).native()
            num_reader_actors = reader_ref_info.num_reader_actors
            assert num_reader_actors != 0
            c_reader_ref_info.num_reader_actors = num_reader_actors
            c_remote_reader_ref_info.push_back(c_reader_ref_info)
            c_remote_reader_nodes.push_back(CNodeID.FromHex(node_id))

        with nogil:
            CCoreWorkerProcess.GetCoreWorker().ExperimentalRegisterMutableObjectWriter(
                    c_writer_ref,
                    c_remote_reader_nodes,
            )
            check_status(
                    CCoreWorkerProcess.GetCoreWorker()
                    .ExperimentalRegisterMutableObjectReaderRemote(
                        c_writer_ref,
                        c_remote_reader_ref_info,
                    ))

    def experimental_channel_register_reader(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker()
                .ExperimentalRegisterMutableObjectReader(c_object_id))

    def put_object(
            self,
            serialized_object,
            *,
            c_bool pin_object,
            owner_address,
            c_bool inline_small_object,
            c_bool _is_experimental_channel,
            tensor_transport: Optional[str] = None,
    ):
        """Create an object reference with the current worker as the owner.
        """
        cdef:
            optional[c_string] c_tensor_transport = NULL_TENSOR_TRANSPORT
            c_string c_tensor_transport_str

        if tensor_transport is not None:
            c_tensor_transport_str = tensor_transport.encode()
            c_tensor_transport.emplace(move(c_tensor_transport_str))

        created_object = self.put_serialized_object_and_increment_local_ref(
            serialized_object, c_tensor_transport, pin_object, owner_address, inline_small_object, _is_experimental_channel)
        if owner_address is None:
            owner_address = CCoreWorkerProcess.GetCoreWorker().GetRpcAddress().SerializeAsString()

        # skip_adding_local_ref is True because it's already added through the call to
        # put_serialized_object_and_increment_local_ref.
        return ObjectRef(
            created_object,
            owner_address,
            skip_adding_local_ref=True,
            tensor_transport=tensor_transport
        )

    cdef put_serialized_object_and_increment_local_ref(
            self,
            serialized_object,
            optional[c_string] c_tensor_transport,
            c_bool pin_object=True,
            owner_address=None,
            c_bool inline_small_object=True,
            c_bool _is_experimental_channel=False,
        ):
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data
            c_vector[CObjectReference] contained_object_refs
            shared_ptr[CBuffer] metadata = string_to_buffer(
                serialized_object.metadata)
            unique_ptr[CAddress] c_owner_address = self._convert_python_address(
                owner_address)
            c_vector[CObjectID] contained_object_ids = ObjectRefsToVector(
                serialized_object.contained_object_refs)
            size_t total_bytes = serialized_object.total_bytes

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                .CreateOwnedAndIncrementLocalRef(
                    _is_experimental_channel,
                    metadata,
                    total_bytes,
                    contained_object_ids,
                    &c_object_id,
                    &data,
                    c_owner_address,
                    inline_small_object,
                    c_tensor_transport))

        if (data.get() == NULL):
            # Object already exists
            return c_object_id.Binary()

        logger.debug(
            f"Serialized object size of {c_object_id.Hex()} is {total_bytes} bytes")

        if total_bytes > 0:
            (<SerializedObject>serialized_object).write_to(
                Buffer.make(data))
        if self.is_local_mode:
            contained_object_refs = (
                    CCoreWorkerProcess.GetCoreWorker().
                    GetObjectRefs(contained_object_ids))
            if owner_address is not None:
                raise Exception(
                    "cannot put data into memory store directly"
                    " and assign owner at the same time")
            check_status(CCoreWorkerProcess.GetCoreWorker().Put(
                    CRayObject(data, metadata, contained_object_refs),
                    contained_object_ids, c_object_id))
        else:
            with nogil:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().SealOwned(
                                c_object_id,
                                pin_object,
                                move(c_owner_address)))
        return c_object_id.Binary()

    def wait(self,
             object_refs_or_generators,
             int num_returns,
             int64_t timeout_ms,
             c_bool fetch_local):
        cdef:
            c_vector[CObjectID] wait_ids
            c_vector[c_bool] results

        object_refs = []
        for ref_or_generator in object_refs_or_generators:
            if (not isinstance(ref_or_generator, ObjectRef)
                    and not isinstance(ref_or_generator, ObjectRefGenerator)):
                raise TypeError(
                    "wait() expected a list of ray.ObjectRef "
                    "or ObjectRefGenerator, "
                    f"got list containing {type(ref_or_generator)}"
                )

            if isinstance(ref_or_generator, ObjectRefGenerator):
                # Before calling wait,
                # get the next reference from a generator.
                object_refs.append(ref_or_generator._get_next_ref())
            else:
                object_refs.append(ref_or_generator)

        wait_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = CCoreWorkerProcess.GetCoreWorker().Wait(
                wait_ids, num_returns, timeout_ms, &results, fetch_local)
        check_status(op_status)

        assert len(results) == len(object_refs)

        ready, not_ready = [], []
        for i, object_ref_or_generator in enumerate(object_refs_or_generators):
            if results[i]:
                ready.append(object_ref_or_generator)
            else:
                not_ready.append(object_ref_or_generator)

        return ready, not_ready

    def free_objects(self, object_refs, c_bool local_only):
        cdef:
            c_vector[CObjectID] free_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().
                         Delete(free_ids, local_only))

    def get_local_ongoing_lineage_reconstruction_tasks(self):
        cdef:
            unordered_map[CLineageReconstructionTask, uint64_t] tasks
            unordered_map[CLineageReconstructionTask, uint64_t].iterator it

        with nogil:
            tasks = (CCoreWorkerProcess.GetCoreWorker().
                     GetLocalOngoingLineageReconstructionTasks())

        result = []
        it = tasks.begin()
        while it != tasks.end():
            task = common_pb2.LineageReconstructionTask()
            task.ParseFromString(dereference(it).first.SerializeAsString())
            result.append((task, dereference(it).second))
            postincrement(it)

        return result

    def get_local_object_locations(self, object_refs):
        cdef:
            c_vector[optional[CObjectLocation]] results
            c_vector[CObjectID] lookup_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetLocalObjectLocations(
                    lookup_ids, &results))

        object_locations = {}
        for i in range(results.size()):
            # core_worker will return a nullptr for objects that couldn't be
            # located
            if not results[i].has_value():
                continue
            else:
                object_locations[object_refs[i]] = \
                    CObjectLocationPtrToDict(&results[i].value())
        return object_locations

    def get_object_locations(self, object_refs, int64_t timeout_ms):
        cdef:
            c_vector[shared_ptr[CObjectLocation]] results
            c_vector[CObjectID] lookup_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetLocationFromOwner(
                    lookup_ids, timeout_ms, &results))

        object_locations = {}
        for i in range(results.size()):
            # core_worker will return a nullptr for objects that couldn't be
            # located
            if not results[i].get():
                continue
            else:
                object_locations[object_refs[i]] = \
                    CObjectLocationPtrToDict(results[i].get())
        return object_locations

    def global_gc(self):
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().TriggerGlobalGC()

    def log_plasma_usage(self):
        """Logs the current usage of the Plasma Store.
        Makes an unretriable blocking IPC to the Plasma Store.

        Raises an error if cannot connect to the Plasma Store. This should
        be fatal for the worker.
        """
        cdef:
            c_string result
        status = CCoreWorkerProcess.GetCoreWorker().GetPlasmaUsage(result)
        check_status(status)
        logger.warning("Plasma Store Usage:\n{}\n".format(
            result.decode("utf-8")))

    def get_memory_store_size(self):
        return CCoreWorkerProcess.GetCoreWorker().GetMemoryStoreSize()

    cdef python_label_match_expressions_to_c(
            self, python_expressions,
            CLabelMatchExpressions *c_expressions):
        cdef:
            CLabelMatchExpression* c_expression
            CLabelIn * c_label_in
            CLabelNotIn * c_label_not_in

        for expression in python_expressions:
            c_expression = c_expressions[0].add_expressions()
            c_expression.set_key(expression.key)
            if isinstance(expression.operator, In):
                c_label_in = c_expression.mutable_operator_()[0].mutable_label_in()
                for value in expression.operator.values:
                    c_label_in[0].add_values(value)
            elif isinstance(expression.operator, NotIn):
                c_label_not_in = \
                    c_expression.mutable_operator_()[0].mutable_label_not_in()
                for value in expression.operator.values:
                    c_label_not_in[0].add_values(value)
            elif isinstance(expression.operator, Exists):
                c_expression.mutable_operator_()[0].mutable_label_exists()
            elif isinstance(expression.operator, DoesNotExist):
                c_expression.mutable_operator_()[0].mutable_label_does_not_exist()

    cdef python_scheduling_strategy_to_c(
            self, python_scheduling_strategy,
            CSchedulingStrategy *c_scheduling_strategy):
        cdef:
            CPlacementGroupSchedulingStrategy \
                *c_placement_group_scheduling_strategy
            CNodeAffinitySchedulingStrategy *c_node_affinity_scheduling_strategy
            CNodeLabelSchedulingStrategy *c_node_label_scheduling_strategy
        assert python_scheduling_strategy is not None
        if python_scheduling_strategy == "DEFAULT":
            c_scheduling_strategy[0].mutable_default_scheduling_strategy()
        elif python_scheduling_strategy == "SPREAD":
            c_scheduling_strategy[0].mutable_spread_scheduling_strategy()
        elif isinstance(python_scheduling_strategy,
                        PlacementGroupSchedulingStrategy):
            c_placement_group_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_placement_group_scheduling_strategy()
            c_placement_group_scheduling_strategy[0].set_placement_group_id(
                python_scheduling_strategy
                .placement_group.id.binary())
            c_placement_group_scheduling_strategy[0] \
                .set_placement_group_bundle_index(
                    python_scheduling_strategy.placement_group_bundle_index)
            c_placement_group_scheduling_strategy[0]\
                .set_placement_group_capture_child_tasks(
                    python_scheduling_strategy
                    .placement_group_capture_child_tasks)
        elif isinstance(python_scheduling_strategy,
                        NodeAffinitySchedulingStrategy):
            c_node_affinity_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_node_affinity_scheduling_strategy()
            c_node_affinity_scheduling_strategy[0].set_node_id(
                NodeID.from_hex(python_scheduling_strategy.node_id).binary())
            c_node_affinity_scheduling_strategy[0].set_soft(
                python_scheduling_strategy.soft)
            c_node_affinity_scheduling_strategy[0].set_spill_on_unavailable(
                python_scheduling_strategy._spill_on_unavailable)
            c_node_affinity_scheduling_strategy[0].set_fail_on_unavailable(
                python_scheduling_strategy._fail_on_unavailable)
        elif isinstance(python_scheduling_strategy,
                        NodeLabelSchedulingStrategy):
            c_node_label_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_node_label_scheduling_strategy()
            self.python_label_match_expressions_to_c(
                python_scheduling_strategy.hard,
                c_node_label_scheduling_strategy[0].mutable_hard())
            self.python_label_match_expressions_to_c(
                python_scheduling_strategy.soft,
                c_node_label_scheduling_strategy[0].mutable_soft())
        else:
            raise ValueError(
                f"Invalid scheduling_strategy value "
                f"{python_scheduling_strategy}. "
                f"Valid values are [\"DEFAULT\""
                f" | \"SPREAD\""
                f" | PlacementGroupSchedulingStrategy"
                f" | NodeAffinitySchedulingStrategy]")

    def submit_task(self,
                    Language language,
                    FunctionDescriptor function_descriptor,
                    args,
                    c_string name,
                    int num_returns,
                    resources,
                    int max_retries,
                    c_bool retry_exceptions,
                    retry_exception_allowlist,
                    scheduling_strategy,
                    c_string debugger_breakpoint,
                    c_string serialized_runtime_env_info,
                    int64_t generator_backpressure_num_objects,
                    c_bool enable_task_events,
                    labels,
                    label_selector,
                    fallback_strategy):
        cdef:
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_vector[CFallbackOption] c_fallback_strategy
            CRayFunction ray_function
            CTaskOptions task_options
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            CSchedulingStrategy c_scheduling_strategy
            c_vector[CObjectID] incremented_put_arg_ids
            c_string serialized_retry_exception_allowlist
            CTaskID current_c_task_id
            TaskID current_task = self.get_current_task_id()
            c_string call_site

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        if RayConfig.instance().record_task_actor_creation_sites():
            # TODO(ryw): unify with get_py_stack used by record_ref_creation_sites.
            call_site = ''.join(traceback.format_stack())

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_labels(labels, &c_labels)
            prepare_label_selector(label_selector, &c_label_selector)
            prepare_fallback_strategy(fallback_strategy, &c_fallback_strategy)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)

            task_options = CTaskOptions(
                name, num_returns, c_resources,
                b"",
                generator_backpressure_num_objects,
                serialized_runtime_env_info,
                enable_task_events,
                c_labels,
                c_label_selector,
                # `tensor_transport` is currently only supported in Ray Actor tasks.
                NULL_TENSOR_TRANSPORT,
                c_fallback_strategy)

            current_c_task_id = current_task.native()

            with nogil:
                return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
                    ray_function, args_vector, task_options,
                    max_retries, retry_exceptions,
                    c_scheduling_strategy,
                    debugger_breakpoint,
                    serialized_retry_exception_allowlist,
                    call_site,
                    current_c_task_id,
                )

            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            # The initial local reference is already acquired internally when
            # adding the pending task.
            return VectorToObjectRefs(return_refs, skip_adding_local_ref=True)

    def create_actor(self,
                     Language language,
                     FunctionDescriptor function_descriptor,
                     args,
                     int64_t max_restarts,
                     int64_t max_task_retries,
                     resources,
                     placement_resources,
                     int32_t max_concurrency,
                     is_detached,
                     c_string name,
                     c_string ray_namespace,
                     c_bool is_asyncio,
                     c_string extension_data,
                     c_string serialized_runtime_env_info,
                     concurrency_groups_dict,
                     int32_t max_pending_calls,
                     scheduling_strategy,
                     c_bool enable_task_events,
                     labels,
                     label_selector,
                     c_bool allow_out_of_order_execution,
                     c_bool enable_tensor_transport,
                     fallback_strategy,
                     ):
        cdef:
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[c_string] dynamic_worker_options
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, double] c_placement_resources
            CActorID c_actor_id
            c_vector[CConcurrencyGroup] c_concurrency_groups
            CSchedulingStrategy c_scheduling_strategy
            c_vector[CObjectID] incremented_put_arg_ids
            optional[c_bool] is_detached_optional = nullopt
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_vector[CFallbackOption] c_fallback_strategy
            c_string call_site

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        if RayConfig.instance().record_task_actor_creation_sites():
            # TODO(ryw): unify with get_py_stack used by record_ref_creation_sites.
            call_site = ''.join(traceback.format_stack())

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            prepare_labels(labels, &c_labels)
            prepare_label_selector(label_selector, &c_label_selector)
            prepare_fallback_strategy(fallback_strategy, &c_fallback_strategy)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)
            prepare_actor_concurrency_groups(
                concurrency_groups_dict, &c_concurrency_groups)

            if is_detached is not None:
                is_detached_optional = make_optional[c_bool](
                    True if is_detached else False)

            with nogil:
                status = CCoreWorkerProcess.GetCoreWorker().CreateActor(
                    ray_function, args_vector,
                    CActorCreationOptions(
                        max_restarts, max_task_retries, max_concurrency,
                        c_resources, c_placement_resources,
                        dynamic_worker_options, is_detached_optional, name,
                        ray_namespace,
                        is_asyncio,
                        c_scheduling_strategy,
                        serialized_runtime_env_info,
                        c_concurrency_groups,
                        allow_out_of_order_execution,
                        max_pending_calls,
                        enable_tensor_transport,
                        enable_task_events,
                        c_labels,
                        c_label_selector,
                        c_fallback_strategy),
                    extension_data,
                    call_site,
                    &c_actor_id,
                )

            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            check_status(status)

            return ActorID(c_actor_id.Binary())

    def create_placement_group(
                            self,
                            c_string name,
                            c_vector[unordered_map[c_string, double]] bundles,
                            c_string strategy,
                            c_bool is_detached,
                            soft_target_node_id,
                            c_vector[unordered_map[c_string, c_string]] bundle_label_selector):
        cdef:
            CPlacementGroupID c_placement_group_id
            CPlacementStrategy c_strategy
            CNodeID c_soft_target_node_id = CNodeID.Nil()

        if strategy == b"PACK":
            c_strategy = PLACEMENT_STRATEGY_PACK
        elif strategy == b"SPREAD":
            c_strategy = PLACEMENT_STRATEGY_SPREAD
        elif strategy == b"STRICT_PACK":
            c_strategy = PLACEMENT_STRATEGY_STRICT_PACK
        else:
            if strategy == b"STRICT_SPREAD":
                c_strategy = PLACEMENT_STRATEGY_STRICT_SPREAD
            else:
                raise TypeError(strategy)

        if soft_target_node_id is not None:
            c_soft_target_node_id = CNodeID.FromHex(soft_target_node_id)

        with nogil:
            check_status(
                        CCoreWorkerProcess.GetCoreWorker().
                        CreatePlacementGroup(
                            CPlacementGroupCreationOptions(
                                name,
                                c_strategy,
                                bundles,
                                is_detached,
                                c_soft_target_node_id,
                                bundle_label_selector),
                            &c_placement_group_id))

        return PlacementGroupID(c_placement_group_id.Binary())

    def remove_placement_group(self, PlacementGroupID placement_group_id):
        cdef:
            CPlacementGroupID c_placement_group_id = \
                placement_group_id.native()

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().
                RemovePlacementGroup(c_placement_group_id))

    def wait_placement_group_ready(self,
                                   PlacementGroupID placement_group_id,
                                   int64_t timeout_seconds):
        cdef CRayStatus status
        cdef CPlacementGroupID cplacement_group_id = (
            CPlacementGroupID.FromBinary(placement_group_id.binary()))
        cdef int64_t ctimeout_seconds = timeout_seconds
        with nogil:
            status = CCoreWorkerProcess.GetCoreWorker() \
                .WaitPlacementGroupReady(cplacement_group_id, ctimeout_seconds)
            if status.IsNotFound():
                raise Exception("Placement group {} does not exist.".format(
                    placement_group_id))
        return status.ok()

    def submit_actor_task(self,
                          Language language,
                          ActorID actor_id,
                          FunctionDescriptor function_descriptor,
                          args,
                          c_string name,
                          int num_returns,
                          int max_retries,
                          c_bool retry_exceptions,
                          retry_exception_allowlist,
                          double num_method_cpus,
                          c_string concurrency_group_name,
                          int64_t generator_backpressure_num_objects,
                          c_bool enable_task_events,
                          tensor_transport: Optional[str]):

        cdef:
            CActorID c_actor_id = actor_id.native()
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            c_vector[CObjectID] incremented_put_arg_ids
            CTaskID current_c_task_id = CTaskID.Nil()
            TaskID current_task = self.get_current_task_id()
            c_string serialized_retry_exception_allowlist
            c_string serialized_runtime_env = b"{}"
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_string call_site
            c_vector[CFallbackOption] c_fallback_strategy
            optional[c_string] c_tensor_transport = NULL_TENSOR_TRANSPORT
            c_string c_tensor_transport_str

        if tensor_transport is not None:
            c_tensor_transport_str = tensor_transport.encode("utf-8")
            c_tensor_transport.emplace(move(c_tensor_transport_str))

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        if RayConfig.instance().record_task_actor_creation_sites():
            call_site = ''.join(traceback.format_stack())

        with self.profile_event(b"submit_task"):
            if num_method_cpus > 0:
                c_resources[b"CPU"] = num_method_cpus
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)

            current_c_task_id = current_task.native()

            with nogil:
                status = CCoreWorkerProcess.GetCoreWorker().SubmitActorTask(
                    c_actor_id,
                    ray_function,
                    args_vector,
                    CTaskOptions(
                        name,
                        num_returns,
                        c_resources,
                        concurrency_group_name,
                        generator_backpressure_num_objects,
                        serialized_runtime_env,
                        enable_task_events,
                        c_labels,
                        c_label_selector,
                        c_tensor_transport,
                        c_fallback_strategy),
                    max_retries,
                    retry_exceptions,
                    serialized_retry_exception_allowlist,
                    call_site,
                    return_refs,
                    current_c_task_id,
                )

            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            if status.ok():
                # The initial local reference is already acquired internally
                # when adding the pending task.
                return VectorToObjectRefs(return_refs,
                                          skip_adding_local_ref=True)
            else:
                if status.IsOutOfResource():
                    actor = self.get_actor_handle(actor_id)
                    actor_handle = (CCoreWorkerProcess.GetCoreWorker()
                                    .GetActorHandle(c_actor_id))
                    raise PendingCallsLimitExceeded(
                        f"The task {function_descriptor.function_name} could not be "
                        f"submitted to {repr(actor)} because more than"
                        f" {(dereference(actor_handle).MaxPendingCalls())}"
                        " tasks are queued on the actor. This limit can be adjusted"
                        " with the `max_pending_calls` actor option.")
                else:
                    raise Exception(f"Failed to submit task to actor {actor_id} "
                                    f"due to {status.message()}")

    def kill_actor(self, ActorID actor_id, c_bool no_restart):
        cdef:
            CActorID c_actor_id = actor_id.native()
            CRayStatus status = CRayStatus.OK()

        with nogil:
            status = CCoreWorkerProcess.GetCoreWorker().KillActor(
                c_actor_id, True, no_restart)

        if status.IsNotFound():
            raise ActorHandleNotFoundError(status.message().decode())

        check_status(status)

    def cancel_task(self, ObjectRef object_ref, c_bool force_kill,
                    c_bool recursive):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CRayStatus status = CRayStatus.OK()

        with nogil:
            status = CCoreWorkerProcess.GetCoreWorker().CancelTask(
                                            c_object_id, force_kill, recursive)

        if status.IsInvalidArgument():
            raise ValueError(status.message().decode())

        if not status.ok():
            raise TypeError(status.message().decode())

    def is_canceled(self):
        """Check if the current task has been canceled.

        Returns:
            True if the current task has been canceled, False otherwise.
        """
        cdef:
            CTaskID c_task_id
            c_bool is_canceled
            TaskID task_id

        # Get the current task ID
        task_id = self.get_current_task_id()
        c_task_id = task_id.native()

        with nogil:
            is_canceled = CCoreWorkerProcess.GetCoreWorker().IsTaskCanceled(c_task_id)

        return is_canceled

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = (
                CCoreWorkerProcess.GetCoreWorker().GetResourceIDs())
            unordered_map[
                c_string, c_vector[pair[int64_t, double]]
            ].iterator iterator = resource_mapping.begin()
            c_vector[pair[int64_t, double]] c_value

        resources_dict = {}
        while iterator != resource_mapping.end():
            key = decode(dereference(iterator).first)
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append(
                    (c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            postincrement(iterator)

        return resources_dict

    def profile_event(self, c_string event_type, object extra_data=None):
        if RayConfig.instance().enable_timeline():
            return ProfileEvent.make(
                CCoreWorkerProcess.GetCoreWorker().CreateProfileEvent(
                    event_type), extra_data)
        else:
            return EmptyProfileEvent()

    def remove_actor_handle_reference(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
        CCoreWorkerProcess.GetCoreWorker().RemoveActorHandleReference(
            c_actor_id)

    def get_local_actor_state(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
            optional[int] state = nullopt
        state = CCoreWorkerProcess.GetCoreWorker().GetLocalActorState(c_actor_id)
        if state.has_value():
            return state.value()
        else:
            return None

    cdef make_actor_handle(self, ActorHandleSharedPtr c_actor_handle,
                           c_bool weak_ref):
        worker = ray._private.worker.global_worker
        worker.check_connected()
        manager = worker.function_actor_manager

        actor_id = ActorID(dereference(c_actor_handle).GetActorID().Binary())
        job_id = JobID(dereference(c_actor_handle).CreationJobID().Binary())
        language = Language.from_native(
            dereference(c_actor_handle).ActorLanguage())
        actor_creation_function_descriptor = CFunctionDescriptorToPython(
            dereference(c_actor_handle).ActorCreationTaskFunctionDescriptor())
        max_task_retries = dereference(c_actor_handle).MaxTaskRetries()
        enable_task_events = dereference(c_actor_handle).EnableTaskEvents()
        allow_out_of_order_execution = dereference(c_actor_handle).AllowOutOfOrderExecution()
        enable_tensor_transport = dereference(c_actor_handle).EnableTensorTransport()
        if language == Language.PYTHON:
            assert isinstance(actor_creation_function_descriptor,
                              PythonFunctionDescriptor)
            # Load actor_method_cpu from actor handle's extension data.
            extension_data = <str>dereference(c_actor_handle).ExtensionData()
            if extension_data:
                actor_method_cpu = int(extension_data)
            else:
                actor_method_cpu = 0  # Actor is created by non Python worker.
            actor_class = manager.load_actor_class(
                job_id, actor_creation_function_descriptor)
            method_meta = ray.actor._ActorClassMethodMetadata.create(
                actor_class, actor_creation_function_descriptor)
            return ray.actor.ActorHandle(language, actor_id, max_task_retries,
                                         enable_task_events,
                                         method_meta.method_is_generator,
                                         method_meta.decorators,
                                         method_meta.signatures,
                                         method_meta.num_returns,
                                         method_meta.max_task_retries,
                                         method_meta.retry_exceptions,
                                         method_meta.generator_backpressure_num_objects, # noqa
                                         method_meta.enable_task_events,
                                         enable_tensor_transport,
                                         method_meta.method_name_to_tensor_transport,
                                         actor_method_cpu,
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref,
                                         allow_out_of_order_execution=allow_out_of_order_execution)
        else:
            return ray.actor.ActorHandle(language, actor_id,
                                         0,   # max_task_retries,
                                         True,  # enable_task_events
                                         {},  # method is_generator
                                         {},  # method decorators
                                         {},  # method signatures
                                         {},  # method num_returns
                                         {},  # method max_task_retries
                                         {},  # method retry_exceptions
                                         {},  # generator_backpressure_num_objects
                                         {},  # enable_task_events
                                         False,  # enable_tensor_transport
                                         None,  # method_name_to_tensor_transport
                                         0,  # actor method cpu
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref,
                                         allow_out_of_order_execution=allow_out_of_order_execution,
                                         )

    def deserialize_and_register_actor_handle(self, const c_string &bytes,
                                              ObjectRef
                                              outer_object_ref,
                                              c_bool weak_ref):
        cdef:
            CObjectID c_outer_object_id = (outer_object_ref.native() if
                                           outer_object_ref else
                                           CObjectID.Nil())
        c_actor_id = (CCoreWorkerProcess
                      .GetCoreWorker()
                      .DeserializeAndRegisterActorHandle(
                          bytes, c_outer_object_id,
                          add_local_ref=not weak_ref))
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id),
            weak_ref)

    def get_named_actor_handle(self, const c_string &name,
                               const c_string &ray_namespace):
        cdef:
            pair[ActorHandleSharedPtr, CRayStatus] named_actor_handle_pair

        # We need it because GetNamedActorHandle needs
        # to call a method that holds the gil.
        with nogil:
            named_actor_handle_pair = (
                CCoreWorkerProcess.GetCoreWorker().GetNamedActorHandle(
                    name, ray_namespace))
        check_status(named_actor_handle_pair.second)

        return self.make_actor_handle(named_actor_handle_pair.first,
                                      weak_ref=True)

    def get_actor_handle(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id),
            weak_ref=True)

    def list_named_actors(self, c_bool all_namespaces):
        """Returns (namespace, name) for named actors in the system.

        If all_namespaces is True, returns all actors in all namespaces,
        else returns only the actors in the current namespace.
        """
        cdef:
            pair[c_vector[pair[c_string, c_string]], CRayStatus] result_pair

        with nogil:
            result_pair = CCoreWorkerProcess.GetCoreWorker().ListNamedActors(
                all_namespaces)
        check_status(result_pair.second)
        return [
            (namespace.decode("utf-8"),
             name.decode("utf-8")) for namespace, name in result_pair.first]

    def serialize_actor_handle(self, ActorID actor_id):
        cdef:
            c_string output
            CObjectID c_actor_handle_id
        check_status(CCoreWorkerProcess.GetCoreWorker().SerializeActorHandle(
            actor_id.native(), &output, &c_actor_handle_id))
        return output, ObjectRef(c_actor_handle_id.Binary())

    def add_object_ref_reference(self, ObjectRef object_ref):
        # Note: faster to not release GIL for short-running op.
        CCoreWorkerProcess.GetCoreWorker().AddLocalReference(
            object_ref.native())

    def remove_object_ref_reference(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
        # We need to release the gil since object destruction may call the
        # unhandled exception handler.
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                c_object_id)

    def get_owner_address(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CAddress c_owner_address
        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                c_object_id, &c_owner_address)
        check_status(op_status)
        return c_owner_address.SerializeAsString()

    def serialize_object_ref(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CAddress c_owner_address = CAddress()
            c_string serialized_object_status
        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(
                c_object_id, &c_owner_address, &serialized_object_status)
        check_status(op_status)
        return (object_ref,
                c_owner_address.SerializeAsString(),
                serialized_object_status)

    def deserialize_and_register_object_ref(
            self, const c_string &object_ref_binary,
            ObjectRef outer_object_ref,
            const c_string &serialized_owner_address,
            const c_string &serialized_object_status,
    ):
        cdef:
            CObjectID c_object_id = CObjectID.FromBinary(object_ref_binary)
            CObjectID c_outer_object_id = (outer_object_ref.native() if
                                           outer_object_ref else
                                           CObjectID.Nil())
            CAddress c_owner_address = CAddress()

        c_owner_address.ParseFromString(serialized_owner_address)
        (CCoreWorkerProcess.GetCoreWorker()
            .RegisterOwnershipInfoAndResolveFuture(
                c_object_id,
                c_outer_object_id,
                c_owner_address,
                serialized_object_status))

    cdef store_task_output(self, serialized_object, const CObjectID &return_id,
                           const CObjectID &generator_id,
                           size_t data_size, shared_ptr[CBuffer] &metadata,
                           const c_vector[CObjectID] &contained_id,
                           const CAddress &caller_address,
                           int64_t *task_output_inlined_bytes,
                           shared_ptr[CRayObject] *return_ptr):
        """Store a task return value in plasma or as an inlined object."""
        with nogil:
            # For objects that can't be inlined, return_ptr will only be set if
            # the object doesn't already exist in plasma.
            check_status(
                CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
                    return_id, data_size, metadata, contained_id, caller_address,
                    task_output_inlined_bytes, return_ptr))

        if return_ptr.get() != NULL:
            if return_ptr.get().HasData():
                (<SerializedObject>serialized_object).write_to(
                    Buffer.make(return_ptr.get().GetData()))
            if self.is_local_mode:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().Put(
                        CRayObject(return_ptr.get().GetData(),
                                   return_ptr.get().GetMetadata(),
                                   c_vector[CObjectReference]()),
                        c_vector[CObjectID](), return_id))
            else:
                with nogil:
                    check_status(
                        CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
                            return_id, return_ptr[0], generator_id, caller_address))
            return True
        else:
            with nogil:
                # Pins the object, succeeds if the object exists in plasma and is
                # sealed.
                success = (
                    CCoreWorkerProcess.GetCoreWorker().PinExistingReturnObject(
                        return_id, return_ptr, generator_id, caller_address))
            return success

    cdef store_task_outputs(self,
                            worker, outputs,
                            const CAddress &caller_address,
                            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]]
                            *returns,
                            ref_generator_id=None,
                            optional[c_string] c_tensor_transport=NULL_TENSOR_TRANSPORT):
        cdef:
            CObjectID return_id
            size_t data_size
            shared_ptr[CBuffer] metadata
            c_vector[CObjectID] contained_id
            int64_t task_output_inlined_bytes
            int64_t num_returns = -1
            CObjectID c_ref_generator_id = CObjectID.Nil()
            shared_ptr[CRayObject] *return_ptr
            c_string c_pickled_rdt_metadata

        if ref_generator_id:
            c_ref_generator_id = CObjectID.FromBinary(ref_generator_id)

        num_outputs_stored = 0
        if not c_ref_generator_id.IsNil():
            # The task specified a dynamic number of return values. Determine
            # the expected number of return values.
            if returns[0].size() > 0:
                # We are re-executing the task. We should return the same
                # number of objects as before.
                num_returns = returns[0].size()
            else:
                # This is the first execution of the task, so we don't know how
                # many return objects it should have yet.
                # NOTE(swang): returns could also be empty if the task returned
                # an empty generator and was re-executed. However, this should
                # not happen because we never reconstruct empty
                # DynamicObjectRefGenerators (since these aren't stored in plasma).
                num_returns = -1
        else:
            # The task specified how many return values it should have.
            num_returns = returns[0].size()

        if num_returns == 0:
            if outputs is not None and len(outputs) > 0:
                # Warn if num_returns=0 but the task returns a non-None value (likely unintended).
                task_name = self.get_current_task_name()
                obj_value = repr(outputs)
                warnings.warn(
                    f"Task '{task_name}' has num_returns=0 but returned a non-None value '{obj_value}'. "
                    "The return value will be ignored.",
                    NumReturnsWarning,
                    stacklevel=2
                )

            return num_outputs_stored

        tensor_transport = None
        if c_tensor_transport.has_value():
            tensor_transport = c_tensor_transport.value().decode("utf-8")
        task_output_inlined_bytes = 0
        i = -1
        for i, output in enumerate(outputs):
            if num_returns >= 0 and i >= num_returns:
                raise ValueError(
                    "Task returned more than num_returns={} objects.".format(
                        num_returns))
            # TODO(sang): Remove it when the streaming generator is
            # enabled by default.
            while i >= returns[0].size():
                return_id = (CCoreWorkerProcess.GetCoreWorker()
                             .AllocateDynamicReturnId(
                                caller_address, CTaskID.Nil(), NULL_PUT_INDEX))
                returns[0].push_back(
                        c_pair[CObjectID, shared_ptr[CRayObject]](
                            return_id, shared_ptr[CRayObject]()))
            assert i < returns[0].size()
            return_id = returns[0][i].first
            if returns[0][i].second == nullptr:
                returns[0][i].second = shared_ptr[CRayObject]()
            return_ptr = &returns[0][i].second

            # Skip return values that we already created.  This can occur if
            # there were multiple return values, and we initially errored while
            # trying to create one of them.
            if (return_ptr.get() != NULL and return_ptr.get().GetData().get()
                    != NULL):
                continue

            context = worker.get_serialization_context()

            # TODO(kevin85421): We should consider unifying both serialization logic in the future
            # when GPU objects are more stable. We currently separate the logic to ensure
            # GPU object-related logic does not affect the normal object serialization logic.
            if tensor_transport is not None:
                # `output` contains tensors. We need to retrieve these tensors from `output`
                # and store them in the GPUObjectManager.
                serialized_object, tensors = context.serialize_gpu_objects(output)
                pickled_rdt_metadata = context.store_gpu_objects(
                    return_id.Hex().decode("ascii"), tensors, tensor_transport)
                # One copy from python bytes object to C++ string
                c_pickled_rdt_metadata = pickled_rdt_metadata
            else:
                serialized_object = context.serialize(output)

            data_size = serialized_object.total_bytes
            metadata_str = serialized_object.metadata
            if ray._private.worker.global_worker.debugger_get_breakpoint:
                breakpoint = (
                    ray._private.worker.global_worker.debugger_get_breakpoint)
                metadata_str += (
                    b"," + ray_constants.OBJECT_METADATA_DEBUG_PREFIX +
                    breakpoint.encode())
                # Reset debugging context of this worker.
                ray._private.worker.global_worker.debugger_get_breakpoint = b""
            metadata = string_to_buffer(metadata_str)
            contained_id = ObjectRefsToVector(
                serialized_object.contained_object_refs)

            # It's possible for store_task_output to fail when the object already
            # exists, but we fail to pin it. We can fail to pin the object if
            # 1. it exists but isn't sealed yet because it's being written to by
            #    another worker. We'll keep looping until it's sealed.
            # 2. it existed during the allocation attempt but was evicted before
            #    the pin attempt. We'll allocate and write the second time.
            base_backoff_s = 1
            attempt = 1
            max_attempts = 6 # 6 attempts =~ 60 seconds of total backoff time
            while not self.store_task_output(
                    serialized_object,
                    return_id,
                    c_ref_generator_id,
                    data_size,
                    metadata,
                    contained_id,
                    caller_address,
                    &task_output_inlined_bytes,
                    return_ptr):
                if (attempt > max_attempts):
                    raise RaySystemError(
                        "Failed to store task output with object id {} after {} attempts.".format(
                            return_id.Hex().decode("ascii"),
                            max_attempts))
                time.sleep(base_backoff_s * (2 ** (attempt-1)))
                attempt += 1
                continue

            if tensor_transport is not None:
                return_ptr.get().SetDirectTransportMetadata(move(c_pickled_rdt_metadata))
                c_pickled_rdt_metadata = c_string()

            num_outputs_stored += 1

        i += 1
        if i < num_returns:
            raise ValueError(
                    "Task returned {} objects, but num_returns={}.".format(
                        i, num_returns))

        return num_outputs_stored

    cdef c_function_descriptors_to_python(
            self,
            const c_vector[CFunctionDescriptor] &c_function_descriptors):

        ret = []
        for i in range(c_function_descriptors.size()):
            ret.append(CFunctionDescriptorToPython(c_function_descriptors[i]))
        return ret

    cdef initialize_eventloops_for_actor_concurrency_group(
            self,
            const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups):

        cdef:
            CConcurrencyGroup c_concurrency_group

        self.cgname_to_eventloop_dict = {}
        self.fd_to_cgname_dict = {}

        self.eventloop_for_default_cg = get_new_event_loop()
        self.thread_for_default_cg = threading.Thread(
            target=lambda: self.eventloop_for_default_cg.run_forever(),
            name="AsyncIO Thread: default"
            )
        self.thread_for_default_cg.start()

        for i in range(c_defined_concurrency_groups.size()):
            c_concurrency_group = c_defined_concurrency_groups[i]
            cg_name = c_concurrency_group.GetName().decode("ascii")
            function_descriptors = self.c_function_descriptors_to_python(
                c_concurrency_group.GetFunctionDescriptors())

            async_eventloop = get_new_event_loop()
            async_thread = threading.Thread(
                target=lambda: async_eventloop.run_forever(),
                name="AsyncIO Thread: {}".format(cg_name)
            )
            async_thread.start()

            self.cgname_to_eventloop_dict[cg_name] = {
                "eventloop": async_eventloop,
                "thread": async_thread,
            }

            for fd in function_descriptors:
                self.fd_to_cgname_dict[fd] = cg_name

    def get_event_loop_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if self.event_loop_executor is None:
            # NOTE: We're deliberately allocating thread-pool executor with
            #       a single thread, provided that many of its use-cases are
            #       not thread-safe yet (for ex, reporting streaming generator output)
            self.event_loop_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        return self.event_loop_executor

    def reset_event_loop_executor(self, executor: concurrent.futures.ThreadPoolExecutor):
        self.event_loop_executor = executor

    def get_event_loop(self, function_descriptor, specified_cgname):
        # __init__ will be invoked in default eventloop
        if function_descriptor.function_name == "__init__":
            return self.eventloop_for_default_cg, self.thread_for_default_cg

        if specified_cgname is not None:
            if specified_cgname in self.cgname_to_eventloop_dict:
                this_group = self.cgname_to_eventloop_dict[specified_cgname]
                return (this_group["eventloop"], this_group["thread"])

        if function_descriptor in self.fd_to_cgname_dict:
            curr_cgname = self.fd_to_cgname_dict[function_descriptor]
            if curr_cgname in self.cgname_to_eventloop_dict:
                return (
                    self.cgname_to_eventloop_dict[curr_cgname]["eventloop"],
                    self.cgname_to_eventloop_dict[curr_cgname]["thread"])
            else:
                raise ValueError(
                    "The function {} is defined to be executed "
                    "in the concurrency group {} . But there is no this group."
                    .format(function_descriptor, curr_cgname))

        return self.eventloop_for_default_cg, self.thread_for_default_cg

    def run_async_func_or_coro_in_event_loop(
          self,
          func_or_coro: Union[Callable[[Any, Any], Awaitable[Any]], Awaitable],
          function_descriptor: FunctionDescriptor,
          specified_cgname: str,
          *,
          task_id: Optional[TaskID] = None,
          task_name: Optional[str] = None,
          func_args: Optional[Tuple] = None,
          func_kwargs: Optional[Dict] = None,
    ):
        """Run the async function or coroutine to the event loop.

        The event loop is running in a separate thread.

        Args:
            func_or_coro: Async function (not a generator) or awaitable objects.
            function_descriptor: The function descriptor.
            specified_cgname: The name of a concurrent group.
            task_id: The task ID to track the future. If None is provided
                the future is not tracked with a task ID.
                (e.g., When we deserialize the arguments, we don't want to
                track the task_id -> future mapping).
            func_args: The arguments for the async function.
            func_kwargs: The keyword arguments for the async function.

        NOTE: func_args and func_kwargs are intentionally passed as a tuple/dict and
        not unpacked to avoid collisions between system arguments and user-provided
        arguments. See https://github.com/ray-project/ray/issues/41272.
        """
        cdef:
            CFiberEvent event

        if func_args is None:
            func_args = tuple()
        if func_kwargs is None:
            func_kwargs = dict()

        # Increase recursion limit if necessary. In asyncio mode,
        # we have many parallel callstacks (represented in fibers)
        # that's suspended for execution. Python interpreter will
        # mistakenly count each callstack towards recusion limit.
        # We don't need to worry about stackoverflow here because
        # the max number of callstacks is limited in direct actor
        # transport with max_concurrency flag.
        increase_recursion_limit()

        eventloop, _ = self.get_event_loop(
            function_descriptor, specified_cgname)

        async def async_func():
            try:
                if task_id:
                    async_task_id.set(task_id)
                if task_name is not None:
                    async_task_name.set(task_name)
                async_task_function_name.set(function_descriptor.repr)

                if inspect.isawaitable(func_or_coro):
                    coroutine = func_or_coro
                else:
                    coroutine = func_or_coro(*func_args, **func_kwargs)

                return await coroutine
            finally:
                event.Notify()

        future = asyncio.run_coroutine_threadsafe(async_func(), eventloop)
        if task_id:
            with self._task_id_to_future_lock:
                self._task_id_to_future[task_id] = future

        with nogil:
            (CCoreWorkerProcess.GetCoreWorker()
                .YieldCurrentFiber(event))
        try:
            result = future.result()
        except concurrent.futures.CancelledError:
            raise TaskCancelledError(task_id)
        finally:
            if task_id:
                with self._task_id_to_future_lock:
                    self._task_id_to_future.pop(task_id)
        return result

    def stop_and_join_asyncio_threads_if_exist(self):
        event_loops = []
        threads = []
        if self.event_loop_executor:
            self.event_loop_executor.shutdown(
                wait=True, cancel_futures=True)
        if self.eventloop_for_default_cg is not None:
            event_loops.append(self.eventloop_for_default_cg)
        if self.thread_for_default_cg is not None:
            threads.append(self.thread_for_default_cg)
        if self.cgname_to_eventloop_dict:
            for event_loop_and_thread in self.cgname_to_eventloop_dict.values():
                event_loops.append(event_loop_and_thread["eventloop"])
                threads.append(event_loop_and_thread["thread"])
        for event_loop in event_loops:
            event_loop.call_soon_threadsafe(
                event_loop.stop)
        for thread in threads:
            thread.join()

    def current_actor_is_asyncio(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorIsAsync())

    def set_current_actor_should_exit(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .SetCurrentActorShouldExit())

    def get_current_actor_should_exit(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .GetCurrentActorShouldExit())

    def current_actor_max_concurrency(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorMaxConcurrency())

    def get_current_root_detached_actor_id(self) -> ActorID:
        # This is only used in test
        return ActorID(CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                       .GetRootDetachedActorID().Binary())

    def get_future_for_running_task(self, task_id: Optional[TaskID]) -> Optional[concurrent.futures.Future]:
        """Get the future corresponding to a running task (or None).

        The underyling asyncio task might be queued, running, or completed.
        """
        with self._task_id_to_future_lock:
            return self._task_id_to_future.get(task_id)

    def get_current_runtime_env(self) -> str:
        # This should never change, so we can safely cache it to avoid ser/de
        if self.current_runtime_env is None:
            if self.is_driver:
                job_config = self.get_job_config()
                serialized_env = job_config.runtime_env_info \
                                           .serialized_runtime_env
            else:
                serialized_env = CCoreWorkerProcess.GetCoreWorker() \
                        .GetWorkerContext().GetCurrentSerializedRuntimeEnv() \
                        .decode("utf-8")

            self.current_runtime_env = serialized_env

        return self.current_runtime_env

    def trigger_gc(self):
        self._gc_thread.trigger_gc()

    def get_pending_children_task_ids(self, parent_task_id: TaskID):
        cdef:
            CTaskID c_parent_task_id = parent_task_id.native()
            c_vector[CTaskID] ret
            c_vector[CTaskID].iterator it

        result = []

        with nogil:
            ret = CCoreWorkerProcess.GetCoreWorker().GetPendingChildrenTasks(
                c_parent_task_id)

        it = ret.begin()
        while it != ret.end():
            result.append(TaskID(dereference(it).Binary()))
            postincrement(it)

        return result

    def get_all_reference_counts(self):
        cdef:
            unordered_map[CObjectID, pair[size_t, size_t]] c_ref_counts
            unordered_map[CObjectID, pair[size_t, size_t]].iterator it

        c_ref_counts = (
            CCoreWorkerProcess.GetCoreWorker().GetAllReferenceCounts())
        it = c_ref_counts.begin()

        ref_counts = {}
        while it != c_ref_counts.end():
            object_ref = dereference(it).first.Hex()
            ref_counts[object_ref] = {
                "local": dereference(it).second.first,
                "submitted": dereference(it).second.second}
            postincrement(it)

        return ref_counts

    def set_get_async_callback(self, ObjectRef object_ref, user_callback: Callable):
        # NOTE: we need to manually increment the Python reference count to avoid the
        # callback object being garbage collected before it's called by the core worker.
        # This means we *must* guarantee that the ref is manually decremented to avoid
        # a leak.
        cpython.Py_INCREF(user_callback)
        CCoreWorkerProcess.GetCoreWorker().GetAsync(
            object_ref.native(),
            async_callback,
            <void*>user_callback
        )

    def push_error(self, JobID job_id, error_type, error_message,
                   double timestamp):
        check_status(CCoreWorkerProcess.GetCoreWorker().PushError(
            job_id.native(), error_type.encode("utf-8"),
            error_message.encode("utf-8"), timestamp))

    def get_job_config(self):
        cdef CJobConfig c_job_config
        # We can cache the deserialized job config object here because
        # the job config will not change after a job is submitted.
        if self.job_config is None:
            c_job_config = CCoreWorkerProcess.GetCoreWorker().GetJobConfig()
            self.job_config = common_pb2.JobConfig()
            self.job_config.ParseFromString(c_job_config.SerializeAsString())
        return self.job_config

    def get_local_memory_store_bytes_used(self):
        cdef:
            int64_t num_bytes_used

        with nogil:
            num_bytes_used = (
                    CCoreWorkerProcess.GetCoreWorker().GetLocalMemoryStoreBytesUsed())
        return num_bytes_used

    def record_task_log_start(
            self, task_id: TaskID, int attempt_number,
            stdout_path, stderr_path,
            int64_t out_start_offset, int64_t err_start_offset):
        cdef:
            CTaskID c_task_id = task_id.native()
            c_string c_stdout_path = stdout_path.encode("utf-8")
            c_string c_stderr_path = stderr_path.encode("utf-8")

        with nogil:
            CCoreWorkerProcess.GetCoreWorker() \
                .RecordTaskLogStart(c_task_id, attempt_number,
                                    c_stdout_path, c_stderr_path,
                                    out_start_offset, err_start_offset)

    def record_task_log_end(
            self, task_id: TaskID, int attempt_number,
            int64_t out_end_offset, int64_t err_end_offset):
        cdef:
            CTaskID c_task_id = task_id.native()

        with nogil:
            CCoreWorkerProcess.GetCoreWorker() \
                .RecordTaskLogEnd(c_task_id, attempt_number,
                                  out_end_offset, err_end_offset)

    cdef CObjectID allocate_dynamic_return_id_for_generator(
            self,
            const CAddress &owner_address,
            const CTaskID &task_id,
            return_size,
            generator_index,
            is_async_actor):
        """Allocate a dynamic return ID for a generator task.

        NOTE: When is_async_actor is True,
        this API SHOULD NOT BE called
        within an async actor's event IO thread. The caller MUST ensure
        this for correctness. It is due to the limitation WorkerContext
        API when async actor is used.
        See https://github.com/ray-project/ray/issues/10324 for further details.

        Args:
            owner_address: The address of the owner (caller) of the
                generator task.
            task_id: The task ID of the generator task.
            return_size: The size of the static return from the task.
            generator_index: The index of dynamically generated object
                ref.
            is_async_actor: True if the allocation is for async actor.
                If async actor is used, we should calculate the
                put_index ourselves.
        """
        # Generator only has 1 static return.
        assert return_size == 1
        if is_async_actor:
            # This part of code has a couple of assumptions.
            # - This API is not called within an asyncio event loop
            #   thread.
            # - Ray object ref is generated by incrementing put_index
            #   whenever a new return value is added or ray.put is called.
            #
            # When an async actor is used, it uses its own thread to execute
            # async tasks. That means all the ray.put will use a put_index
            # scoped to a asyncio event loop thread.
            # This means the execution thread that this API will be called
            # will only create "return" objects. That means if we use
            # return_size + genreator_index as a put_index, it is guaranteed
            # to be unique.
            #
            # Why do we need it?
            #
            # We have to provide a put_index ourselves here because
            # the current implementation only has 1 worker context at any
            # given time, meaning WorkerContext::TaskID & WorkerContext::PutIndex
            # both could be incorrect (duplicated) when this API is called.
            return CCoreWorkerProcess.GetCoreWorker().AllocateDynamicReturnId(
                owner_address,
                task_id,
                # Should add 1 because put index is always incremented
                # before it is used. So if you have 1 return object
                # the next index will be 2.
                make_optional[ObjectIDIndexType](
                    <int>1 + <int>return_size + <int>generator_index)  # put_index
            )
        else:
            return CCoreWorkerProcess.GetCoreWorker().AllocateDynamicReturnId(
                owner_address,
                task_id,
                make_optional[ObjectIDIndexType](
                    <int>1 + <int>return_size + <int>generator_index))

    def async_delete_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()

        with nogil:
            CCoreWorkerProcess.GetCoreWorker().AsyncDelObjectRefStream(c_generator_id)

    def try_read_next_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            CObjectReference c_object_ref

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().TryReadObjectRefStream(
                    c_generator_id, &c_object_ref))

        return ObjectRef(
            c_object_ref.object_id(),
            c_object_ref.owner_address().SerializeAsString(),
            "",
            # Already added when the ref is updated.
            skip_adding_local_ref=True)

    def is_object_ref_stream_finished(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            c_bool finished

        with nogil:
            finished = CCoreWorkerProcess.GetCoreWorker().StreamingGeneratorIsFinished(
                c_generator_id)
        return finished

    def peek_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            pair[CObjectReference, c_bool] c_object_ref_and_is_ready_pair

        with nogil:
            c_object_ref_and_is_ready_pair = (
                    CCoreWorkerProcess.GetCoreWorker().PeekObjectRefStream(
                        c_generator_id))

        return (ObjectRef(
                    c_object_ref_and_is_ready_pair.first.object_id(),
                    c_object_ref_and_is_ready_pair.first.owner_address().SerializeAsString()), # noqa
                c_object_ref_and_is_ready_pair.second)

cdef void async_callback(shared_ptr[CRayObject] obj,
                         CObjectID object_ref,
                         void *user_callback_ptr) with gil:
    cdef:
        c_vector[shared_ptr[CRayObject]] objects_to_deserialize

    try:
        # Object is retrieved from in memory store.
        # Here we go through the code path used to deserialize objects.
        objects_to_deserialize.push_back(obj)
        serialized_ray_objects = RayObjectsToSerializedRayObjects(
            objects_to_deserialize)
        ids_to_deserialize = [ObjectRef(object_ref.Binary())]
        result = ray._private.worker.global_worker.deserialize_objects(
            serialized_ray_objects, ids_to_deserialize)[0]

        user_callback = <object>user_callback_ptr
        user_callback(result)
    except Exception:
        # Only log the error here because this callback is called from Cpp
        # and Cython will ignore the exception anyway
        logger.exception("failed to run async callback (user func)")
    finally:
        # NOTE: we manually increment the Python reference count of the callback when
        # registering it in the core worker, so we must decrement here to avoid a leak.
        cpython.Py_DECREF(user_callback)
