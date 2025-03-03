// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <optional>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "ray/common/task/task_common.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/util/process.h"

namespace ray {
namespace core {

// If you change this options's definition, you must change the options used in
// other files. Please take a global search and modify them !!!
struct CoreWorkerOptions {
  // Callback that must be implemented and provided by the language-specific worker
  // frontend to execute tasks and return their results.
  using TaskExecutionCallback = std::function<Status(
      const rpc::Address &caller_address,
      TaskType task_type,
      const std::string task_name,
      const RayFunction &ray_function,
      const std::unordered_map<std::string, double> &required_resources,
      const std::vector<std::shared_ptr<RayObject>> &args,
      const std::vector<rpc::ObjectReference> &arg_refs,
      const std::string &debugger_breakpoint,
      const std::string &serialized_retry_exception_allowlist,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *returns,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_returns,
      std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
      std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes,
      bool *is_retryable_error,
      // Application error string, empty if no error.
      std::string *application_error,
      // The following 2 parameters `defined_concurrency_groups` and
      // `name_of_concurrency_group_to_execute` are used for Python
      // asyncio actor only.
      //
      // Defined concurrency groups of this actor. Note this is only
      // used for actor creation task.
      const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
      const std::string name_of_concurrency_group_to_execute,
      bool is_reattempt,
      // True if the task is for streaming generator.
      // TODO(sang): Remove it and combine it with dynamic returns.
      bool is_streaming_generator,
      // True if task can be retried upon exception.
      bool retry_exception,
      // The max number of unconsumed objects where a generator
      // can run without a pause.
      int64_t generator_backpressure_num_objects)>;

  CoreWorkerOptions()
      : store_socket(""),
        raylet_socket(""),
        enable_logging(false),
        log_dir(""),
        install_failure_signal_handler(false),
        interactive(false),
        node_ip_address(""),
        node_manager_port(0),
        raylet_ip_address(""),
        driver_name(""),
        task_execution_callback(nullptr),
        check_signals(nullptr),
        initialize_thread_callback(nullptr),
        gc_collect(nullptr),
        spill_objects(nullptr),
        restore_spilled_objects(nullptr),
        delete_spilled_objects(nullptr),
        unhandled_exception_handler(nullptr),
        get_lang_stack(nullptr),
        kill_main(nullptr),
        cancel_async_task(nullptr),
        is_local_mode(false),
        terminate_asyncio_thread(nullptr),
        serialized_job_config(""),
        metrics_agent_port(-1),
        runtime_env_hash(0),
        cluster_id(ClusterID::Nil()),
        session_name(""),
        entrypoint(""),
        worker_launch_time_ms(-1),
        worker_launched_time_ms(-1),
        assigned_worker_port(std::nullopt),
        assigned_raylet_id(std::nullopt),
        debug_source("") {
    // TODO(hjiang): Add invariant check: for worker, both should be assigned; for driver,
    // neither should be assigned.
  }

  /// Type of this worker (i.e., DRIVER or WORKER).
  WorkerType worker_type;
  /// Application language of this worker (i.e., PYTHON or JAVA).
  Language language;
  /// Object store socket to connect to.
  std::string store_socket;
  /// Raylet socket to connect to.
  std::string raylet_socket;
  /// Job ID of this worker.
  JobID job_id;
  /// Options for the GCS client.
  gcs::GcsClientOptions gcs_options;
  /// Initialize logging if true. Otherwise, it must be initialized and cleaned up by the
  /// caller.
  bool enable_logging;
  /// Directory to write logs to. If this is empty, logs won't be written to a file.
  std::string log_dir;
  /// If false, will not call `RayLog::InstallFailureSignalHandler()`.
  bool install_failure_signal_handler;
  /// Whether this worker is running in a tty.
  bool interactive;
  /// IP address of the node.
  std::string node_ip_address;
  /// Port of the local raylet.
  int node_manager_port;
  /// IP address of the raylet.
  std::string raylet_ip_address;
  /// The name of the driver.
  std::string driver_name;
  /// Application-language worker callback to execute tasks.
  TaskExecutionCallback task_execution_callback;
  /// The callback to be called when shutting down a `CoreWorker` instance.
  std::function<void(const WorkerID &)> on_worker_shutdown;
  /// Application-language callback to check for signals that have been received
  /// since calling into C++. This will be called periodically (at least every
  /// 1s) during long-running operations. If the function returns anything but StatusOK,
  /// any long-running operations in the core worker will short circuit and return that
  /// status.
  std::function<Status()> check_signals;
  /// Application-language callback that initializes a thread and returns a function to
  /// be called when the thread is destroyed.
  std::function<std::function<void()>()> initialize_thread_callback;
  /// Application-language callback to trigger garbage collection in the language
  /// runtime. This is required to free distributed references that may otherwise
  /// be held up in garbage objects.
  std::function<void(bool triggered_by_global_gc)> gc_collect;
  /// Application-language callback to spill objects to external storage.
  std::function<std::vector<std::string>(const std::vector<rpc::ObjectReference> &)>
      spill_objects;
  /// Application-language callback to restore objects from external storage.
  std::function<int64_t(const std::vector<rpc::ObjectReference> &,
                        const std::vector<std::string> &)>
      restore_spilled_objects;
  /// Application-language callback to delete objects from external storage.
  std::function<void(const std::vector<std::string> &, rpc::WorkerType)>
      delete_spilled_objects;
  /// Function to call on error objects never retrieved.
  std::function<void(const RayObject &error)> unhandled_exception_handler;
  /// Language worker callback to get the current call stack.
  std::function<void(std::string *)> get_lang_stack;
  // Function that tries to interrupt the currently running Python thread if its
  // task ID matches the one given.
  std::function<bool(const TaskID &task_id)> kill_main;
  std::function<void(const TaskID &task_id,
                     const RayFunction &ray_function,
                     const std::string name_of_concurrency_group_to_execute)>
      cancel_async_task;
  /// Is local mode being used.
  bool is_local_mode;
  /// The function to destroy asyncio event and loops.
  std::function<void()> terminate_asyncio_thread;
  /// Serialized representation of JobConfig.
  std::string serialized_job_config;
  /// The port number of a metrics agent that imports metrics from core workers.
  /// -1 means there's no such agent.
  int metrics_agent_port;
  /// The hash of the runtime env for this worker.
  int runtime_env_hash;
  /// The startup token of the process assigned to it
  /// during startup via command line arguments.
  /// This is needed because the actual core worker process
  /// may not have the same pid as the process the worker pool
  /// starts (due to shim processes).
  StartupToken startup_token{0};
  /// Cluster ID associated with the core worker.
  ClusterID cluster_id;
  /// The function to allocate a new object for the memory store.
  /// This allows allocating the objects in the language frontend's memory.
  /// For example, for the Java worker, we can allocate the objects in the JVM heap
  /// memory, and enables the JVM to manage the memory of the memory store objects.
  std::function<std::shared_ptr<ray::RayObject>(const ray::RayObject &object,
                                                const ObjectID &object_id)>
      object_allocator;
  /// Session name (Cluster ID) of the cluster.
  std::string session_name;
  std::string entrypoint;
  int64_t worker_launch_time_ms;
  int64_t worker_launched_time_ms;
  /// Available port number for the worker.
  ///
  /// TODO(hjiang): Figure out how to assign available port at core worker start, also
  /// need to add an end-to-end integration test.
  ///
  /// On the next end-to-end integrartion PR, we should check
  /// - non-empty for worker
  /// - and empty for driver
  std::optional<int> assigned_worker_port;
  /// Same as [assigned_worker_port], will be assigned for worker, and left empty for
  /// driver.
  std::optional<NodeID> assigned_raylet_id;

  // Source information for `CoreWorker`, used for debugging and informational purpose,
  // rather than functional purpose.
  std::string debug_source;
};
}  // namespace core
}  // namespace ray
