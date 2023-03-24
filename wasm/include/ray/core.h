// Copyright 2020-2023 The Ray Authors.
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
#include <cstdint>

#if !defined(_CPP_CORE_H)

#define _CPP_CORE_H

typedef struct CoreWorkerOptions {
    uint8_t worker_type;
    uint8_t language;
    const char *store_socket;
    const char *raylet_socket;
    int32_t job_id;
    const char *gcs_address;
    uint8_t enable_logging;
    const char *log_dir;
    uint8_t install_failure_signal_handler;
    uint8_t interactive;
    const char *node_ip_address;
    uint32_t node_manager_port;
    const char *raylet_ip_address;
    const char *driver_name;
    const char *stdout_file;
    const char *stderr_file;

    void *task_execution_callback;
    void *on_worker_shutdown;
    void *check_signals;
    void *gc_collect;
    void *spill_objects;
    void *restore_spilled_objects;
    void *delete_spilled_objects;
    void * unhandled_exception_handler;
    void *get_lang_stack;
    void *kill_main;

    uint8_t is_local_mode;
    void *terminate_asyncio_thread;
    const char* serialized_job_config;

    int32_t metrics_agent_port;
    uint8_t connect_on_start;
    int32_t runtime_env_hash;

    int64_t startup_token;
    void *object_allocator;

    const char *session_name;
    const char *entrypoint;
} CoreWorkerOptions;

#endif // _CPP_CORE_H
