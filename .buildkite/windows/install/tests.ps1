#test_core
bazel test --config=ci --build_event_json_file bazel_log.test --build_tests_only -- //:all -rllib/... -core_worker_test
bazel test --config=ci --build_event_json_file bazel_log.test --build_tests_only -- //:all -rllib/... -event_test
bazel test --config=ci --build_event_json_file bazel_log.test --build_tests_only -- //:all -rllib/... -gcs_pub_sub_test
bazel test --config=ci --build_event_json_file bazel_log.test --build_tests_only -- //:all -rllib/... -gcs_server_test
bazel test --config=ci --build_event_json_file bazel_log.test --build_tests_only -- //:all -rllib/... -gcs_server_rpc_test

#test_python
python python/ray/serve/tests/test_api.py
python python/ray/serve/tests/test_router.py
python python/ray/serve/tests/test_handle.py
python python/ray/serve/tests/test_backend_worker.py
python python/ray/serve/tests/test_controller_crashes.py
python python/ray/tests/test_actor_advanced.py
python python/ray/tests/test_actor_failures.py
python python/ray/tests/test_advanced_2
python python/ray/tests/test_advanced_3.py 
python python/ray/tests/test_autoscaler.py
python python/ray/tests/test_autoscaler_aws.py
python python/ray/tests/test_component_failures.py
python python/ray/tests/test_component_failures_3.py
python python/ray/tests/test_basic_2.py 
python python/ray/tests/test_basic_2_client_mode.py
python python/ray/tests/test_basic_3.py 
python python/ray/tests/test_basic_3_client_mode.py
python python/ray/tests/test_cli.py
python python/ray/tests/test_client_init.py
python python/ray/tests/test_command_runner.py
python python/ray/tests/test_failure.py
python python/ray/tests/test_failure_2.py
python python/ray/tests/test_gcs_fault_tolerance.py
python python/ray/tests/test_global_gc.py
python python/ray/tests/test_job.py
python python/ray/tests/test_memstat.py
python python/ray/tests/test_metrics.py
python python/ray/tests/test_metrics_agent.py
python python/ray/tests/test_multi_node.py
python python/ray/tests/test_multi_node_2.py
python python/ray/tests/test_multi_node_3.py
python python/ray/tests/test_multiprocessing.py 
python python/ray/tests/test_node_manager.py
python python/ray/tests/test_object_manager.py
python python/ray/tests/test_placement_group.py
python python/ray/tests/test_placement_group_mini_integration.py
python python/ray/tests/test_placement_group_2.py
python python/ray/tests/test_placement_group_3.py
python python/ray/tests/test_ray_init.py 
python python/ray/tests/test_resource_demand_scheduler.py
python python/ray/tests/test_runtime_env_env_vars.py
python python/ray/tests/test_stress.py 
python python/ray/tests/test_stress_sharded.py 
python python/ray/tests/test_k8s_operator_unit_tests.py
python python/ray/tests/test_tracing.py 
