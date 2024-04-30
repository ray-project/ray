from pytest_shutil.workspace import pytest

# def test_resource_constrained_triggers_autoscaling(monkeypatch):
#     RESOURCE_REQUEST_TIMEOUT = 5
#     monkeypatch.setattr(
#         ray.data._internal.execution.autoscaling_requester,
#         "RESOURCE_REQUEST_TIMEOUT",
#         RESOURCE_REQUEST_TIMEOUT,
#     )
#     monkeypatch.setattr(
#         ray.data._internal.execution.autoscaling_requester,
#         "PURGE_INTERVAL",
#         RESOURCE_REQUEST_TIMEOUT,
#     )
#     from ray.data._internal.execution.autoscaling_requester import (
#         get_or_create_autoscaling_requester_actor,
#     )

#     ray.shutdown()
#     ray.init(num_cpus=3, num_gpus=1)

#     def run_execution(
#         execution_id: str, incremental_cpu: int = 1, autoscaling_state=None
#     ):
#         if autoscaling_state is None:
#             autoscaling_state = AutoscalingState()
#         opt = ExecutionOptions()
#         inputs = make_ref_bundles([[x] for x in range(20)])
#         o1 = InputDataBuffer(inputs)
#         o2 = MapOperator.create(
#             make_map_transformer(lambda block: [b * -1 for b in block]),
#             o1,
#         )
#         o2.num_active_tasks = MagicMock(return_value=1)
#         o3 = MapOperator.create(
#             make_map_transformer(lambda block: [b * 2 for b in block]),
#             o2,
#         )
#         o3.num_active_tasks = MagicMock(return_value=1)
#         o4 = MapOperator.create(
#             make_map_transformer(lambda block: [b * 3 for b in block]),
#             o3,
#             compute_strategy=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
#             ray_remote_args={"num_gpus": incremental_cpu},
#         )
#         o4.num_active_tasks = MagicMock(return_value=1)
#         o4.incremental_resource_usage = MagicMock(
#             return_value=ExecutionResources(gpu=1)
#         )
#         topo = build_streaming_topology(o4, opt)[0]
#         # Make sure only two operator's inqueues has data.
#         topo[o2].inqueues[0].append(make_ref_bundle("dummy"))
#         topo[o4].inqueues[0].append(make_ref_bundle("dummy"))
#         resource_manager = mock_resource_manager(
#             global_usage=ExecutionResources(cpu=2, gpu=1, object_store_memory=1000),
#             global_limits=ExecutionResources.for_limits(
#                 cpu=2, gpu=1, object_store_memory=1000
#             ),
#         )
#         selected_op = select_operator_to_run(
#             topo,
#             resource_manager,
#             [],
#             True,
#             execution_id,
#             autoscaling_state,
#         )
#         assert selected_op is None
#         for op in topo:
#             op.shutdown()

#     test_timeout = 3
#     ac = get_or_create_autoscaling_requester_actor()
#     ray.get(ac._test_set_timeout.remote(test_timeout))

#     run_execution("1")
#     assert ray.get(ac._aggregate_requests.remote()) == [
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#         {"CPU": 1},
#     ]

#     # For the same execution_id, the later request overrides the previous one.
#     run_execution("1")
#     assert ray.get(ac._aggregate_requests.remote()) == [
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#         {"CPU": 1},
#     ]

#     # Having another execution, so the resource bundles expanded.
#     run_execution("2")
#     assert ray.get(ac._aggregate_requests.remote()) == [
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#     ]

#     # Requesting for existing execution again, so no change in resource bundles.
#     run_execution("1")
#     assert ray.get(ac._aggregate_requests.remote()) == [
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#     ]

#     # After the timeout, all requests should have been purged.
#     time.sleep(test_timeout + 1)
#     ray.get(ac._purge.remote())
#     assert ray.get(ac._aggregate_requests.remote()) == []

#     # Test throttling by sending 100 requests: only one request actually
#     # got sent to the actor.
#     autoscaling_state = AutoscalingState()
#     for i in range(5):
#         run_execution("1", 1, autoscaling_state)
#     assert ray.get(ac._aggregate_requests.remote()) == [
#         {"CPU": 1},
#         {"CPU": 1},
#         {"CPU": 1},
#         {"GPU": 1},
#         {"GPU": 1},
#         {"CPU": 1},
#     ]

#     # Test that the resource requests will be purged after the timeout.
#     wait_for_condition(
#         lambda: ray.get(ac._aggregate_requests.remote()) == [],
#         timeout=RESOURCE_REQUEST_TIMEOUT * 2,
#     )

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
