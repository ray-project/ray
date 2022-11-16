import pytest
import ray
import time
import tempfile
import numpy as np
import json
import os
import sys
from ray.tests.conftest import (
    mock_distributed_fs_object_spilling_config,
)


def test_owner_failed(ray_start_cluster):
    NODE_NUMBER = 2
    cluster_node_config = [
        {"num_cpus": 1, "resources": {f"node{i+1}": 10}} for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for kwargs in cluster_node_config:
        cluster.add_node(**kwargs)
    ray.init(address=cluster.address)

    OBJECT_NUMBER = 10

    @ray.remote(resources={"node1": 1}, num_cpus=0)
    class Creator:
        def gen_object_refs(self):
            refs = []
            for _ in range(OBJECT_NUMBER):
                refs.append(ray.put(np.random.rand(2, 2), _ha=True))
            return refs

    @ray.remote(resources={"node2": 1}, num_cpus=0)
    class Borrower:
        def __init__(self):
            self.refs = None

        def set_object_refs(self, refs):
            self.refs = refs

        def get_objects(self):
            ray.get(self.refs)
            return True

    creator = Creator.remote()
    borrower = Borrower.remote()

    refs = ray.get(creator.gen_object_refs.remote())
    ray.get(borrower.set_object_refs.remote(refs))
    owner = ray.get_actor("_ray_global_owner_")
    ray.kill(owner, no_restart=True)

    assert ray.get(borrower.get_objects.remote(), timeout=120)


@pytest.mark.parametrize(
    "node_resources",
    [
        dict(
            zip(
                ["creator", "borrower", "execute_ray_get"], [{f"node{i}": 1} for i in _]
            )
        )
        for _ in [
            [2, 3, 1],
            [2, 2, 1],
            [1, 2, 1],
            [2, 1, 1],
            [1, 1, 1],
        ]
    ],
)
def test_object_location(ray_start_cluster, node_resources):
    NODE_NUMBER = 3
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for index, kwargs in enumerate(cluster_node_config):
        cluster.add_node(**kwargs)
        # make sure driver on node 1
        if index == 0:
            ray.init(address=cluster.address)

    class Worker:
        def __init__(self):
            self.refs = None

        def create_obj(self):
            self.refs = [
                ray.put(np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8), _ha=True)
            ]
            print("global owner: ", ray.ActorID(self.refs[0].global_owner_id()))
            return self.refs

        def send_refs(self, refs):
            self.refs = refs

        def try_get(self):
            print("global owner in get: ", ray.ActorID(self.refs[0].global_owner_id()))
            [data] = ray.get(self.refs)
            return len(data)

        def warmup(self):
            return True

        def evict_all_object(self):
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))

        def exit(self):
            sys.exit(0)

    worker_1 = (
        ray.remote(Worker)
        .options(resources=node_resources["creator"], num_cpus=0)
        .remote()
    )
    ray.get(worker_1.warmup.remote())
    worker_2 = (
        ray.remote(Worker)
        .options(resources=node_resources["borrower"], num_cpus=0)
        .remote()
    )
    ray.get(worker_2.warmup.remote())

    refs = ray.get(worker_1.create_obj.remote())
    owner = ray.get_actor("_ray_global_owner_")
    print(
        "test object ref: ",
        refs[0],
        "global owner:",
        ray.ActorID(refs[0].global_owner_id()),
        "checkout url:",
        refs[0].spilled_url().decode("utf-8"),
    )
    ray.get(worker_2.send_refs.remote(refs))
    print("try get obj before kill:", ray.get(worker_2.try_get.remote()))
    print("old owner pid:", ray.get(owner.getpid.remote()))
    ray.get(worker_2.evict_all_object.remote())
    print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())
    try:
        ray.get(owner.exit.remote())
    except ray.exceptions.RayActorError:
        pass
    else:
        raise RuntimeError
    print("wait 5 seconds for owner died.")
    checkout_url = refs[0].spilled_url().decode("utf-8").split("?")[0]
    os.remove(checkout_url)
    time.sleep(5)
    print("new owner pid:", ray.get(owner.getpid.remote()))
    if node_resources["creator"] != node_resources["borrower"]:
        print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    else:
        with pytest.raises(ray.exceptions.GetTimeoutError):
            ray.get(worker_2.try_get.remote(), timeout=10)


@pytest.mark.parametrize(
    "node_resources",
    [
        dict(
            zip(
                ["creator", "borrower", "execute_ray_get"], [{f"node{i}": 1} for i in _]
            )
        )
        for _ in [
            [2, 3, 1],
            [2, 2, 1],
            [1, 2, 1],
            [2, 1, 1],
            [1, 1, 1],
        ]
    ],
)
def test_ha_object_with_raylet_dead(ray_start_cluster, node_resources):
    NODE_NUMBER = 3
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for index, kwargs in enumerate(cluster_node_config):
        cluster.add_node(**kwargs)
        # make sure driver on node 1
        if index == 0:
            ray.init(address=cluster.address)
            assert len(cluster.worker_nodes) == 0
    assert len(cluster.worker_nodes) == NODE_NUMBER - 1

    class Worker:
        def __init__(self):
            self.refs = None

        def create_obj(self):
            self.refs = [
                ray.put(np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8), _ha=True)
            ]
            print("global owner: ", ray.ActorID(self.refs[0].global_owner_id()))
            return self.refs

        def send_refs(self, refs):
            self.refs = refs

        def try_get(self):
            print("global owner in get: ", ray.ActorID(self.refs[0].global_owner_id()))
            [data] = ray.get(self.refs)
            return len(data)

        def warmup(self):
            return True

        def evict_all_object(self):
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))

        def exit(self):
            sys.exit(0)

    worker_1 = (
        ray.remote(Worker)
        .options(resources=node_resources["creator"], num_cpus=0)
        .remote()
    )
    ray.get(worker_1.warmup.remote())
    worker_2 = (
        ray.remote(Worker)
        .options(resources=node_resources["borrower"], num_cpus=0)
        .remote()
    )
    ray.get(worker_2.warmup.remote())
    refs = ray.get(worker_1.create_obj.remote())

    ray.get(worker_2.send_refs.remote(refs))
    print("try get obj before kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())
    print("try get obj after kill:", ray.get(worker_2.try_get.remote()))
    ray.get(worker_2.evict_all_object.remote())

    for worker_node in list(cluster.worker_nodes):
        cluster.remove_node(worker_node)
    assert ray.get(refs[0]).shape == (30 * 1024 * 1024, 1)


@pytest.mark.parametrize(
    "node_resources",
    [
        dict(zip(["task", "caller", "execute_ray_get"], [{f"node{i}": 1} for i in _]))
        for _ in [
            [2, 3, 1],
            [2, 2, 1],
            [1, 2, 1],
            [2, 1, 1],
            [1, 1, 1],
        ]
    ],
)
@pytest.mark.parametrize("test_type", ["normal_task", "actor_task"])
def test_normal_task_returned_object(ray_start_cluster, test_type, node_resources):
    NODE_NUMBER = 3
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for index, kwargs in enumerate(cluster_node_config):
        cluster.add_node(**kwargs)
        # make sure driver on node 1
        if index == 0:
            ray.init(address=cluster.address)
            assert len(cluster.worker_nodes) == 0
    assert len(cluster.worker_nodes) == NODE_NUMBER - 1

    if test_type == "normal_task":

        @ray.remote(resources=node_resources["task"], _ha=True)
        def test_remote_function():
            return "TEST", np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8)

        @ray.remote(resources=node_resources["task"], _ha=True)
        def test_remote_function_with_small_return():
            return "TEST"

    elif test_type == "actor_task":

        @ray.remote(resources=node_resources["task"], _ha=True)
        class TestActor:
            def test_remote_function(self):
                return "TEST", np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8)

            def test_remote_function_with_small_return(self):
                return "TEST"

        test_actor = TestActor.remote()
        test_remote_function = test_actor.test_remote_function
        test_remote_function_with_small_return = (
            test_actor.test_remote_function_with_small_return
        )
    else:
        raise ValueError("test_type must in ['normal_task', 'actor_task']")

    @ray.remote(resources=node_resources["caller"])
    class Worker:
        def do_test(self):
            ref_1 = test_remote_function.remote()
            assert ray.get(ref_1)[0] == "TEST"
            assert ref_1.spilled_url().decode("utf-8") == "PLACEMENT_HOLD"

            # evict all object in plasma
            ref1 = ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            ref2 = ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            del ref1, ref2

            assert ray.get(ref_1)[0] == "TEST"

            ref_2 = test_remote_function_with_small_return.remote()
            assert ray.get(ref_2) == "TEST"
            assert ref_2.spilled_url().decode("utf-8") == "PLACEMENT_HOLD"
            return [ref_1, ref_2]

    worker = Worker.remote()
    [ref_1, ref_2] = ray.get(worker.do_test.remote())
    for worker_node in list(cluster.worker_nodes):
        cluster.remove_node(worker_node)
    assert ray.get(ref_1)[0] == "TEST"
    assert ray.get(ref_2) == "TEST"


@pytest.mark.parametrize("test_type", ["normal_task", "actor_task"])
def test_normal_task_returned_object_caller_worker_dead(ray_start_cluster, test_type):
    NODE_NUMBER = 3
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for index, kwargs in enumerate(cluster_node_config):
        cluster.add_node(**kwargs)
        # make sure driver on node 1
        if index == 0:
            ray.init(address=cluster.address)
            assert len(cluster.worker_nodes) == 0
    assert len(cluster.worker_nodes) == NODE_NUMBER - 1

    with tempfile.TemporaryDirectory() as tmp_dir:
        trigger_file_1 = os.path.join(tmp_dir, "task_finished_1")
        trigger_file_2 = os.path.join(tmp_dir, "task_finished_2")

        def _check_task_finished(trigger_file):
            while True:
                if os.path.exists(trigger_file):
                    return True
                time.sleep(1)

        if test_type == "normal_task":

            @ray.remote(resources={"node1": 1}, _ha=True)
            def test_remote_function(trigger_file):
                _check_task_finished(trigger_file)
                return "TEST", np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8)

            @ray.remote(resources={"node1": 1}, _ha=True)
            def test_remote_function_with_small_return(trigger_file):
                _check_task_finished(trigger_file)
                return "TEST"

        elif test_type == "actor_task":

            @ray.remote(resources={"node1": 1}, _ha=True)
            class TestActor:
                def test_remote_function(self, trigger_file):
                    _check_task_finished(trigger_file)
                    return "TEST", np.zeros((30 * 1024 * 1024, 1)).astype(np.uint8)

                def test_remote_function_with_small_return(self, trigger_file):
                    _check_task_finished(trigger_file)
                    return "TEST"

            test_actor = TestActor.remote()
            test_remote_function = test_actor.test_remote_function
            test_remote_function_with_small_return = (
                test_actor.test_remote_function_with_small_return
            )
        else:
            raise ValueError("test_type must in ['normal_task', 'actor_task']")

        @ray.remote(resources={"node1": 1})
        class CallerWorker:
            def do_test(self):
                ref1 = test_remote_function.remote(trigger_file_1)
                ref2 = test_remote_function_with_small_return.remote(trigger_file_1)
                ref3 = test_remote_function.remote(trigger_file_2)
                ref4 = test_remote_function_with_small_return.remote(trigger_file_2)
                with pytest.raises(ray.exceptions.GetTimeoutError):
                    ray.get(ref1, timeout=2)
                with pytest.raises(ray.exceptions.GetTimeoutError):
                    ray.get(ref2, timeout=2)
                with pytest.raises(ray.exceptions.GetTimeoutError):
                    ray.get(ref3, timeout=2)
                with pytest.raises(ray.exceptions.GetTimeoutError):
                    ray.get(ref4, timeout=2)
                return [ref1, ref2, ref3, ref4]

        caller_worker = CallerWorker.remote()
        [ref1, ref2, ref3, ref4] = ray.get(caller_worker.do_test.remote())

        print(f"ref1: {ref1}, ref2: {ref2}, ref3: {ref3}, ref4: {ref4}")
        with open(trigger_file_1, "w") as f:
            pass
        time.sleep(10)
        ray.kill(caller_worker, no_restart=True)

        assert ray.get(ref1, timeout=5)[0] == "TEST"
        assert ray.get(ref2, timeout=5) == "TEST"
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref3)
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref4)


@pytest.mark.parametrize(
    "node_resources",
    [
        dict(zip(["task", "caller", "execute_ray_get"], [{f"node{i}": 1} for i in _]))
        for _ in [
            [2, 3, 1],
            [2, 2, 1],
            [1, 2, 1],
            [2, 1, 1],
            [1, 1, 1],
        ]
    ],
)
def test_dynamic_returned_object(ray_start_cluster, node_resources):
    NODE_NUMBER = 3
    cluster_node_config = [
        {
            "num_cpus": 10,
            "resources": {f"node{i+1}": 10},
            "object_store_memory": 75 * 1024 * 1024,
        }
        for i in range(NODE_NUMBER)
    ]
    cluster_node_config[0]["_system_config"] = {
        "object_spilling_config": json.dumps(mock_distributed_fs_object_spilling_config)
    }
    cluster = ray_start_cluster
    for index, kwargs in enumerate(cluster_node_config):
        cluster.add_node(**kwargs)
        # make sure driver on node 1
        if index == 0:
            ray.init(address=cluster.address)
            assert len(cluster.worker_nodes) == 0
    assert len(cluster.worker_nodes) == NODE_NUMBER - 1


    @ray.remote(resources=node_resources["task"], _ha=True, num_returns="dynamic")
    def test_remote_function():
        for _ in range(3):
            yield "TEST", np.zeros((10 * 1024 * 1024, 1)).astype(np.uint8)

    @ray.remote(resources=node_resources["task"], _ha=True, num_returns="dynamic")
    def test_remote_function_with_small_return():
        for _ in range(3):
            yield "TEST"

    @ray.remote(resources=node_resources["caller"])
    class Worker:
        def do_test(self):
            refs_1 = list(ray.get(test_remote_function.remote()))
            assert len(refs_1) == 3
            assert refs_1[0].spilled_url().decode("utf-8") != "PLACEMENT_HOLD"
            assert ray.get(refs_1[0])[0] == "TEST"

            # evict all object in plasma
            ref1 = ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            ref2 = ray.put(np.zeros((50 * 1024 * 1024, 1)).astype(np.uint8))
            del ref1, ref2

            assert ray.get(refs_1[0])[0] == "TEST"

            refs_2 = list(ray.get(test_remote_function_with_small_return.remote()))
            assert len(refs_2) == 3
            assert ray.get(refs_2[0]) == "TEST"
            assert refs_2[0].spilled_url().decode("utf-8") != "PLACEMENT_HOLD"
            return [refs_1, refs_2]

    worker = Worker.remote()
    [refs_1, refs_2] = ray.get(worker.do_test.remote())
    for worker_node in list(cluster.worker_nodes):
        cluster.remove_node(worker_node)
    for ref_1, ref_2 in zip(refs_1, refs_2):
        try:
            assert ray.get(ref_1)[0] == "TEST"
            assert ray.get(ref_2) == "TEST"
        except Exception:
            print(ref_1.spilled_url().decode("utf-8"), ref_2.spilled_url().decode("utf-8"))
            raise


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
