import pytest

from ray.tests.test_experimental_client import ray_start_client_server

def test_get_ray_metadata(ray_start_regular_shared):
    """
    Test the ClusterInfo client data pathway and API surface
    """
    with ray_start_client_server() as ray:
        nodes = ray.nodes()
        assert len(nodes) == 1, nodes

        workers = ray.state.workers()
        assert len(workers) == 1, workers

        current_node_id = ray.state.current_node_id()
        assert current_node_id == "node:" + ray_start_regular_shared["node_ip_address"]

        node_ids = ray.state.node_ids()
        assert len(node_ids) == 1
        assert current_node_id in node_ids


        actors_before = ray.actors()
        assert len(actors_before) == 0

        @ray.remote
        class Counter:
            def __init__(self):
                self.a = 1

            def inc(self):
                self.a += 1

        counter = Counter.remote()
        counter2 = Counter.remote()
        counter2.inc.remote()

        actors_after = ray.actors()
        assert len(actors_after) == 2
        counter_info = ray.actors(counter._actor_id.hex())
        assert counter_info == actors_after[counter_info["ActorID"]]


        objects_before = ray.objects()
        # We put the Counter class as an object
        assert len(objects_before) == 1

        hello = ray.put("Hello World")
        hello2 = ray.put("Hello Other World")

        objects_after = ray.objects()
        assert len(objects_after) == 3

        hello_info = ray.objects(hello.binary().hex())
        assert hello_info == objects_after[hello_info["ObjectRef"]]


        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()

        assert cluster_resources["CPU"] == 1.0
        assert current_node_id in cluster_resources
        assert current_node_id in available_resources

