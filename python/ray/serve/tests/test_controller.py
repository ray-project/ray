import pytest

import ray


def test_controller_inflight_requests_clear(serve_instance):
    client = serve_instance
    initial_number_reqs = ray.get(
        client._controller._num_pending_goals.remote())

    def function(_):
        return "hello"

    client.create_backend("tst", function)
    client.create_endpoint("end_pt", backend="tst")

    assert ray.get(client._controller._num_pending_goals.remote()
                   ) - initial_number_reqs == 0


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
