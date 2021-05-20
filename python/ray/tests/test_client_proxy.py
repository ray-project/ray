import os
import pytest
import time

import grpc
import ray
from ray.job_config import JobConfig
import ray.util.client.server.proxier as proxier


def test_proxy_manager(shutdown_only):
    ray_instance = ray.init()
    proxier.CHECK_PROCESS_INTERVAL_S = 1
    os.environ["TIMEOUT_FOR_SPECIFIC_SERVER_S"] = "5"
    pm = proxier.ProxyManager(ray_instance["redis_address"],
                              ray_instance["session_dir"])
    pm._free_ports = [45000, 45001]
    client = "client1"

    pm.start_specific_server(client, JobConfig())
    # Channel should be ready and corresponding to an existing server
    grpc.channel_ready_future(pm.get_channel(client)).result(timeout=5)

    proc = pm._get_server_for_client(client)
    assert proc.port == 45000

    proc.process_handle().process.wait(5)
    # Wait for reconcile loop
    time.sleep(2)

    assert len(pm._free_ports) == 2
    assert pm._get_unused_port() == 45001


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
