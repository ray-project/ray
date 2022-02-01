import asyncio
import os

from ray.serve.benchmarks.microbenchmark import main as benchmark_main
from ray.serve.utils import logger
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
)
from serve_test_utils import (
    save_test_results,
)


async def main():
    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "1")
    if smoke_test == "1":
        setup_local_single_node_cluster(1)
    else:
        setup_anyscale_cluster()

    result_json = await benchmark_main()
    logger.info(result_json)
    save_test_results(result_json, default_output_file="/tmp/micro_benchmark.json")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
