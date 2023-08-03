import asyncio
import logging

from ray.serve.benchmarks.microbenchmark import main as benchmark_main
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
)
from serve_test_utils import (
    save_test_results,
    is_smoke_test,
)

logger = logging.getLogger(__file__)


async def main():
    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    if is_smoke_test():
        setup_local_single_node_cluster(1)
    else:
        setup_anyscale_cluster()

    result_json = await benchmark_main()
    logger.info(result_json)
    save_test_results(result_json, default_output_file="/tmp/micro_benchmark.json")


if __name__ == "__main__":
    asyncio.run(main())
