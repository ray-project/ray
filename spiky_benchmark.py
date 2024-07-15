import datetime
import json
import os
import time
import numpy as np
import timeline
import ray

LOG_FILE = "test_large_e2e_backpressure.log"


class Logger:
    def __init__(self, filename: str = LOG_FILE):
        self._filename = filename
        self._start_time = time.time()

    def record_start(self):
        self._start_time = time.time()
        with open(self._filename, "w"):
            pass

    def log(self, payload: dict):
        payload = {
            **payload,
            "time": time.time() - self._start_time,
            "clock_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f"),
        }
        with open(self._filename, "a") as f:
            f.write(json.dumps(payload) + "\n")


logger = Logger()

TIME_UNIT = 0.5


def test_large_e2e_backpressure(is_flink: bool):
    """Test backpressure on a synthetic large-scale workload."""
    # The cluster has 10 CPUs and 200MB object store memory.
    # The dataset will have 200MB * 25% = 50MB memory budget.
    #
    # Each produce task generates 10 blocks, each of which has 10MB data.
    #
    # Without any backpressure, the producer tasks will output at most
    # 10 * 10 * 10MB = 1000MB data.
    #
    # With StreamingOutputBackpressurePolicy and the following configuration,
    # the executor will still schedule 10 produce tasks, but only the first task is
    # allowed to output all blocks. The total size of pending blocks will be
    # (10 + 9 * 1 + 1) * 10MB = 200MB, where
    # - 10 is the number of blocks in the first task.
    # - 9 * 1 is the number of blocks pending at the streaming generator level of
    #   the other 15 tasks.
    # - 1 is the number of blocks pending at the output queue.

    os.environ["RAY_DATA_OP_RESERVATION_RATIO"] = "0"

    NUM_CPUS = 8
    NUM_ROWS_PER_TASK = 10
    NUM_TASKS = 16 * 5
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS  
    BLOCK_SIZE = 10 * 1024 * 1024 * 10

    # Write the data to file.
    # array = np.zeros(BLOCK_SIZE, dtype=np.uint8)
    # file_path = 'zeros_block.bin'
    # array.tofile(file_path)

    def produce(batch):
        logger.log({"name": "producer_start", "id": [int(x) for x in batch["id"]]})
        if (int(batch["id"][0].item()) / 10) % 10 == 0:
            time.sleep(TIME_UNIT * 10)
        else:
            time.sleep(TIME_UNIT)
        for id in batch["id"]:
            # logger.log({"name": "produce", "id": int(id)})
            yield {
                "id": [id],
                "image": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                # Read the data from file.
                # "image": [np.fromfile(file_path, dtype=np.uint8)],
            }

    def consume(batch):
        logger.log({"name": "consume", "id": int(batch["id"].item())})
        if int(batch["id"][0].item()) % 10 == 0:
            time.sleep(TIME_UNIT * 10)
        else:
            time.sleep(TIME_UNIT)
        return {"id": batch["id"], "result": [0 for _ in batch["id"]]}

    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    if is_flink:
        data_context.is_budget_policy = False # Disable our policy. 
    else:
        data_context.is_budget_policy = True
        
    ray.init(num_cpus=NUM_CPUS, object_store_memory=25 * BLOCK_SIZE)

    ds = ray.data.range(NUM_ROWS_TOTAL, override_num_blocks=NUM_TASKS)
    
    if is_flink:
        ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK, concurrency=2)
        ds = ds.map_batches(consume, batch_size=None, num_cpus=0.99, concurrency=6) 
    else:
        ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
        ds = ds.map_batches(consume, batch_size=None, num_cpus=0.99)

    logger.record_start()

    start_time = time.time()
    logger.log({"name": "execution_start"})
    for i, _ in enumerate(ds.iter_batches(batch_size=NUM_ROWS_PER_TASK)):
        logger.log({"name": "iteration", "id": i})
        pass
    end_time = time.time()
    print(ds.stats())
    print(ray._private.internal_api.memory_summary(stats_only=True))
    print(f"Total time: {end_time - start_time:.4f}s")
    timeline.save_timeline(f"timeline_{'ray' if not is_flink else 'flink'}_spiky.json")
    ray.shutdown()

if __name__ == "__main__": 

    test_large_e2e_backpressure(is_flink=True) 
    test_large_e2e_backpressure(is_flink=False)
