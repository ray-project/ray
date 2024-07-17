import datetime
import json
import os
import time
import numpy as np
import timeline_utils
import ray
import psutil

LOG_FILE = "four_stage_problem.log"


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


def main(is_flink: bool):

    os.environ["RAY_DATA_OP_RESERVATION_RATIO"] = "0"

    NUM_CPUS = 8
    NUM_GPUS = 4
    NUM_ROWS_PER_TASK = 10
    BUFFER_SIZE_LIMIT = 30
    NUM_TASKS = 16 * 5
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS  
    BLOCK_SIZE = 10 * 1024 * 1024 * 10

    def produce(batch):
        logger.log({
            "name": "producer_start", 
            "id": [int(x) for x in batch["id"]]})
        time.sleep(TIME_UNIT * 10)
        for id in batch["id"]:
            yield {
                "id": [id],
                "image": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
            }

    def consume(batch):
        logger.log({"name": "consume", "id": int(batch["id"].item())})
        time.sleep(TIME_UNIT)
        return {"id": batch["id"], "result": [0 for _ in batch["id"]]}

    def inference(batch):
        logger.log({"name": "inference", "id": int(batch["id"].item())})
        time.sleep(TIME_UNIT)
        return {"id": batch["id"]}
    
    def write(batch):
        logger.log({"name": "write", "id": int(batch["id"].item())})
        time.sleep(TIME_UNIT)
        return {"id": batch["id"]}

    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    if is_flink:
        data_context.is_budget_policy = False # Disable our policy. 
    else:
        data_context.is_budget_policy = True
        
    ray.init(num_cpus=NUM_CPUS, num_gpus=NUM_GPUS, object_store_memory=BUFFER_SIZE_LIMIT * BLOCK_SIZE)

    ds = ray.data.range(NUM_ROWS_TOTAL, override_num_blocks=NUM_TASKS)
    
    if is_flink:
        ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK, concurrency=2)
        ds = ds.map_batches(consume, batch_size=1, num_cpus=0.99, concurrency=3)
        ds = ds.map_batches(inference, batch_size=1, num_gpus=1, concurrency=4) 
        ds = ds.map_batches(write, batch_size=1, num_cpus=0.99, concurrency=3) 

    else:
        ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
        ds = ds.map_batches(consume, batch_size=1, num_cpus=0.99)
        ds = ds.map_batches(inference, batch_size=1, num_gpus=1) 
        ds = ds.map_batches(write, batch_size=1, num_cpus=0.99)

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
    timeline_utils.save_timeline_with_cpus_gpus(f"timeline_{'ray' if not is_flink else 'flink'}_four_stage.json", NUM_CPUS,  NUM_GPUS)
    ray.shutdown()

if __name__ == "__main__": 
    # main(is_flink=True) 
    main(is_flink=False)
