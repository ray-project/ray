from automl.client import AutoMLClient
import logging
import sys
import time

logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

client = AutoMLClient("127.0.0.1:1234")

task_id = client.do_auto_ml(
    "s3://anonymous@m5-benchmarks/data/train/target.parquet", 
    "FOODS_1_001_CA_1", 
    #[6, 7],
    [6],
    ["ZNA", "ZZZ"]
)

while True:
    result = client.get_result(task_id)
    if result:
        logger.info(f"The task has already finished, the result {result}.")
        break
    else:
        logger.info(f"The task is not finished, wait...")
    time.sleep(5)

logger.info("Finished!")
