import os
import sys

import ray
from ray.streaming import StreamingContext


def test_union_stream():
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))
    ctx = StreamingContext.Builder() \
        .option("streaming.metrics.reporters", "") \
        .build()
    sink_file = "/tmp/test_union_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            print("sink_func", x)
            f.write(str(x))

    stream1 = ctx.from_values(1, 2)
    stream2 = ctx.from_values(3, 4)
    stream3 = ctx.from_values(5, 6)
    stream1.union(stream2, stream3).sink(sink_func)
    ctx.submit("test_union_stream")
    import time
    slept_time = 0
    while True:
        if os.path.exists(sink_file):
            time.sleep(3)
            with open(sink_file, "r") as f:
                result = f.read()
                print("sink result", result)
                assert set(result) == {"1", "2", "3", "4", "5", "6"}
            print("Execution succeed")
            break
        if slept_time >= 60:
            raise Exception("Execution not finished")
        slept_time = slept_time + 1
        print("Wait finish...")
        time.sleep(1)

    ray.shutdown()


if __name__ == "__main__":
    test_union_stream()
