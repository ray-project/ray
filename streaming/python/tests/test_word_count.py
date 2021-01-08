import os
import sys
import ray
from ray.streaming import StreamingContext
from ray.test_utils import wait_for_condition


def test_word_count():
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))
    ctx = StreamingContext.Builder() \
        .build()
    ctx.read_text_file(__file__) \
        .set_parallelism(1) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .filter(lambda x: "ray" not in x) \
        .sink(lambda x: print("result", x))
    ctx.submit("word_count")
    import time
    time.sleep(3)
    ray.shutdown()


def test_simple_word_count():
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))
    ctx = StreamingContext.Builder() \
        .build()
    sink_file = "/tmp/ray_streaming_test_simple_word_count.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        with open(sink_file, "a") as f:
            line = "{}:{},".format(x[0], x[1])
            print("sink_func", line)
            f.write(line)

    ctx.from_values("a", "b", "c") \
        .set_parallelism(1) \
        .flat_map(lambda x: [x, x]) \
        .map(lambda x: (x, 1)) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .sink(sink_func)
    ctx.submit("word_count")

    def check_succeed():
        if os.path.exists(sink_file):
            with open(sink_file, "r") as f:
                result = f.read()
                return "a:2" in result and "b:2" in result and "c:2" in result
        return False

    wait_for_condition(check_succeed, timeout=60, retry_interval_ms=1000)
    print("Execution succeed")
    ray.shutdown()


if __name__ == "__main__":
    test_word_count()
    test_simple_word_count()
