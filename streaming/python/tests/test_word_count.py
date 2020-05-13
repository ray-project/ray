import os
import ray
from ray.streaming import StreamingContext


def test_word_count():
    ray.init(load_code_from_local=True, include_java=True)
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
    ray.init(load_code_from_local=True, include_java=True)
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
    import time
    time.sleep(3)
    ray.shutdown()
    with open(sink_file, "r") as f:
        result = f.read()
        assert "a:2" in result
        assert "b:2" in result
        assert "c:2" in result


if __name__ == "__main__":
    # test_word_count()
    test_simple_word_count()
