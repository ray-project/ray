import ray
from ray.streaming import StreamingContext
from ray.streaming.config import Config


def test_word_count():
    ray.init(load_code_from_local=True, include_java=True)
    ctx = StreamingContext.Builder() \
        .option(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL) \
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
    ctx.execute("word_count")
    import time
    time.sleep(10)
    ray.shutdown()


if __name__ == "__main__":
    test_word_count()
