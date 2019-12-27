import ray
from ray.streaming.config import Config
from ray.streaming.streaming import Environment, Conf


def test_word_count():
    ray.init()
    env = Environment(config=Conf(channel_type=Config.NATIVE_CHANNEL))
    env.read_text_file(__file__) \
        .set_parallelism(1) \
        .filter(lambda x: "word" in x) \
        .inspect(lambda x: print("result", x))
    env_handle = env.execute()
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    ray.shutdown()


if __name__ == "__main__":
    test_word_count()
