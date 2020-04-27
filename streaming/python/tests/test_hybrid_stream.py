import json
import ray
from ray.streaming import StreamingContext
import subprocess
import os


def map_func1(x):
    print("HybridStreamTest map_func1", x)
    return str(x)


def filter_func1(x):
    print("HybridStreamTest filter_func1", x)
    return "b" not in x


def sink_func1(x):
    print("HybridStreamTest sink_func1 value:", x)


def test_hybrid_stream():
    subprocess.check_call(
        ["bazel", "build", "//streaming/java:all_streaming_tests_deploy.jar"])
    current_dir = os.path.abspath(os.path.dirname(__file__))
    jar_path = os.path.join(
        current_dir,
        "../../../bazel-bin/streaming/java/all_streaming_tests_deploy.jar")
    jar_path = os.path.abspath(jar_path)
    print("jar_path", jar_path)
    java_worker_options = json.dumps(["-classpath", jar_path])
    print("java_worker_options", java_worker_options)
    assert not ray.is_initialized()
    ray.init(
        load_code_from_local=True,
        include_java=True,
        java_worker_options=java_worker_options,
        _internal_config=json.dumps({
            "num_workers_per_process_java": 1
        }))

    sink_file = "/tmp/ray_streaming_test_hybrid_stream.txt"
    if os.path.exists(sink_file):
        os.remove(sink_file)

    def sink_func(x):
        print("HybridStreamTest", x)
        with open(sink_file, "a") as f:
            f.write(str(x))

    ctx = StreamingContext.Builder().build()
    ctx.from_values("a", "b", "c") \
        .as_java_stream() \
        .map("io.ray.streaming.runtime.demo.HybridStreamTest$Mapper1") \
        .filter("io.ray.streaming.runtime.demo.HybridStreamTest$Filter1") \
        .as_python_stream() \
        .sink(sink_func)
    ctx.submit("HybridStreamTest")
    import time
    time.sleep(3)
    ray.shutdown()
    with open(sink_file, "r") as f:
        result = f.read()
        assert "a" in result
        assert "b" not in result
        assert "c" in result


if __name__ == "__main__":
    test_hybrid_stream()
