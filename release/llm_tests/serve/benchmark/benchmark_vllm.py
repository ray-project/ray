# There is a dead-lock issue that arises due to gevent's monkey-patching
# https://github.com/ipython/ipython/issues/11730
# Fix: We do this import first before anything else
# Thus the # noqa tags are needed below
import gevent.monkey

gevent.monkey.patch_all()

import sys  # noqa: E402
import argparse  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
import subprocess  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from datetime import datetime  # noqa: E402

from bm import run_bm  # noqa: E402
from common import write_to_s3, get_llm_config  # noqa: E402

RAYLLM_RELEASE_TEST_PERF_SERVICE_NAME = "rayllm_release_test_perf_service"
THREAD_CLEANUP_TIMEOUT_S = 10


logger = logging.getLogger(__file__)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


def get_timestamp():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class ColoredLogger:
    HEADER = "\033[95m"
    SERVER = "\033[94m"
    CLIENT = "\033[92m"
    WARNING = "\033[93m"
    ERROR = "\033[91m"
    ENDC = "\033[0m"

    @staticmethod
    def log_server(msg):
        print(
            f"{ColoredLogger.SERVER}[SERVER {get_timestamp()}] {msg}{ColoredLogger.ENDC}"
        )

    @staticmethod
    def log_client(msg):
        print(
            f"{ColoredLogger.CLIENT}[CLIENT {get_timestamp()}] {msg}{ColoredLogger.ENDC}"
        )

    @staticmethod
    def log_error(msg):
        print(
            f"{ColoredLogger.ERROR}[ERROR {get_timestamp()}] {msg}{ColoredLogger.ENDC}"
        )


def stream_process_output(process, logger_func, stop_event, is_error: bool = False):
    while not stop_event.is_set():
        if is_error:
            output_line = process.stderr.readline()
        else:
            output_line = process.stdout.readline()
        if output_line:
            logger_func(output_line.strip())
        elif process.poll() is not None:
            break


def get_vllm_cli_args(llm_config):
    engine_kwargs = llm_config["engine_kwargs"]
    # When we define tokenizer_pool size, vllm, by default, uses Ray
    # that breaks the assumption that this script should not use ray
    # TODO (Kourosh): When the job issue with non driver ray
    # subprocesses are resolved we can remove these constraints
    engine_kwargs.pop("tokenizer_pool_extra_config", None)
    engine_kwargs.pop("tokenizer_pool_size", None)
    engine_kwargs.pop("tokenizer_pool_type", None)

    cli_args = ["--model", llm_config["model_loading_config"]["model_id"]]
    for key, value in engine_kwargs.items():
        cli_args.append("--" + key.replace("_", "-"))
        if isinstance(value, dict):
            cli_args.append(json.dumps(value))
        elif isinstance(value, bool):
            pass
        else:
            cli_args.append(str(value))

    cli_args.extend(
        ["--tensor-parallel-size", str(engine_kwargs["tensor_parallel_size"])]
    )

    if "max_model_len" in engine_kwargs:
        cli_args.extend(["--max-model-len", str(engine_kwargs["max_model_len"])])

    return cli_args


def get_ray_options(llm_config):
    num_gpus = llm_config["tensor_parallelism"]["degree"]
    acc_type = llm_config["accelerator_type"]
    resources = {f"accelerator_type:{acc_type}": 0.001}

    return {"num_gpus": num_gpus, "resources": resources}


def start_vllm_process(vllm_cli_args):
    server_process = subprocess.Popen(
        ["python", "-m", "vllm.entrypoints.openai.api_server"] + vllm_cli_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,  # Return strings from stdout/stderr
        bufsize=1,  # Line buffered
        env=os.environ.copy(),
    )

    return server_process


def run_vllm_benchmark(vllm_cli_args):
    stop_event = threading.Event()
    results = {}

    try:
        # Start the server process
        ColoredLogger.log_server("Starting vLLM server...")
        server_process = start_vllm_process(vllm_cli_args)

        # Start server output streaming thread
        server_thread = threading.Thread(
            target=stream_process_output,
            args=(server_process, ColoredLogger.log_server, stop_event),
            daemon=True,  # Daemonize the thread so it stops when the main thread stops.
        )
        server_thread.start()

        # Start server error streaming thread
        server_error_thread = threading.Thread(
            target=stream_process_output,
            args=(server_process, ColoredLogger.log_server, stop_event),
            kwargs={"is_error": True},
            daemon=True,  # Daemonize the thread so it stops when the main thread stops.
        )
        server_error_thread.start()

        # Wait for server to be ready
        server_ready = False
        start_time = time.time()
        timeout = 300  # 5 minutes timeout

        while not server_ready and time.time() - start_time < timeout:
            if server_process.poll() is not None:
                raise Exception("Server process terminated unexpectedly")

            # Check if server is responding
            try:
                import requests

                response = requests.get("http://localhost:8000/health")
                if response.status_code == 200:
                    server_ready = True
                    ColoredLogger.log_server("Server is ready!")
            except Exception:
                time.sleep(1)
                continue

        if not server_ready:
            raise TimeoutError("Server failed to start within timeout period")

        # Start benchmark
        ColoredLogger.log_client("Starting benchmark...")
        results = run_bm(
            api_url="http://localhost:8000",
            api_key="NONE",
            concurrency=[1, 2, 4, 8, 16, 32],
            run_time="1m",
            prompt_tokens=256,
            max_tokens=64,
            stream=False,
            summary_file="./results.csv",
        )
        print(
            "Writing final result to AWS Firehose:",
            json.dumps(results, indent=4, sort_keys=True),
            sep="\n",
        )

        ColoredLogger.log_client("Benchmark completed successfully")

    except Exception as e:
        ColoredLogger.log_error(f"Error during benchmark: {str(e)}")
        raise

    finally:
        # Clean up

        if "server_process" in locals():
            ColoredLogger.log_server("Shutting down server...")
            server_process.terminate()
            server_process.wait(timeout=THREAD_CLEANUP_TIMEOUT_S)

        stop_event.set()
        # Wait for all threads to complete
        if "server_thread" in locals():
            server_thread.join(timeout=THREAD_CLEANUP_TIMEOUT_S)
        if "server_error_thread" in locals():
            server_error_thread.join(timeout=THREAD_CLEANUP_TIMEOUT_S)

        # Wait some time to make sure everyting is cleanend up.
        time.sleep(5)

    return results


def upload_results_to_s3(s3_path, results, service_metadata):
    if any(result is None for result in results):
        raise ValueError(
            "Found None results during benchmarking. " "This should not have happened."
        )

    data_to_write = [{**result, **service_metadata} for result in results]
    write_to_s3(data_to_write, s3_path)


def main(pargs):
    llm_config = get_llm_config(pargs.llm_config)
    vllm_cli_args = get_vllm_cli_args(llm_config)

    results = run_vllm_benchmark(vllm_cli_args)

    tag = f"{llm_config['accelerator_type']}-TP{llm_config['engine_kwargs']['tensor_parallel_size']}"
    service_metadata = {
        "cloud_name": "",
        "service_name": "",
        "py_version": f"py{sys.version_info.major}{sys.version_info.minor}",
        "tag": tag,
        "vllm_engine": f"V{os.environ.get('VLLM_USE_V1', '')}",
    }

    # Post the results to S3
    if results:
        print(
            "Writing final result to AWS S3:",
            json.dumps(results, indent=4, sort_keys=True),
            sep="\n",
        )
        upload_results_to_s3(pargs.remote_result_path, results, service_metadata)
    else:
        raise ValueError(
            "For some reason the benchmarking results are empty. Something is wrong."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--llm-config",
        type=str,
        required=True,
        default="The LLM config to start vLLM engine",
    )
    parser.add_argument(
        "--remote-result-path",
        type=str,
        required=True,
        help="The remote s3 path to store intermediate results on.",
    )
    main(parser.parse_args())
