import argparse
import json
import os
import requests
import subprocess
import sys
import time
from datetime import datetime
from typing import Any
import pandas as pd
import yaml

DEFAULT_CONFIG = {
    # Server Info
    "host": "127.0.0.1",
    "router_port": 8000,
    # "worker_ports": "8001,8002,8003,8004",
    "routing_strategies_dict": {
        # "random": "ray.serve._private.request_router.random_router.RandomRequestRouter",
        # "round_robin": "ray.serve._private.request_router.round_robin_router.RoundRobinRequestRouter",
        # "pow_of_2": "ray.serve._private.request_router.pow_2_router.PowerOfTwoChoicesRequestRouter",
        "prefix_aware": "ray.serve._private.request_router.prefix_aware_router.PrefixAwareRequestRouter",
    },
    # Model Info
    "model_name": "Qwen/Qwen2.5-1.5B-Instruct",
    "gpu_type": "L4",
    "num_servers": 4,
    "enable_prefix_caching": True,
    "enable_chunked_prefill": True,
    # Benchmark Info
    "benchmark_label": "load_distribution",
    "dataset_name": "sharegpt",
    "max_concurrency": 40,  # Max concurrency (total)
    "min_output_len": 10,
    "max_output_len": 200,
    "with_warmup": False,
    "disable_ignore_eos": False,  # If false, will ignore EOS token and generate output_len tokens. If True, might stop early at EOS token. Use false for more control.
    "disable_stream": False,
    "request_rate": 100,
    # Generate Shared Prefix Info
    "gen-num-prefixes": 1000,
    "gen-suffixes-per-prefix": 2,
    "gen-prefix-len": 1000,
    "gen-suffix-len": 1000,
    # ShareGPT Info
    "num_prompts": 1000,  # Number of prompts to sample from ShareGPT
    "max_conversations": 10000,  # Max conversations to include from ShareGPT; num_unique_prefixes is approximately 3/100 * max_conversations.
    # To aim for average_prompts_per_prefix = 3, set max_conversations = 10 * num_prompts.
    "dataset_link": "https://huggingface.co/datasets/samos123/share-gpt-long-convos/resolve/main/sharegpt_16_messages_or_more.json",
}


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run a benchmark sweep for ShareGPT with different routing strategies"
    )

    # Server info
    parser.add_argument("--host", type=str, default=DEFAULT_CONFIG["host"], help="Host")
    parser.add_argument(
        "--router-port",
        type=int,
        default=DEFAULT_CONFIG["router_port"],
        help="Router port",
    )
    # parser.add_argument("--worker-ports", type=str, default=DEFAULT_CONFIG["worker_ports"], help="Comma-separated list of worker ports")
    parser.add_argument(
        "--routing-strategies-dict",
        type=str,
        nargs="+",
        default=DEFAULT_CONFIG["routing_strategies_dict"],
        help="List of request router strategies paths to benchmark",
    )

    # Model info
    parser.add_argument(
        "--model-name",
        type=str,
        default=DEFAULT_CONFIG["model_name"],
        help="Model name",
    )
    parser.add_argument(
        "--gpu-type",
        type=str,
        default=DEFAULT_CONFIG["gpu_type"],
        help="GPU type (e.g., A100, H100)",
    )
    parser.add_argument(
        "--num-servers",
        type=int,
        default=DEFAULT_CONFIG["num_servers"],
        help="Number of servers",
    )
    parser.add_argument(
        "--enable-prefix-caching",
        type=bool,
        default=DEFAULT_CONFIG["enable_prefix_caching"],
        help="Whether prefix caching is enabled",
    )
    parser.add_argument(
        "--enable-chunked-prefill",
        type=bool,
        default=DEFAULT_CONFIG["enable_chunked_prefill"],
        help="Whether chunked prefill is enabled",
    )

    # Benchmark info
    parser.add_argument(
        "--benchmark-label",
        type=str,
        default=DEFAULT_CONFIG["benchmark_label"],
        help="Benchmark label",
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default=DEFAULT_CONFIG["dataset_name"],
        help="Dataset name",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_CONFIG["max_concurrency"],
        help="Maximum concurrency (total)",
    )
    parser.add_argument(
        "--request-rate",
        type=Any,
        default=DEFAULT_CONFIG["request_rate"],
        help="Request rate",
    )
    parser.add_argument(
        "--min-output-len",
        type=int,
        default=DEFAULT_CONFIG["min_output_len"],
        help="Minimum output length",
    )
    parser.add_argument(
        "--max-output-len",
        type=int,
        default=DEFAULT_CONFIG["max_output_len"],
        help="Maximum output length",
    )
    parser.add_argument(
        "--with-warmup",
        type=bool,
        default=DEFAULT_CONFIG["with_warmup"],
        help="Whether to run warmup",
    )
    parser.add_argument(
        "--disable-ignore-eos",
        type=bool,
        default=DEFAULT_CONFIG["disable_ignore_eos"],
        help="Whether to disable ignoring EOS",
    )
    parser.add_argument(
        "--disable-stream",
        type=bool,
        default=DEFAULT_CONFIG["disable_stream"],
        help="Whether to disable streaming",
    )

    # Generate Shared Prefix info
    parser.add_argument(
        "--gen-num-prefixes",
        type=int,
        default=DEFAULT_CONFIG["gen-num-prefixes"],
        help="Number of prefixes for generated-shared-prefix dataset",
    )
    parser.add_argument(
        "--gen-suffixes-per-prefix",
        type=int,
        default=DEFAULT_CONFIG["gen-suffixes-per-prefix"],
        help="Suffixes per prefix for generated-shared-prefix dataset",
    )
    parser.add_argument(
        "--gen-prefix-len",
        type=int,
        default=DEFAULT_CONFIG["gen-prefix-len"],
        help="Prefix length for generated-shared-prefix dataset",
    )
    parser.add_argument(
        "--gen-suffix-len",
        type=int,
        default=DEFAULT_CONFIG["gen-suffix-len"],
        help="Suffix length for generated-shared-prefix dataset",
    )

    # ShareGPT info
    parser.add_argument(
        "--max-conversations",
        type=int,
        default=DEFAULT_CONFIG["max_conversations"],
        help="Maximum number of conversations to include from ShareGPT",
    )
    parser.add_argument(
        "--num-prompts",
        type=int,
        default=DEFAULT_CONFIG["num_prompts"],
        help="Number of prompts to sample from ShareGPT",
    )
    parser.add_argument(
        "--dataset-link",
        type=str,
        default=DEFAULT_CONFIG["dataset_link"],
        help="Link to ShareGPT dataset (will be downloaded every time)",
    )

    return parser.parse_args()


# def reset_prefix_caches(host, worker_ports):
#     """Reset prefix caches for all workers"""
#     print("Resetting prefix caches for all workers...")

#     # Reset worker caches
#     for port in worker_ports.split(","):
#         try:
#             response = requests.post(f"http://{host}:{port.strip()}/reset_prefix_cache")
#             print(f"Worker cache reset on port {port}: {response.status_code}")
#         except Exception as e:
#             print(f"Failed to reset worker cache on port {port}: {e}")


def restart_server_with_strategy(strategy, args):
    """Restart the server with a specific routing strategy."""
    print(f"\nRestarting server with routing strategy: {strategy}")

    # # Kill existing server process if running
    # print(f"Shutting down existing server...")
    # try:
    #     # Shut down the Ray Serve instance
    #     subprocess.run(["serve", "shutdown"], check=False)
    #     time.sleep(2)  # Give it time to shut down
    # except Exception as e:
    #     print(f"Error stopping server: {e}")

    # Create a temporary JSON config for passing arguments
    config = {
        "applications": [
            {
                "args": {
                    "llm_configs": [
                        {
                            "model_loading_config": {
                                "model_id": args.model_name,
                                "model_source": args.model_name,
                            },
                            "accelerator_type": args.gpu_type,
                            "engine_kwargs": {
                                "disable_log_requests": True,
                                "enable_prefix_caching": args.enable_prefix_caching,
                                "enable_chunked_prefill": args.enable_chunked_prefill,
                            },
                            "deployment_config": {
                                "autoscaling_config": {
                                    "min_replicas": args.num_servers,
                                    "max_replicas": args.num_servers,
                                    "initial_replicas": args.num_servers,
                                },
                            },
                            # "log_engine_metrics": True,
                            # "request_router_cls_path": args.routing_strategies_dict[strategy]
                        }
                    ]
                },
                "import_path": "ray.serve.llm:build_openai_app",
                "name": "llm_app",
                "route_prefix": "/",
            }
        ]
    }
    # Write to a temporary JSON file
    temp_path = "temp_config.yaml"
    with open(temp_path, "w") as f:
        yaml.dump(config, f)

    print(
        f"Starting server with strategy '{strategy}': {args.routing_strategies_dict[strategy]}"
    )
    cmd = ["serve", "run", temp_path]
    print(f"Executing: {' '.join(cmd)}")

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Open log files for writing
    stdout_log = open(f"logs/{strategy}_stdout.log", "w")
    stderr_log = open(f"logs/{strategy}_stderr.log", "w")

    server_process = subprocess.Popen(
        cmd,
        stdout=stdout_log,
        stderr=stderr_log
        # stdout=subprocess.DEVNULL,
        # stderr=subprocess.DEVNULL
    )

    # Wait for server to start - give it more time and retry health checks
    print("Waiting for server to start...")
    max_retries = 20
    retry_interval = 5  # seconds
    for i in range(max_retries):
        time.sleep(retry_interval)
        try:
            response = requests.get(f"http://{args.host}:{args.router_port}/v1/models")
            if response.status_code == 200:
                print(
                    f"Health check attempt {i+1}/{max_retries}: Server started successfully with strategy {strategy}"
                )
                # Delete the temporary config file after server process starts
                try:
                    os.remove(temp_path)
                    print(f"Deleted temporary config file: {temp_path}")
                except Exception as e:
                    print(f"Warning: Failed to delete temporary config file: {e}")
                return server_process, stdout_log, stderr_log
            else:
                print(
                    f"Health check attempt {i+1}/{max_retries}: Status code {response.status_code}"
                )
        except Exception as e:
            print(f"Health check attempt {i+1}/{max_retries}: {e}")
    print(
        f"Failed to start server with strategy {strategy} after {max_retries} attempts"
    )
    # Delete the temporary config file if server fails to start
    try:
        os.remove(temp_path)
        print(f"Deleted temporary config file: {temp_path}")
    except Exception as e:
        print(f"Warning: Failed to delete temporary config file: {e}")
    return None, None, None


def run_single_benchmark(strategy, args):
    """Run a single benchmark with the given routing strategy and return the result."""
    print(f"\nRunning benchmark with routing strategy = {strategy} ...")

    now = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f"sharegpt_{strategy}_{now}.jsonl"

    # Reset prefix caches before the benchmark
    # print(f"Resetting prefix caches before benchmark ...")
    # reset_prefix_caches(args.host, args.worker_ports)
    # time.sleep(5)

    if args.dataset_name == "generated-shared-prefix":
        cmd = [
            "python",
            "-m",
            "benchmark",
            "--backend",
            "vllm",
            "--model",
            args.model_name,
            "--host",
            str(args.host),
            "--port",
            str(args.router_port),
            "--dataset-name",
            "generated-shared-prefix",
            "--output-file",
            str(output_file),
            "--min-output-len",
            str(args.min_output_len),
            "--max-output-len",
            str(args.max_output_len),
            "--max-concurrency",
            str(args.max_concurrency),
            "--request-rate",
            str(args.request_rate),
            "--with-warmup",
            str(args.with_warmup),
            "--disable-ignore-eos",
            str(args.disable_ignore_eos),
            "--disable-stream",
            str(args.disable_stream),
            # Parameters specific to dataset
            "--gen-num-prefixes",
            str(args.gen_num_prefixes),
            "--gen-suffixes-per-prefix",
            str(args.gen_suffixes_per_prefix),
            "--gen-prefix-len",
            str(args.gen_prefix_len),
            "--gen-suffix-len",
            str(args.gen_suffix_len),
        ]
    if args.dataset_name == "sharegpt":
        cmd = [
            "python",
            "-m",
            "benchmark",
            "--backend",
            "vllm",
            "--model",
            args.model_name,
            "--host",
            str(args.host),
            "--port",
            str(args.router_port),
            "--dataset-name",
            "sharegpt",
            "--dataset-link",
            args.dataset_link,
            "--output-file",
            str(output_file),
            "--min-output-len",
            str(args.min_output_len),
            "--max-output-len",
            str(args.max_output_len),
            "--max-concurrency",
            str(args.max_concurrency),
            "--request-rate",
            str(args.request_rate),
            "--with-warmup",
            str(args.with_warmup),
            "--disable-ignore-eos",
            str(args.disable_ignore_eos),
            "--disable-stream",
            str(args.disable_stream),
            # Parameters specific to dataset
            "--max-conversations",
            str(args.max_conversations),
            "--num-prompts",
            str(args.num_prompts),
        ]
    subprocess.run(cmd, check=True)

    with open(output_file, "r") as f:
        line = f.readline().strip()
        result = json.loads(line)
        os.remove(output_file)

    # Add additional metadata to the result.
    result.update(
        {
            "gpu_type": args.gpu_type,
            "model_name": args.model_name,
            "num_servers": args.num_servers,
            "enable_prefix_caching": args.enable_prefix_caching,
            "enable_chunked_prefill": args.enable_chunked_prefill,
            "benchmark_label": args.benchmark_label,
            "routing_strategy": strategy,
            "min_output_len": args.min_output_len,
            "max_output_len": args.max_output_len,
            "max_concurrency": args.max_concurrency,
            "request_rate": args.request_rate,
            "with_warmup": args.with_warmup,
            "disable_ignore_eos": args.disable_ignore_eos,
            "disable_stream": args.disable_stream,
        }
    )
    if args.dataset_name == "generated-shared-prefix":
        result.update(
            {
                "num_prefixes": args.gen_num_prefixes,
                "suffixes_per_prefix": args.gen_suffixes_per_prefix,
                "prefix_len": args.gen_prefix_len,
                "suffix_len": args.gen_suffix_len,
            }
        )
    elif args.dataset_name == "sharegpt":
        result.update(
            {
                "max_conversations": args.max_conversations,
                "num_prompts": args.num_prompts,
            }
        )
    return result


def save_results_to_csv(results, args):
    """Save the benchmark results to two CSV files: one for Serve metrics, one for vLLM metrics."""
    # === Shared column definitions ===
    shared_params = [
        "gpu_type",
        "model_name",
        "num_servers",
        "enable_prefix_caching",
        "enable_chunked_prefill",
        "benchmark_label",
        "routing_strategy",
        "min_output_len",
        "max_output_len",
        "max_concurrency",
        "request_rate",
        "with_warmup",
        "disable_ignore_eos",
        "disable_stream",
    ]

    if args.dataset_name == "generated-shared-prefix":
        dataset_params = [
            "num_prefixes",
            "suffixes_per_prefix",
            "prefix_len",
            "suffix_len",
        ]
    elif args.dataset_name == "sharegpt":
        dataset_params = ["max_conversations", "num_prompts"]
    else:
        dataset_params = []

    # === Serve result saving ===
    serve_result_keys = [
        "duration",
        "completed",
        "request_throughput",
        "input_throughput",
        "output_throughput",
        "mean_ttft_ms",
        "median_ttft_ms",
        "std_ttft_ms",
        "p99_ttft_ms",
        "mean_tpot_ms",
        "median_tpot_ms",
        "std_tpot_ms",
        "p99_tpot_ms",
        "mean_itl_ms",
        "median_itl_ms",
        "std_itl_ms",
        "p99_itl_ms",
        "mean_e2e_latency_ms",
        "median_e2e_latency_ms",
    ]

    ordered_columns = shared_params + dataset_params + serve_result_keys
    df = pd.DataFrame(results)
    df = df[[col for col in ordered_columns if col in df.columns]]

    # Round numeric values
    numeric_columns = df.select_dtypes(include=["float", "int"]).columns
    df[numeric_columns] = df[numeric_columns].round(4)

    # Save Serve results
    serve_csv_file = f"/home/ray/default/work/ray/_benchmarking_scripts/csv_results/serve_{args.dataset_name}_sweep_results.csv"
    df.to_csv(serve_csv_file, mode="a", index=False)
    print(f"\nAppended Serve results to {serve_csv_file}")

    # === vLLM metric collection and saving ===
    try:
        output = subprocess.check_output(
            ["curl", "-s", "http://localhost:5001/metrics"]
        ).decode("utf-8")
        # output = subprocess.check_output(["curl", "-s", "http://localhost:8085/metrics"]).decode("utf-8")
        lines = output.strip().split("\n")
        current_vllm_metrics = {}

        for line in lines:
            if line.startswith("#") or "vllm" not in line:
                continue

            parts = line.split()
            if len(parts) != 2:
                continue

            metric_line, value = parts
            try:
                value = float(value)
            except ValueError:
                continue

            # Parse metric name and labels
            if "{" in metric_line:
                name, label_str = metric_line.split("{", 1)
                label_str = label_str.rstrip("}")
                labels = dict(item.split("=") for item in label_str.split(","))
                labels = {k: v.strip('"') for k, v in labels.items()}
            else:
                name = metric_line
                labels = {}

            worker_id = labels.get("WorkerId", "unknown")
            if worker_id not in current_vllm_metrics:
                current_vllm_metrics[worker_id] = {}
            current_vllm_metrics[worker_id][name] = (
                current_vllm_metrics[worker_id].get(name, 0.0) + value
            )

        # === Aggregate vLLM metrics ===
        counter_metrics = [
            "ray_vllm:prompt_tokens_total",
            "ray_vllm:generation_tokens_total",
            "ray_vllm:request_success_total",
        ]

        average_metrics = [
            "ray_vllm:time_to_first_token_seconds",
            "ray_vllm:time_per_output_token_seconds",
            "ray_vllm:e2e_request_latency_seconds",
            "ray_vllm:request_queue_time_seconds",
            "ray_vllm:request_inference_time_seconds",
            "ray_vllm:request_prefill_time_seconds",
            "ray_vllm:request_decode_time_seconds",
        ]

        vllm_summary = {}

        # Sum counters
        for metric in counter_metrics:
            total = sum(
                worker.get(metric, 0.0) for worker in current_vllm_metrics.values()
            )
            vllm_summary[metric] = round(total, 4)
        # Compute averages
        for base_metric in average_metrics:
            sum_metric = f"{base_metric}_sum"
            count_metric = f"{base_metric}_count"
            total_sum = sum(
                worker.get(sum_metric, 0.0) for worker in current_vllm_metrics.values()
            )
            total_count = sum(
                worker.get(count_metric, 0.0)
                for worker in current_vllm_metrics.values()
            )
            average = total_sum / total_count if total_count > 0 else 0.0
            vllm_summary[base_metric] = round(average, 7)
        # === Format vLLM results with shared metadata ===
        base_record = {
            k: results[0][k] for k in shared_params + dataset_params if k in results[0]
        }
        vllm_record = {**base_record, **vllm_summary}
        vllm_df = pd.DataFrame([vllm_record])

        vllm_csv_file = f"/home/ray/default/work/ray/_benchmarking_scripts/csv_results/vllm_{args.dataset_name}_sweep_results.csv"
        vllm_df.to_csv(vllm_csv_file, mode="a", index=False)

    except subprocess.CalledProcessError:
        print("Error: Failed to curl vLLM metrics at http://localhost:5001/metrics")
        # print("Error: Failed to curl vLLM metrics at http://localhost:8085/metrics")


def main():
    """Main function to run the benchmark sweep."""
    args = parse_arguments()

    # Define sweep configurations
    sweeps_configs = [{} for _ in range(1)]

    # Loop through each sweep configuration
    for config_idx, sweep_config in enumerate(sweeps_configs):
        print(f"\n{'='*80}")
        print(f"Running sweep configuration {config_idx+1}/{len(sweeps_configs)}")
        print(f"Configuration: {sweep_config}")
        print(f"{'='*80}\n")

        # Update args with the current sweep configuration
        for key, value in sweep_config.items():
            setattr(args, key, value)

        # try:
        for strategy in args.routing_strategies_dict.keys():
            # Store the returned log file handles
            server_process, stdout_log, stderr_log = restart_server_with_strategy(
                strategy, args
            )

            # Run benchmark with this strategy
            try:
                result = run_single_benchmark(strategy, args)
                print(
                    "Sleeping for 5 seconds (to allow load distribution to be written to file and for results to be written to csv)..."
                )
                time.sleep(5)
                save_results_to_csv([result], args)
            except Exception as e:
                print(f"Error running benchmark with strategy {strategy}: {e}")
            finally:
                # Close log files
                stdout_log.close()
                stderr_log.close()

                # Clean up: stop server if still running
                subprocess.run(
                    ["serve", "shutdown", "-a", "http://localhost:5002", "--yes"],
                    check=False,
                )
                # subprocess.run(["serve", "shutdown", "--yes"], check=False)
                print("Sleeping for 5 seconds (to allow server to shut down)...")
                time.sleep(5)  # Give it time to shut down


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        sys.exit(1)
