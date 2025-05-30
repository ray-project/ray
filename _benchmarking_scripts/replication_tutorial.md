# File directory tutorial:

## Core Files (Outside _benchmarking_scripts)
- `python/ray/serve/_private/request_router/prefix_aware_request_router.py`: Implements the prefix-aware request router that routes requests to replicas based on prefix matching
- `python/ray/llm/_internal/serve/request_router/prefix_aware/prefix_tree.py`: Data structure for efficient prefix matching and tracking of replica KV cache states
- `python/ray/llm/tests/serve/cpu/deployments/test_prefix_aware_request_router.py`: Unit tests for the prefix-aware router
- `python/ray/llm/tests/serve/cpu/deployments/test_prefix_tree.py`: Unit tests for the prefix tree data structure
- `python/ray/llm/_internal/serve/deployments/llm/llm_server.py`: Main server implementation that wraps LLM deployments with request routers

## Benchmarking Scripts (_benchmarking_scripts/)
- `sweep_strategies.py`: Main script for running benchmarks with different request routers and configurations
- `benchmark.py`: Script for running individual benchmarks and measuring performance metrics
- `visualize_results.ipynb`: Jupyter notebook for visualizing benchmark results
- `prefix_tree_operation_benchmark.ipynb`: Jupyter notebook for benchmarking prefix tree operations and analyzing their performance
- `config.yaml`: Simple configuration file for testing the LLM server with basic settings
- `logs/`: Directory containing stdout/stderr logs for each benchmark run
- `csv_results/`: Directory containing benchmark results in CSV format
  - `serve_sharegpt_sweep_results.csv`: Serve metrics from ShareGPT benchmarks
  - `vllm_sharegpt_sweep_results.csv`: vLLM metrics from ShareGPT benchmarks
  - `serve_generated-shared-prefix_sweep_results.csv`: Serve metrics from synthetic benchmarks
  - `vllm_generated-shared-prefix_sweep_results.csv`: vLLM metrics from synthetic benchmarks
- `custom_results/`: Directory for storing custom benchmark results and analysis
  - `char_count_over_time/`: Contains measurements of prefix tree size over time by querying the `tenant_to_char_count` dictionary, useful for monitoring eviction policy effectiveness
  - `routing_mismatches/`: Contains logs investigating routing mismatches by comparing each task's initial request with its final assigned request
- `replication_tutorial.md`: This file - comprehensive guide for setting up and running benchmarks

# Setup:

## Environment setup
1. Create an Anyscale Workspace, using `anyscale/ray:nightly-py311-cu124` and 4xL4 head node. I tried setting `VLLM_USE_V1=1` as an environment variable, but a lot of annoying warnings came up so I turned it off. Also, VLLM V1 seemed to cause longer model loading times.
    - Weird bug that Seiji helped me discover: you need to turn off Pip Packages in Workspace -> Dependencies, or else we run into dependency issues.
    - Another weird bug: in fact you need to explicitly set `VLLM_USE_V1=0` or else vllm gets confused and tries to import stuff from V1.
2. `mkdir work, cd work`. Clone my repo, then `cd ray`.
3. `python python/ray/setup-dev.py` for `serve`, `llm`, and `serve/llm`.
4. To output vllm metrics more frequently than the default 10 seconds, run: `ray start --head --system-config='{"metrics_report_interval_ms": 100}' --port 5000 --metrics-export-port 5001 --dashboard-port 5002`.
    - My metric logging scripts are set up to curl from http://localhost:5001/metrics, instead of the default http://localhost:8085/metrics. If you switch to using `log_engine_metrics=True`, make sure to edit my scripts to curl from 8085. Also edit `sweep_strategies.py` to shut down the server after each benchmark run at the default dashboard port instead of 5002.
    - I've also modified `vllm_engine.py` to add `RayPrometheusStatLogger` to `MQLLMEngine`. I know that I can set `VLLM_USE_V1=1` to output metrics and bypass modifying `vllm_engine`, but besides the annoying warnings, longer model loading times I observed, I also don't know how to customize the metrics report interval. So all my results are obtained with VLLM V0.
5. `pip install vllm==0.8.5`. You might need to `pip uninstall llvmlite && pip install llvmlite`
6. Some of Gene's changes that aren't merged into master yet aren't compiled in protobuf by nightly. So I manually regenerate protobuf:
    ```bash
    ci/env/install-bazel.sh
    cd ~/bin/
    export PATH="$HOME/bin:$PATH"
    bazel --version
    cd ~/default/work/ray
    bazel build //:install_py_proto
    ```
7. To run unit tests:
    ```bash
    pip install -c python/requirements_compiled.txt -r python/requirements/test-requirements.txt
    pip install -r python/requirements/llm/llm-requirements.txt -r python/requirements/llm/llm-test-requirements.txt
    python -m pytest -v -s /home/ray/default/work/ray/python/ray/llm/tests/serve/cpu/deployments/test_prefix_tree.py
    python -m pytest -v -s /home/ray/default/work/ray/python/ray/llm/tests/serve/cpu/deployments/test_prefix_aware_request_router.py
    ```
8. Linting:
    ```bash
    pip install -c python/requirements_compiled.txt -r python/requirements/lint-requirements.txt
    scripts/format.sh
    ./ci/lint/lint.sh pre_commit
    ```

## Spin up vLLM replica
1. `cd work/_benchmarking_scripts`
2. `serve run config.yaml` will run `build_openai_app` and spin up 2 Qwen-0.5B models, accessible at `localhost:8000`. Ensure `accelerator_type` matches your workspace's head node.
3. By default, the replicas use the Pow2 request router, except I modified `llm_server.py`: I wrap `LLMDeployment` with `@serve.deployment(request_router=PrefixAwareRequestRouter)`. So these two vLLM replicas will be configured with a prefix aware request router.
    - To revert back to Pow2, just comment out `@serve.deployment(request_router=PrefixAwareRequestRouter)`. Previously, I did hack together a configuration field in `llm_configs` in `config.yaml`, but I removed this to use Gene's `@serve.deployment` method. After Gene's PRs for custom request routing API land, there should be a field configurable in `config.yaml` to set the request router.
4. You can run a single query to make sure it was configured correctly:
    ```
    curl -X POST http://localhost:8000/v1/completions \
        -H "Content-Type: application/json" \
        -d '{"model":"Qwen/Qwen2.5-1.5B-Instruct","prompt":"Why is the sky blue?","max_tokens":50, "stream": false}'
    ```
5. When I was implementing and tinkering with the prefix aware router, my workflow was usually:
    1. Make a change to the prefix router logic.
    2. Add logs in the router (e.g. print the input text, the matched text, the matched replica).
    3. `serve run config.yaml`.
    4. Send curl queries and observe if the logs match my expectations.

## Benchmark
1. `benchmark.py` is based off of [sglang's serving benchmark](https://github.com/sgl-project/sglang/blob/844a8f42c74bcd917e9f1456d406ba4f1deebda3/python/sglang/bench_serving.py#L4). This script generates a batch of queries, sends them to a port that corresponds to a running LLM router, and measures serving metrics. For example, this is the same command I used for all my benchmark results:
    ```bash
    python -m benchmark \
        --backend vllm \
        --model Qwen/Qwen2.5-1.5B-Instruct \
        --host 127.0.0.1 \
        --port 8000 \
        --dataset-name sharegpt \
        --dataset-link "https://huggingface.co/datasets/samos123/share-gpt-long-convos/resolve/main/sharegpt_16_messages_or_more.json" \
        --output-file benchmark_results.jsonl \
        --min-output-len 10 \
        --max-output-len 200 \
        --max-concurrency 40 \
        --request-rate 100 \
        --with-warmup false \
        --disable-ignore-eos false \
        --disable-stream false \
        --max-conversations 10000 \
        --num-prompts 1000
    ```
    Note: This command only runs a benchmark on a given port, it does not spin up the server on that port. To successfully run this, first run `serve run config.yaml`, then run the command above. Or, run `python sweep_strategies`, which handles both starting the server *and* running the benchmark.

    General benchmark parameters:
    - `max-concurrency=40` means the client will only have max 40 requests ongoing at once. I set this to 40 because we have 4 LLM replicas, each with their own `max-concurrency=20`, and we want them to each be saturated but not overloaded.
    - `request-rate=100` instead of infinity helps request send-offs to be staggered instead of immediate. When this was set to infinity, we noticed jumps and drops in the load distribution graph (see the slide "3/27-4/2 Progress" in my result slides).
    - `disable-ignore-eos=false` means the LLM does not stop generating once it generates an EOS token. This is intended since to control the `output-len` for each request.
    - `disable-stream=false` means the response is streamed back to the client, which lets us measure TTFT and TPOT. When we were using a Serve deployment instead of Ray actor for the request router, having streaming on meant the event loop was bogged down, causing long deployment overhead. See the slide "4/17-4/30 Progress" to see a comparison between streaming on and off. But now that we use a Ray actor, the overhead of `actor.remote()` calls is very small.

    Parameters specific to ShareGPT:
    - The dataset is downloaded from [ShareGPT](https://huggingface.co/datasets/samos123/share-gpt-long-convos/tree/main). I don't keep it in the file directory since it's too large for Github.
    - Each request's output length is a random integer between `min-output-len` and `max-output-len`. This is preferred over a constant output length so requests finish at staggered times.
    - `max-conversations=10_000` limits the pool of conversations to select from. For example, say the ShareGPT dataset has 100K conversations, each with 5 back-and-forth User-Assistant turns. The way our requests are created is we first select `max-conversations=10K` out of the 100K conversations. Then, we break the 10K selected conversations into 50K pieces of conversations. To clarify, suppose one conversation looks like "User: U1 / Asst: A1 / User: U2 ... Asst: A5". Then we extract 5 conversation pieces: "User: U1 / Asst: A1", "User: U1 ... Asst: A2", ..., "User: U1 ... Asst: A5". Now, we select `num-prompts=1K` out of the 50K pieces of conversations. I find that setting `max-conversations=10K, num-prompts=1K` results in about 333 unique "groups" of prompts, with about 3 prompts per group. This is a good balance to measure the improvement of prefix-aware over pow2: if we have 1000 unique groups of prompts, then we'd have no KV cache reuse, and prefix-aware would be no better than pow2. If we have 1 unique group of 1000 prompts, then all the replicas' KV caches would be saturated anyways, so pow2 will be very similar to prefix-aware. Having a middle-ground means that pow2 won't be able to smartly route requests to utilize GPUs' KV caches, whereas prefix aware will.

2. So `benchmark.py` runs a single benchmark by generating a batch of queries and sending them to a running LLM router. But how do I run multiple benchmarks, with differing request routers?
    - I wrote `sweep_strategies.py` to do this. It uses the parameters set in `DEFAULT_CONFIG` to 1) set up an LLM server and 2) call `benchmark.py`.
    - For example, here are the default configs:
        ```py
        # Configs to set up server:
        host: 127.0.0.1,
        router_port: 8000,
        routing_strategies_dict: {
            "pow_of_2": "ray.serve...PowerOfTwoChoicesRequestRouter",
            "prefix_aware": "ray.serve...PrefixAwareRequestRouter"
        }
        model_name: "Qwen/Qwen2.5-1.5B-Instruct",
        gpu_type: "L4",
        num_servers: 4,
        enable_prefix_caching: True, # This isn't necessary because it's by default on, but I include anyways
        enable_chunked_prefill: True, # This isn't on for VLLM V0 AFAIK
        benchmark_label: "prefix_aware_with_eviction_loop", # Some identifiable label
        with_warmup: False, # If True, runs the benchmark twice, but doesn't reset the server in between. Essentially, this "warms up" the KV caches. Used initially to test that prefix-caching is indeed working (the second run should be faster than the first).

        # Configs to run benchmark (same as the ones in `benchmark.py`):
        # e.g. max_concurrency, min-output-len, disable_ignore_eos, ...
        ```
    - So if I make a code change to `PrefixAwareRequestRouter.py` and want to benchmark its performance, I just have to edit `benchmark_label`, run `python sweep_strategies.py`, and the benchmark will be run with my default parameters. If I'm not editing Pow2, I can comment out "pow_of_2" in `routing_strategies_dict` so the benchmark is only being run with the prefix aware request router.
        - Note: the ability to sweep multiple strategies in a single `python sweep_strategies.py` is dependent on being able to configure `request_router` in `temp_config.yaml`. I mentioned in "Spin up vLLM replica" that I removed this in favor of the `@serve.deployment(request_router=...)` wrapper, but this means that `sweep_strategies.py` currently does *not* spin up LLM servers with different request routers. You have to manually comment out `@serve.deployment()` in `llm_server.py` if you want to switch between PrefixAware, Pow2, or another request router.
    - Also, I redirect the stdout to `logs/{strategy}_stdout.log` and `logs/{strategy}_stderr.log`, so if you have logging statements in your code, you can look for them there. These can be large files, so delete them when pushing to Git.
    - By default, `benchmark.py` doesn't write the results anywhere. I modify it to take in `output-file` as a parameter, and write the results there. Then, `sweep_strategies.py` will generate an `output-file` path, call `benchmark.py` with it, read the written results, and compile them in a single CSV line to `serve_sharegpt_sweep_results.csv`.
        - Note: you'll notice I have `serve_generated-shared-prefix_sweep_results.csv`. These are leftover from when I was using a different dataset, before ShareGPT. This dataset is designed by [SGLang](https://github.com/sgl-project/sglang/pull/1990) and generates synthetic data with configurable prefix length, output length, number of prefix groups. Essentially, it offers more fine-grained control over how much "prefix sharing" you want in the dataset. Here's a writeup on [Generated Shared Prefix](https://docs.google.com/document/d/1Osit1QZJDktvx8ETjSIpHtN80k42KUzYHmoImmXmuEk/edit?tab=t.0). I still leave the option to use Generated Shared Prefix instead of ShareGPT; just change `dataset-name` to `generated-shared-prefix` in `sweep_strategies.py`.
    - For example, here's the output of a single benchmark run:

        | Field                         | Value                             |
        |------------------------------|-----------------------------------|
        | **The first half are just the benchmark parameters:**
        | gpu_type                     | L4                                 |
        | model_name                   | Qwen/Qwen2.5-1.5B-Instruct         |
        | num_servers                  | 4                                  |
        | enable_prefix_caching        | True                               |
        | enable_chunked_prefill       | True                               |
        | benchmark_label              | prefix_aware_with_eviction           |
        | request_router               | prefix_aware                       |
        | min_output_len               | 10                                 |
        | max_output_len               | 200                                |
        | max_concurrency              | 40                                 |
        | request_rate                 | 100                                |
        | with_warmup                  | False                              |
        | disable_ignore_eos           | False                              |
        | disable_stream               | False                              |
        | max_conversations            | 10000                              |
        | num_prompts                  | 1000                               |
        | **The second half are the benchmark metrics:** |
        | duration                     | 56.4809                            |
        | completed                    | 1000                               |
        | request_throughput           | 17.7051                            |
        | input_throughput             | 31506.2116                         |
        | output_throughput            | 1805.4434                          |
        | mean_ttft_ms                 | 136.1831                           |
        | median_ttft_ms               | 113.4312                           |
        | std_ttft_ms                  | 110.973                            |
        | p99_ttft_ms                  | 715.0978                           |
        | mean_tpot_ms                 | 20.3178                            |
        | median_tpot_ms               | 19.8729                            |
        | std_tpot_ms                  | 3.0302                             |
        | p99_tpot_ms                  | 30.5048                            |
        | mean_itl_ms                  | 20.4028                            |
        | median_itl_ms                | 0.0142                             |
        | std_itl_ms                   | 28.7998                            |
        | p99_itl_ms                   | 101.4521                           |
        | mean_e2e_latency_ms          | 2194.5704                          |
        | median_e2e_latency_ms        | 2106.8943                          |
    - Note that these metrics are as measured by the client, *not* VLLM. I separately record the VLLM metrics by querying `localhost:5001` at the very end of the benchmark. This gives me the TTFT, TPOT, E2E, etc. for each replica as measured by VLLM. I average these across replicas, and write to `vllm_sharegpt_sweep_results.csv`. These are the recorded metrics:
        ```
        prompt_tokens_tot   al, generation_tokens_total, request_success_total,time_to_first_token_seconds, time_per_output_token_seconds, e2e_request_latency_seconds,request_queue_time_seconds, request_inference_time_seconds, request_prefill_time_seconds,request_decode_time_seconds
        ```
3. Great! So now you can run `sweep_strategies.py`, wait 1-2 minutes, and the results will pop up in `serve_sharegpt_sweep_results.csv` and `vllm_sharegpt_sweep_results.csv`. Now comes the fun part: visualization.
    - `visualize_results.ipynb` is your one-stop-shop. Simply copy the results you want to visualize from `{serve, vllm}_sharegpt_sweep_results.csv` to `{serve, vllm}_chosen_sweep_results.csv` and call any visualization function with that file path.
