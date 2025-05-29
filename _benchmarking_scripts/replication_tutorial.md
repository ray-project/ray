# Setup:

## Environment setup
1. Create an Anyscale Workspace, using `anyscale/ray:nightly-py311-cu124` and 4xL4 head node. I tried setting `VLLM_USE_V1=1` as an environment variable, but a lot of annoying warnings came up so I turned it off. Also, VLLM V1 seemed to cause longer model loading times.
2. Make an empty work dir, then `cd work`. Clone my repo, then `cd ray`.
3. `python python/ray/setup-dev.py` for `serve`, `llm`, and `serve/llm`.
4. To output vllm metrics more frequently than the default 10 seconds, run: `ray start --head --system-config='{"metrics_report_interval_ms": 100}' --port 5000 --metrics-export-port 5001 --dashboard-port 5002`.
    - My metric logging scripts are set up to curl from http://localhost:5001/metrics, instead of the default http://localhost:8085/metrics.
    - I've also modified `vllm_engine.py` to add `RayPrometheusStatLogger` to `MQLLMEngine`. I know that I can set `VLLM_USE_V1=1` to output metrics and bypass modifying `vllm_engine`, but besides the annoying warnings, longer model loading times I observed, I also don't know how to customize the metrics report interval. So all my results are obtained with VLLM V0.
5. `pip install vllm==0.8.5`
6. Some of Gene's changes that aren't merged into master yet aren't compiled in protobuf by nightly. So I manually regenerate protobuf:
    ```
    ci/env/install-bazel.sh
    cd ~/bin/
    export PATH="$HOME/bin:$PATH"
    bazel --version
    cd ~/default/work/ray
    bazel build //:install_py_proto
    ```
7. To run unit tests:
    ```
    pip install -c python/requirements_compiled.txt -r python/requirements/test-requirements.txt
    pip install -r python/requirements/llm/llm-requirements.txt -r python/requirements/llm/llm-test-requirements.txt
    ```
8. Linting:
    ```
    pip install -c python/requirements_compiled.txt -r python/requirements/lint-requirements.txt
    scripts/format.sh
    ./ci/lint/lint.sh pre_commit
    ```

## Spin up vLLM replica
1. `cd work/_benchmarking_scripts`
2. `serve run config.yaml` will run `build_openai_app` and spin up 2 Qwen-0.5B models, accessible at `localhost:8000`. Ensure `accelerator_type` matches your workspace.
3. This will by default use Pow2 replica scheduler, except I modified `llm_server.py`: I wrap `LLMDeployment` with `serve.deployment(replica_scheduler=PrefixAwareReplicaScheduler)`. So these two vLLM replicas will be configured with a prefix aware scheduler.
4. You can run a single query to make sure it was configured correctly:
    ```
    curl -X POST http://localhost:8000/v1/completions \
        -H "Content-Type: application/json" \
        -d '{"model":"qwen-0.5b","prompt":"Why is the sky blue?","max_tokens":50, "stream": false}'
    ```
5. When I was implementing the prefix aware scheduler, my workflow was usually:
    1. Make a change to the prefix scheduler logic
    2. Add logs in the scheduler (e.g. print the input text, the matched text, the matched replica)
    3. `serve run config.yaml`
    4. Send curl queries and observe if the logs match my expectations.

## Benchmark
1. `benchmark.py` is based off of [sglang's serving benchmark](https://github.com/sgl-project/sglang/blob/844a8f42c74bcd917e9f1456d406ba4f1deebda3/python/sglang/bench_serving.py#L4). This script generates a batch of queries, sends them to a port, and measures serving metrics. For example, this is the same command I used for all my benchmark results:
    ```
    python -m benchmark \
        --backend vllm \
        --model Qwen/Qwen2.5-1.5B-Instruct \
        --host localhost \
        --port 8000 \
        --dataset-name sharegpt \
        --dataset-path /home/ray/default/work/ray/_benchmarking_scripts/sharegpt.json \
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
    General benchmark parameters:
    - If `dataset-path` does not exist, it will automatically download from the huggingface link (hardcoded in `benchmark.py`) to that path to be cached for future benchmarks.
    - `request-rate=100` instead of infinity also helps request send-offs to be staggered instead of immediate.
    - `disable-ignore-eos=false` means the LLM does not stop generating once it generates an EOS token.
    - `disable-stream=false` means the response is streamed back to the client, which lets us measure TTFT and TPOT.

    Parameters specific to ShareGPT:
    - Each request's output length is a random integer between `min-output-len` and `max-output-len`. This is preferred over a constant output length so requests finish at staggered times.
    - `max-conversations` limits the pool of conversations to select from. So if the dataset has 100K conversations, but `max-conversations=