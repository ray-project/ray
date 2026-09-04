# KV-aware routing on Claude Code traces

This reproducer compares strict session affinity with two `KVAwareRouter` settings on the same Weka Claude Code replay.

## Setup

- **Ray cluster:** build a cluster image from `anyscale/ray-llm:2.58.0-py312-cu130` with Dynamo installed, then start the Ray cluster with that derived image:

  ```dockerfile
  FROM anyscale/ray-llm:2.58.0-py312-cu130
  RUN python -m pip install --no-cache-dir "ai-dynamo-runtime==1.4.0"
  ```

  The cluster needs **8× H100 GPUs**.

- **Model & Parallelism**: Serve `openai/gpt-oss-120b` as **4 replicas × TP=2**.
- **Client:** run the benchmark from a shell connected to that Ray cluster.
- **Model cache:** set `HF_HOME` to a Hugging Face cache that contains, or may download, `openai/gpt-oss-120b`.

Create an AIPerf environment on the client.

```bash
cd /home/ray/default
uv venv --system-site-packages --python python3.12 .venv-aiperf
source .venv-aiperf/bin/activate
uv pip install --editable ./aiperf
export HF_HOME=/path/to/hf-cache
```

Keep this virtual environment activated for the run.

`reproduce.sh` sources `scripts/env.sh`. Its small Ray runtime hook removes the managed cluster's inherited working directory before vLLM's EngineCore reconnects to Ray; without it, this cluster attempts to upload that multi-gigabyte directory for every engine.

## Run

Start from a clean serving environment and pass a artifact path.

```bash
cd /home/ray/default/ray/python/ray/llm/examples/blogs/kv_token_aware_routing/claude_code_traces
./reproduce.sh --out /path/to/artifact
```

A **cell** is one router variant measured at one target concurrency on its own fresh deployment. It runs 15 cells: three router variants at concurrency [8, 16, 24, 32, 40]. Each cell replays the workload for 900 seconds by default. Override this with `BENCHMARK_DURATION=...` for a separate experiment. The run stops at the first failed or invalid cell and retains its raw artifacts for inspection.

Each deployment is constructed with Ray's supported `build_openai_app` builder and started with `serve.run`. For a `KVAwareRouter` subclass, that builder automatically enables the vLLM KV-event publisher and Ray's corresponding event consumer; the benchmark does not call Ray's internal KV-event setup helper directly.

The command writes:

```text
/path/to/artifact/cc_traces_distribution.png
/path/to/artifact/cc_traces_router_comparison.png
```

It also retains the raw AIPerf exports, route logs, reconstructed dataset cache, and `cells.csv` used by the comparison plot.

## Workload

- `reproduce.sh` downloads the pinned revision of the [Weka Claude Code traces](https://huggingface.co/datasets/semianalysisai/cc-traces-weka-062126-256k), filters it to the committed 120K selection, and materializes the 71 roots under the output artifact directory. `data/weka_trace_selection.json` fixes the source revision, cap, exclusion, and resulting trace IDs; raw traces are not stored in this example.
- AIPerf deterministically reconstructs prompts from those blocks, carries each conversation’s prior turns forward, and assigns a stable correlation ID to each reconstructed main-agent or child-agent conversation.
- The replay passes AIPerf's `--unsafe-override` because its submission scenario accepts only a registered Hugging Face corpus, while this bundle intentionally replays a checked-in local slice. The trace manifest and all per-cell correctness checks still run; AIPerf marks the run as non-submission-valid.
- The replay contains 249 sticky conversations and 2,597 requests after excluding one target-incompatible trace tree. The server has a 131,072-token model limit, so the **120K cap applies to `input tokens + requested output tokens`** for every raw request. The remaining 11,072 tokens leave room for chat-template and tokenization overhead, avoiding an over-limit request during replay. It is an eligibility filter, not truncation: if any request in a trace tree exceeds the cap, that whole tree is excluded. The largest retained request is 119,457 tokens by that sum.
- Each cell deploys four replicas with TP=2, GPU-resident prefix caching, no CPU KV offload, and the same trace order. Starting each cell from a clean deployment prevents one router–concurrency measurement from inheriting another’s cache.
- AIPerf uses traffic seed `42`; vLLM's default engine seed is `0`, and Ray pins the Python hash seed for KV-aware deployments. GPU scheduling remains non-deterministic, so exact latency and throughput values can differ between runs. The overall router-comparison trend should be similar.

The distribution figure shows request lengths, session lifetime work, and turns per sticky conversation. The router figure shows p90 TTFT, p90 TPOT, output throughput, active decode-block load CV, and the server-reported cached-prompt-token fraction.

## Router policies

- **`session-affinity` / ConsistentHashRouter:** 100 virtual nodes and zero fallback replicas. Every turn with the same reconstructed conversation ID goes to one replica.
- **`kv-token-aware-balanced` / KVAwareRouter:** overlap credit `c=0.5`, overlap-credit decay `γ=1.0`, and active-request weight `w=32`.
- **`kv-token-aware-kv-biased` / KVAwareRouter:** overlap credit `c=2.0`, no overlap decay (`γ=0`), and active-request weight `w=0`.

KVAwareRouter chooses the eligible replica with the lowest score:

```text
score = prefill scale × max(0, active prefill blocks + request blocks − cache credit)
      + projected active decode blocks
      + active-request weight × active requests
```

The prefill scale is `1.0` for both variants. Cache credit is proportional to matching GPU KV blocks (`c`). When `γ > 0`, Dynamo reduces that credit for a candidate whose active prefill work exceeds the least prefill-loaded candidate. Thus, the balanced variant accounts for active prefill work both directly and by discounting cache credit; the cache-biased variant (`γ=0`) preserves its full cache credit. Host and disk cache offload are disabled in this benchmark.

The balanced variant gives cache reuse modest credit, discounts that credit on a prefill-loaded replica, and charges every active decode request. It therefore moves work when live load is uneven. The cache-biased variant doubles GPU-overlap credit and removes the active-request penalty, making it more willing to keep a cache-rich session on its current replica. RoundRobin is intentionally omitted: it is neither part of the comparison nor required to compute either figure.

## Dynamo's role

`KVAwareRouter` uses Dynamo's `dynamo.llm.SelectionService` to score and rank the Ray replicas already eligible for a request, based on KV-prefix overlap and projected token load. Dynamo does not receive or forward inference requests.

The request lifecycle, replica orchestration, KV event plane, and data plane remain Ray-native: Ray ingress handles and streams requests, Ray Serve manages engine replicas, Ray configures and consumes KV events, and Ray-managed vLLM replicas execute the model.

## Correctness checks

Before traffic, the script verifies the exact trace manifest, hash-block scope, request count, and `input + output <= 120K` cap. After every cell it verifies:

- Four TP=2 replicas and prefix caching;
- Complete successful AIPerf profiling records with server cache telemetry;
- A routing decision for every request;
- Strict co-location for the `session-affinity` variant; and
- Populated token tracking for both KVAwareRouter variants.

## Evaluation metrics

- **p90 TTFT:** per-request time from AIPerf request start to first output token; report the 90th percentile across successful profiling requests. The analysis uses `time_to_first_token`, falling back to `time_to_first_output_token` when needed.
- **p90 TPOT:** AIPerf's per-request inter-token latency; report the 90th percentile across successful profiling requests. Both percentiles use linear interpolation of sorted samples.
- **Output throughput:** sum actual output sequence lengths across successful profiling requests, divide by elapsed time from the earliest request start to the latest response completion, then divide by the deployment's eight GPUs.
- **Prefix cache hit rate:** sum the server-reported `usage_prompt_cache_read_tokens` across responses with cache telemetry, divided by summed server-reported `usage_prompt_tokens` for those same responses. `cells.csv` records telemetry coverage separately; this metric is not inferred from trace hash blocks.
- **Mean active decode-block load CV:** every 0.5 seconds, reconstruct each replica's in-flight decoding KV load by summing estimated 64-token blocks for its requests. A request contributes from measured first output through completion: `ceil((ISL + decoded_fraction × OSL) / 64)`. Compute the coefficient of variation (population standard deviation divided by mean) across all four replicas, including idle replicas as zero, then average the nonempty samples. This is an offline estimate from AIPerf timings and route logs, not a vLLM-exported gauge.

The comparison plot reports these five metrics. Lower p90 TTFT, p90 TPOT, and decode-block CV are better; higher output throughput and prefix cache hit rate are better.

## Layout

```text
data/weka_trace_selection.json  pinned source revision and selected trace IDs
scripts/           workload, deployment, validation, reduction, and plotting
reproduce.sh       required --out entry point
```
