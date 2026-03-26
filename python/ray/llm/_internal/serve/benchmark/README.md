# Multi-Turn LLM Benchmark

A benchmark tool for OpenAI-compatible LLM inference servers that supports
multi-turn conversations with configurable prefix cache hit rates, input/output
sequence lengths, and cross-session prefix sharing.

## Entry Point

```
python -m ray.llm._internal.serve.benchmark.cli [OPTIONS]
```

## Modes

| Command | Mode | Description |
|---------|------|-------------|
| `... -s` | Smoke | Single request health check |
| `... --concurrency 8 ...` | Direct (concurrency) | Closed-loop concurrency benchmark |
| `... --request-rate 10 ...` | Direct (rate) | Constant-QPS benchmark |
| `... -i` | Interactive server | Long-running server with UNIX socket control |
| `... -i --client` | Interactive client | Connect to server; REPL or `--cmd` one-shot |

## Quick Examples

### Smoke test

```bash
python -m ray.llm._internal.serve.benchmark.cli -s \
  -u http://localhost:8000 -m my-model
```

### Concurrency benchmark

```bash
python -m ray.llm._internal.serve.benchmark.cli \
  -u http://localhost:8000 -m meta-llama/Llama-3-8B-Instruct \
  --concurrency 8 --num-sessions 200 \
  --isl 2000 --osl 200 --hit-rate 0.7 --num-turns 5 \
  --think-time 1.0 --save-result results.json
```

### Rate benchmark

```bash
python -m ray.llm._internal.serve.benchmark.cli \
  -u http://localhost:8000 -m meta-llama/Llama-3-8B-Instruct \
  --request-rate 10 --duration 120 \
  --isl 2000 --osl 200 --hit-rate 0.7 --num-turns 5 \
  --warm-up 10 --save-result results.json
```

### Interactive server

```bash
python -m ray.llm._internal.serve.benchmark.cli -i \
  -u http://localhost:8000 -m meta-llama/Llama-3-8B-Instruct \
  --isl 2000 --osl 200 --hit-rate 0.7 --num-turns 5
```

### Interactive client (REPL)

```bash
python -m ray.llm._internal.serve.benchmark.cli -i --client
```

### Interactive client (one-shot)

```bash
python -m ray.llm._internal.serve.benchmark.cli -i --client --cmd "rate 10"
python -m ray.llm._internal.serve.benchmark.cli -i --client --cmd "status"
```

## Workload Parameters

All workload parameters use **simple mode**: you specify user-facing values
and the tool derives internal parameters (per-turn user tokens `u` and
system prompt tokens `s`) automatically.

| Parameter | Flag | Description |
|-----------|------|-------------|
| ISL | `--isl` | Average input sequence length (tokens) across all turns |
| OSL | `--osl` | Output tokens per turn |
| Hit rate | `--hit-rate` | Target prefix cache hit rate [0, 1] |
| Cross-sharing | `--cross-sharing` | Fraction of system prompt shared across sessions (default: 1.0) |
| Num turns | `--num-turns` | Number of turns per conversation session |
| Think time | `--think-time` | Simulated user think-time between turns in seconds (default: 0) |
| Chunk size | `--chunk-size` | Number of SSE content chunks before recording first-chunk latency (default: 16) |

The solver derives `u` (new user tokens per turn) and `s` (total system prompt
tokens) from these inputs. The `print_summary()` output shows the resolved
per-turn token breakdown including cached vs. new tokens at each turn.

## Tokenizer

By default, `--tokenizer` is `None`, which causes the tool to use the
`--model` value as the HuggingFace tokenizer name. This works when `--model`
is a valid HuggingFace model ID (e.g., `meta-llama/Llama-3-8B-Instruct`).

Provide `--tokenizer` explicitly when:
- The `--model` value is an alias or deployment name that is not a valid
  HuggingFace repo (e.g., `--model my-deployment --tokenizer meta-llama/Llama-3-8B-Instruct`).
- You want to use a local tokenizer path.

## Warm-Up Strategies

### Concurrency mode

Warm-up is **automatic** using entropy-based detection. The tool monitors the
distribution of active turns across concurrent sessions. Once the Shannon
entropy of the turn distribution reaches 50% of its theoretical maximum, the
pool is considered at steady state and measurement begins. All requests
dispatched before that point are discarded.

### Rate mode

Warm-up is **time-based** via the `--warm-up` flag (in seconds). All requests
whose dispatch time falls within the warm-up window are excluded from reported
metrics. Set this to allow the server's KV cache to fill and stabilize.

### Interactive mode

Warm-up is **manual**. The operator starts traffic with `rate <qps>`, waits
for the system to stabilize, then explicitly starts a measurement window with
`start` or `measure <n>`.

## Interactive Commands

| Command | Description |
|---------|-------------|
| `help` | Show available commands |
| `rate <qps>` | Set target request rate (0 to pause) |
| `start` | Start open-ended measurement window |
| `measure <n>` | Start measurement capturing next `n` completed requests |
| `stop` | Stop measurement and print summary |
| `status` | Show current state: QPS, inflight, completed, measured |
| `workload [k=v ...]` | Show or update workload parameters (e.g., `workload isl=3000 osl=300`) |
| `save [path]` | Save last measurement window to JSON |
| `save-dir <path>` | Set default directory for saved results |
| `quit` | Stop the benchmark server |

## JSON Result Schema

Results saved with `--save-result` (direct mode) contain these top-level keys:

| Key | Description |
|-----|-------------|
| `config` | Run configuration (concurrency/rate, model, etc.) |
| `spec` | Resolved workload spec with per-turn token breakdown |
| `chunk_size` | Chunk size used for first-chunk latency |
| `benchmark` | Run metadata: total requests, duration, warm-up info |
| `stats` | Aggregate latency statistics (avg, P50, P90, P99 for TTFT, FC, TPOT, latency) |
| `per_turn` | Per-turn breakdown of count, avg ISL, and latency percentiles |
| `raw_metrics` | Array of per-request metrics (session_id, turn, all latency fields, token counts) |

Interactive mode saves with `save` produce a similar structure with a `window`
summary instead of `benchmark`/`stats`/`per_turn`.

## Typical Workflow

1. **Smoke test** to verify connectivity:
   ```bash
   python -m ray.llm._internal.serve.benchmark.cli -s -u http://localhost:8000 -m my-model
   ```

2. **Direct benchmark** for a fixed workload:
   ```bash
   python -m ray.llm._internal.serve.benchmark.cli \
     --concurrency 8 --num-sessions 200 \
     --isl 2000 --osl 200 --hit-rate 0.7 --num-turns 5 \
     -u http://localhost:8000 -m meta-llama/Llama-3-8B-Instruct \
     --save-result concurrency_8.json
   ```

3. **Interactive mode** for exploratory testing:
   ```bash
   # Terminal 1: start server
   python -m ray.llm._internal.serve.benchmark.cli -i \
     --isl 2000 --osl 200 --hit-rate 0.7 --num-turns 5 \
     -u http://localhost:8000 -m meta-llama/Llama-3-8B-Instruct

   # Terminal 2: control
   python -m ray.llm._internal.serve.benchmark.cli -i --client
   benchctl> rate 5
   benchctl> measure 500
   benchctl> status
   benchctl> save results_qps5.json
   benchctl> rate 10
   benchctl> measure 500
   benchctl> save results_qps10.json
   benchctl> quit
   ```

4. **Sweep** over multiple configurations: write an external script that loops
   over the CLI with different parameters. The tool does not include built-in
   sweep orchestration.
