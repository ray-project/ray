# LLM dependency patches

| Patch | Purpose | Upstream PR |
| --- | --- | --- |
| [vllm-device-aware-compile-cache.patch](vllm-device-aware-compile-cache.patch) | Separate compile caches by device. | [vLLM #38962](https://github.com/vllm-project/vllm/pull/38962); reverted by [#53304](https://github.com/vllm-project/vllm/pull/53304), still needed in vLLM master. |
| [vllm-preserve-model-eos-tokens.patch](vllm-preserve-model-eos-tokens.patch) | Preserve model EOS IDs in xgrammar structured output. | [vLLM #52153](https://github.com/vllm-project/vllm/pull/52153) (open). |
| [vllm-stream-token-counts.patch](vllm-stream-token-counts.patch) | Each streamed completion and chat choice carries `num_tokens`, the tokens since its previous chunk, which HAProxy reads for Ray Serve LLM streaming metrics. | [jeffreywang88/vllm `response-token-count`](https://github.com/jeffreywang88/vllm/tree/response-token-count) (not yet a PR). |
