# Examples

End-to-end tutorials for deploying LLMs with Ray Serve. Each one walks through configuration, deployment, and querying for a representative model. For the minimal path, start with the {doc}`Quickstart <quick-start>`.

## By model size

- {doc}`Deploy a small-sized LLM </_collections/serve/tutorials/deployment-serve-llm/small-size-llm/README>`: serve a model that fits on a single GPU. The best starting point.
- {doc}`Deploy a medium-sized LLM </_collections/serve/tutorials/deployment-serve-llm/medium-size-llm/README>`: shard a model across multiple GPUs on one node with tensor parallelism.
- {doc}`Deploy a large-sized LLM </_collections/serve/tutorials/deployment-serve-llm/large-size-llm/README>`: span a model across multiple nodes with cross-node parallelism.

## By capability

- {doc}`Deploy a vision LLM </_collections/serve/tutorials/deployment-serve-llm/vision-llm/README>`: serve a vision-language model that accepts image inputs.
- {doc}`Deploy a reasoning LLM </_collections/serve/tutorials/deployment-serve-llm/reasoning-llm/README>`: serve a reasoning model and handle its reasoning output.
- {doc}`Deploy a hybrid reasoning LLM </_collections/serve/tutorials/deployment-serve-llm/hybrid-reasoning-llm/README>`: serve a model that can switch reasoning on and off per request.
- {doc}`Deploy gpt-oss </_collections/serve/tutorials/deployment-serve-llm/gpt-oss/README>`: deploy OpenAI's open-weight gpt-oss model.

```{toctree}
:hidden:

/_collections/serve/tutorials/deployment-serve-llm/small-size-llm/README
/_collections/serve/tutorials/deployment-serve-llm/medium-size-llm/README
/_collections/serve/tutorials/deployment-serve-llm/large-size-llm/README
/_collections/serve/tutorials/deployment-serve-llm/vision-llm/README
/_collections/serve/tutorials/deployment-serve-llm/reasoning-llm/README
/_collections/serve/tutorials/deployment-serve-llm/hybrid-reasoning-llm/README
/_collections/serve/tutorials/deployment-serve-llm/gpt-oss/README
```
