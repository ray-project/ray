import ray
import torch

from settings import (
    GROUP_SIZE,
    MAX_CONTEXT_LENGTH,
    MAX_GENERATION_TOKENS,
    MAX_NUM_BATCHED_TOKENS,
    MAX_NUM_SEQS,
    MODEL_ID,
    RAY_NAMESPACE,
    REGISTRY_NAME,
    TEMPERATURE,
    WORLD_SIZE_PER_MODEL,
)

from vllm import LLM, SamplingParams


@ray.remote(num_gpus=WORLD_SIZE_PER_MODEL)
class GeneratorCore:
    def __init__(self) -> None:
        self.llm = LLM(
            model=MODEL_ID,
            download_dir="/mnt/cluster_storage/ricardo/weights/",
            dtype="float16",
            enforce_eager=True,  # Reduce start time.
            max_num_batched_tokens=MAX_NUM_BATCHED_TOKENS,
            worker_extension_cls="worker_wrap.WorkerWrap",
            max_model_len=MAX_CONTEXT_LENGTH,
            max_num_seqs=MAX_NUM_SEQS,
            tensor_parallel_size=WORLD_SIZE_PER_MODEL,  # Total number of GPUs to parallelize the model over.
            gpu_memory_utilization=0.9,
        )
        self._sampling_params = SamplingParams(
            n=GROUP_SIZE,
            max_tokens=MAX_GENERATION_TOKENS,
            temperature=TEMPERATURE,
            detokenize=True,
            logprobs=1,  # TODO double check that we want this
        )

    def _get_engine(self):
        """Return the underlying engine for RPC calls."""
        return self.llm.engine if hasattr(self.llm, "engine") else self.llm

    def vllm_generate_many(self, prompt: str) -> list[tuple[str, float]]:
        """Generate GROUP_SIZE responses for a single prompt."""
        outputs = self.llm.generate([prompt], sampling_params=self._sampling_params)
        outs: list[tuple[str, float]] = []
        for out in outputs:
            for o in out.outputs:
                response = o.text
                logprobs = o.logprobs  # List of dicts, length [RESPONSE_LEN]
                # Sum log probabilities across all tokens in the response
                lp_sum = 0.0
                for token_id, token_lp_dict in zip(o.token_ids, logprobs):
                    chosen = token_lp_dict[token_id]
                    lp_sum += chosen.logprob
                outs.append((response, lp_sum))

        return outs

    def update_weights(self) -> None:
        engine = self._get_engine()

        # Reset the prefix cache to avoid stale system prompt effects.
        engine.reset_prefix_cache()

        # Note: collective_rpc can't send ray.ObjectRefs due to serialization issues https://gist.github.com/crypdick/8bd703085f5c8f8b2f4d2def58bac516
        # Note: collective_rpc can only be called from inside a Ray actor.
        engine.collective_rpc("update_weights")


class Generator:
    """Facade to coordinate vLLM generation and weight syncing."""

    def __init__(self, scorer) -> None:
        super().__init__()
        self.scorer = scorer
        self.policy_version = 1

        self.generator_core = GeneratorCore.remote()

    def generate(self, prompts_and_golds: list[tuple[str, str]]) -> None:
        """Generate responses for a batch and send them with metadata to the scorer.

        Args:
            prompts_and_golds: List of (prompt, gold_answer) tuples, length BATCH_SIZE
        """
        for prompt, gold in prompts_and_golds:  # Iterate BATCH_SIZE times
            additional_instructions = " Only respond with the answer as a plain integer with no whitespace or punctuation."
            prompt = prompt + additional_instructions
            triples = ray.get(self.generator_core.vllm_generate_many.remote(prompt))
            responses, old_logps = zip(*triples)  # Each has length GROUP_SIZE

            group = {
                "policy_version": self.policy_version,
                "prompt": prompt,
                "gold_answer": gold,
                "responses": responses,
                "old_logps": old_logps,
            }
            self.scorer.score_group.remote(group)

    def update_weights(self) -> None:
        """Load CUDA tensors pushed from the learner via NIXL transport."""

        ray.get(self.generator_core.update_weights.remote())

        # Reset the registry.
        registry_handle = ray.get_actor(REGISTRY_NAME, namespace=RAY_NAMESPACE)
        ray.get(registry_handle.reset.remote())

        print("[Generator] Updated weights on all vLLM workers", flush=True)
        torch.cuda.synchronize()
        self.policy_version += 1
