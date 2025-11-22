from typing import Optional

import ray

from settings import WORLD_SIZE_PER_MODEL

import copy
import os
import socket
import time
from typing import Any

import torch
import torch.nn as nn
import torch.distributed as dist
from torch.distributed.checkpoint.state_dict import get_state_dict, StateDictOptions

from env_utils import apply_fsdp2, check_cuda_availability, FSDPState
from transformers import AutoModelForCausalLM, AutoTokenizer
from settings import (
    BATCH_SIZE,
    GRAD_CLIP_NORM,
    GROUP_SIZE,
    GRPO_CLIP_EPS,
    MODEL_ID,
    LEARNING_RATE,
    RAY_NAMESPACE,
    REGISTRY_NAME,
    WEIGHT_DECAY,
)

TO_BE_IGNORED = -100


@ray.remote(num_gpus=1)
class LearnerWorker:
    def __init__(self, replay_buffer) -> None:
        super().__init__()

        self.replay_buffer = replay_buffer
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)

        # Use the end of sequence token as the padding token.
        self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
        self.model = (
            AutoModelForCausalLM.from_pretrained(
                MODEL_ID,
                torch_dtype=torch.float16,
            )
            .to("cuda")
            .train()
        )

        # FSDP state management
        self._fsdp_state: Optional[FSDPState] = None
        self._fsdp_rank: int = 0
        self._distributed_initialized: bool = False
        check_cuda_availability()

        # Optimizer will be created after FSDP wrapping in initialize_distributed
        self.optim: Optional[torch.optim.Optimizer] = None

        self.global_batch_count = 0

    def initialize_distributed(
        self,
        rank: int,
        master_addr: Optional[str],
        master_port: Optional[int],
    ) -> bool:
        """Initialize torch.distributed for FSDP and wrap the model."""
        if self._distributed_initialized:
            return True

        check_cuda_availability()
        self._fsdp_rank = int(rank)

        if master_addr is None:
            master_addr = os.environ.get("MASTER_ADDR", "127.0.0.1")
        if master_port is None:
            master_port = int(os.environ.get("MASTER_PORT", "29500"))

        os.environ["MASTER_ADDR"] = str(master_addr)
        os.environ["MASTER_PORT"] = str(master_port)
        os.environ["RANK"] = str(self._fsdp_rank)
        os.environ["WORLD_SIZE"] = str(WORLD_SIZE_PER_MODEL)

        if "LOCAL_RANK" not in os.environ:
            local_gpu_ids = ray.get_gpu_ids()
            local_rank = int(local_gpu_ids[0]) if local_gpu_ids else 0
            os.environ["LOCAL_RANK"] = str(local_rank)
        torch.cuda.set_device(0)

        print(
            f"[Learner-rank{self._fsdp_rank}] Initializing process group: "
            f"master={master_addr}:{master_port}, world_size={WORLD_SIZE_PER_MODEL}",
            flush=True,
        )

        if not dist.is_initialized():
            dist.init_process_group(
                backend="nccl",
                init_method=f"tcp://{master_addr}:{master_port}",
                world_size=WORLD_SIZE_PER_MODEL,
                rank=self._fsdp_rank,
            )

        # Wrap the base model with FSDP2.
        if self._fsdp_state is None:
            self._fsdp_state = apply_fsdp2(self.model.model)
            self.model.model = self._fsdp_state.module
            self.model.model.train()

        if self.optim is None:
            self.optim = torch.optim.Adam(
                self.model.parameters(),
                lr=LEARNING_RATE,
                weight_decay=WEIGHT_DECAY,
            )

        self._distributed_initialized = True
        print(
            f"[Learner-rank{self._fsdp_rank}] Distributed init complete; "
            f"dist.is_initialized={dist.is_initialized()}",
            flush=True,
        )
        return True

    def get_node_ip(self) -> str:
        return ray.util.get_node_ip_address()

    def choose_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("", 0))
            return int(sock.getsockname()[1])

    def forward(
        self,
        *,
        input_ids: torch.Tensor,  # [BATCH_SIZE, SEQ_LEN]
        attention_mask: torch.Tensor | None = None,  # [BATCH_SIZE, SEQ_LEN]
    ) -> torch.Tensor:
        out = self.model(
            input_ids=input_ids,
            attention_mask=attention_mask,
            use_cache=False,
        )
        return out.logits  # [BATCH_SIZE, SEQ_LEN, VOCAB_SIZE]

    def _apply_update(
        self,
        groups: list[dict[str, Any]],
    ) -> torch.Tensor:
        tokenizer = self.tokenizer
        pad_token_id = tokenizer.pad_token_id

        seq_tensors: list[
            torch.Tensor
        ] = []  # List of [SEQ_LEN_i] tensors, variable length
        label_tensors: list[
            torch.Tensor
        ] = []  # List of [SEQ_LEN_i] tensors, variable length
        advantages: list[float] = []  # List of length BATCH_SIZE * GROUP_SIZE
        generation_logp_sum: list[float] = []  # List of length BATCH_SIZE * GROUP_SIZE

        # Batch tokenize prompts and responses.
        prompt_texts: list[str] = []
        response_texts: list[str] = []
        advantages_flat: list[float] = []  # Advantage per (prompt, response) pair
        logp_sums_flat: list[float] = []  # Old log-prob sum per pair

        for group in groups:
            rewards = group["rewards"]
            baseline = sum(rewards) / len(rewards)
            # Replicate the prompt for every response in the group so that we
            # can batch-tokenize later.
            prompt_texts.extend([group["prompt"]] * len(group["responses"]))
            response_texts.extend(group["responses"])
            # Advantage is reward minus baseline, one entry per response.
            advantages_flat.extend([r - baseline for r in rewards])
            logp_sums_flat.extend(group["old_logps"])

        # Tokenize in vectorized calls.
        prompt_ids_list: list[list[int]] = tokenizer(
            prompt_texts, add_special_tokens=False
        )["input_ids"]
        response_ids_list: list[list[int]] = tokenizer(
            response_texts, add_special_tokens=False
        )["input_ids"]

        # Assemble input/label tensors one sample at a time due to variable length sequences.
        for prompt_ids, resp_ids, adv, old_lp in zip(
            prompt_ids_list, response_ids_list, advantages_flat, logp_sums_flat
        ):
            if not resp_ids:
                # Avoid zero-length label tensors to preserve the mask shape.
                resp_ids = [tokenizer.eos_token_id]

            input_ids = prompt_ids + resp_ids  # [PROMPT_LEN + RESPONSE_LEN]
            labels = [TO_BE_IGNORED] * len(prompt_ids) + resp_ids

            seq_tensors.append(torch.tensor(input_ids, dtype=torch.long))
            label_tensors.append(torch.tensor(labels, dtype=torch.long))
            advantages.append(adv)
            generation_logp_sum.append(old_lp)

        # Pad the variable-length sequences to the same length so that we can stack the tensors.
        input_ids = nn.utils.rnn.pad_sequence(
            seq_tensors, batch_first=True, padding_value=pad_token_id
        ).to("cuda")  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN]
        labels = nn.utils.rnn.pad_sequence(
            label_tensors, batch_first=True, padding_value=TO_BE_IGNORED
        ).to("cuda")  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN]
        # Do not attend to padding tokens.
        attention_mask = (
            (input_ids != pad_token_id).long().to("cuda")
        )  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN]

        expected_batch = len(seq_tensors)

        current_outputs = self.model.forward(
            input_ids=input_ids,
            attention_mask=attention_mask,
        )
        # Compute log_softmax in float32 for numerical stability.
        logp_full = torch.log_softmax(
            current_outputs.logits.float(), dim=-1
        )  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN, VOCAB_SIZE]

        # Trim logits/labels to align targets by removing Huggingface's next tokens.
        target_ids = input_ids[:, 1:]  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1]
        logp_full = logp_full[
            :, :-1, :
        ]  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1, VOCAB_SIZE]
        trim_labels = labels[:, 1:]  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1]
        token_mask = (
            trim_labels != TO_BE_IGNORED
        )  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1]
        token_mask_f = token_mask.float()  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1]

        assert target_ids.shape[0] == expected_batch, (
            f"target_ids batch mismatch: {target_ids.shape[0]} != {expected_batch}"
        )

        # Extract the log probability for each token that was actually generated.
        chosen_logp_per_token = logp_full.gather(-1, target_ids.unsqueeze(-1)).squeeze(
            -1
        )  # [BATCH_SIZE * GROUP_SIZE, MAX_SEQ_LEN - 1]

        # Sum token log probabilities over generated tokens only.
        chosen_logp_sum = (chosen_logp_per_token * token_mask_f).sum(
            dim=1
        )  # [BATCH_SIZE * GROUP_SIZE]

        advantages_tensor = torch.tensor(
            advantages, dtype=torch.float32, device="cuda"
        )  # [BATCH_SIZE * GROUP_SIZE]

        if advantages_tensor.numel() > 1:
            # Normalize advantages to keep their magnitudes consistent across training.
            advantages_tensor = (advantages_tensor - advantages_tensor.mean()) / (
                advantages_tensor.std(unbiased=False) + 1e-8
            )  # [BATCH_SIZE * GROUP_SIZE]

        generation_logp_sum = torch.tensor(
            generation_logp_sum, dtype=torch.float32, device="cuda"
        )

        # Compute GRPO loss with PPO-style importance sampling.
        ratio = torch.exp(chosen_logp_sum - generation_logp_sum)

        unclipped = ratio * advantages_tensor
        # Clip the ratio to enforce the trust region.
        clipped = (
            torch.clamp(ratio, 1 - GRPO_CLIP_EPS, 1 + GRPO_CLIP_EPS) * advantages_tensor
        )
        grpo_loss = -torch.min(unclipped, clipped).mean()

        # Apply GRPO update.
        self.optim.zero_grad()
        grpo_loss.backward()

        clipped_norm = nn.utils.clip_grad_norm_(self.model.parameters(), GRAD_CLIP_NORM)

        # Skip optimizer step if gradients are effectively zero to avoid NaN weights.
        # This can happen when all advantages are zero. This can happen if none of the answers are correct.
        if clipped_norm > 1e-8:
            self.optim.step()
        else:
            print(
                "[WARNING] Skipping optimizer step due to zero gradients - all samples likely have same reward",
                flush=True,
            )

        return {
            "loss": grpo_loss.detach().item(),
        }

    def step(self) -> dict[str, Any]:
        groups_cpu: list[dict[str, Any]] = ray.get(
            self.replay_buffer.sample_groups.remote(GROUP_SIZE)
        )
        while len(groups_cpu) < GROUP_SIZE:
            print(
                f"[Learner] Not enough groups to sample batch size {GROUP_SIZE} from replay buffer. Sleeping for 0.05 seconds."
            )
            time.sleep(0.05)
            groups_cpu = ray.get(self.replay_buffer.sample_groups.remote(BATCH_SIZE))
        results = self._apply_update(groups_cpu)
        results["batch_num"] = self.global_batch_count
        self.global_batch_count += 1
        return results

    def get_gpu_uuid(self) -> str:
        device = torch.cuda.current_device()
        return str(torch.cuda.get_device_properties(device).uuid)

    @ray.method(tensor_transport="nixl")
    def get_weights(self):
        """Extract model weights from FSDP and prepare GPU-to-GPU transfer.

        Note: This is a collective call; only rank 0 sends weights to the registry.
        """
        if self._fsdp_state is not None:
            # Gather a full state dict across ranks.
            state_dict, _optimizer_state_dict = get_state_dict(
                self._fsdp_state.module,
                optimizers=(),
                options=StateDictOptions(
                    full_state_dict=True,  # Materialize DTensors as dense tensors
                    cpu_offload=False,  # Keep tensors on GPU for NIXL transfer
                ),
            )

            # Only rank 0 keeps the full state dict.
            if self._fsdp_rank != 0:
                return

            # Re-add the "model." prefix expected by vLLM.
            state_dict = {f"model.{k}": v for k, v in state_dict.items()}

            # Add unsharded lm_head weights.
            if hasattr(self.model, "lm_head") and self.model.lm_head is not None:
                state_dict["lm_head.weight"] = self.model.lm_head.weight.data
        else:
            # Non-FSDP fallback.
            state_dict = self.model.state_dict()

        # Send the state dict to the Ray object store via NIXL in chunks.
        registry_handle = ray.get_actor(REGISTRY_NAME, namespace=RAY_NAMESPACE)
        ray.get(registry_handle.reset.remote())
        for name, weight in state_dict.items():
            # FIXME: Qiaolin, remove deepcopy once the bug with sending the same weights multiple times is fixed.
            name_weight = (name, weight)
            name_weight = copy.deepcopy(name_weight)
            single_tensor_ref = ray.put(name_weight, _tensor_transport="nixl")
            # Wrap the ref to avoid eager materialization in the registry actor.
            contained_ref = [single_tensor_ref]
            ray.get(registry_handle.put.remote(contained_ref))


class Learner:
    """Driver-side facade that manages learner worker actors."""

    def __init__(self, replay_buffer) -> None:
        super().__init__()
        self._replay_buffer = replay_buffer

        # Initialize worker actor pool for FSDP sharding
        self._workers = [
            LearnerWorker.remote(self._replay_buffer)
            for _ in range(WORLD_SIZE_PER_MODEL)
        ]
        self._rank0 = self._workers[0]
        self._init_distributed()

    def _init_distributed(self) -> None:
        """Initialize FSDP training across all learner workers."""
        master_addr, master_port = ray.get(
            [self._rank0.get_node_ip.remote(), self._rank0.choose_free_port.remote()]
        )

        ray.get(
            [
                worker.initialize_distributed.remote(
                    rank,
                    master_addr,
                    master_port,
                )
                for rank, worker in enumerate(self._workers)
            ]
        )

    def step(self) -> list[ray.ObjectRef]:
        return [worker.step.remote() for worker in self._workers]

    def get_gpu_uuid(self) -> Optional[str]:
        return ray.get(self._rank0.get_gpu_uuid.remote())

    def get_weights(self):
        """Materialize FSDP weights and send them to the registry."""
        # All workers must participate in the collective get_state_dict operation.
        # Only rank 0 will send weights to the registry; other ranks will exit early.
        ray.get([worker.get_weights.remote() for worker in self._workers])
