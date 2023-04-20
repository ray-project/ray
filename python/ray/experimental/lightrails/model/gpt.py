#!/usr/bin/env python3

import copy
import functools
import threading
import time
from collections import defaultdict, deque

import ray
import torch
from accelerate import dispatch_model
from ray.experimental.lightrails.coordinator import Coordinator
from ray.experimental.lightrails.physical_plan import (
    ModuleParition,
    SimplePhysicalPlanner,
    _get_device_name,
)
from ray.experimental.lightrails.schedule import (
    ExecuteSchedule,
    InputSchedule,
    OutputSchedule,
)
from ray.experimental.lightrails.util import BlockBatchLoader
from ray.util.placement_group import placement_group
from torch import nn
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    GPTNeoForCausalLM,
)
from transformers.generation.logits_process import (
    LogitNormalization,
    LogitsProcessorList,
)


# subprocess.check_call("pip install -U accelerate 'numpy<1.24' transformers", shell=True)
"""
Notes:
* layer_past is an optimization for decoder-only transformer models
    only the query token is updated each block, so can share previous k,v to optimize
    for pipline parallel probably should disable this, not sure if it's worth communication cost.
* Need to unravel this loop
    https://github.com/huggingface/transformers/blob/v4.27.2/src/transformers/models/gpt_neo/modeling_gpt_neo.py#L600
    I think the first and last ones are easy; just replace self.h with a sublist.
    the intermediate ones need something else.. I think for many transformer models since it's literally just
    sequential blocks, we can get away with re-implementing the block call ourselves..
"""

# taken from set_module_tensor_to_device, but works for name pointing to element of ModuleList
def resolve_module(module, name):
    for split in name.split("."):
        module = getattr(module, split)
        assert module is not None, f"{name} error on {split}"
    return module


def extract_blocks(h, block_indices, verbose=False):
    new_h = torch.nn.ModuleList()
    unused_h = torch.nn.ModuleList()

    for i, block in enumerate(h):
        if verbose:
            print(i, block_indices)
        if i in block_indices:
            new_h.append(block)
        else:
            unused_h.append(block)

    return new_h, unused_h


def remove_blocks_from_model(transformer, _lightrails_blocks):
    old_h = transformer.h
    transformer.h, unused_h = extract_blocks(transformer.h, _lightrails_blocks)
    # TODO delete unused unused_h


def custom_forward_pass_first_pp(self, *args, **kwargs):
    output_attentions = False
    output_hidden_states = False
    use_cache = False
    return_dict = False
    inputs_embeds = None
    attention_mask = None
    head_mask = None

    self = self.transformer

    input_ids = kwargs.pop("input_ids")
    input_shape = input_ids.size()
    input_ids = input_ids.view(-1, input_shape[-1])
    batch_size = input_ids.shape[0]
    device = input_ids.device

    past_length = 0
    past_key_values = tuple([None] * len(self.h))

    position_ids = torch.arange(
        past_length, input_shape[-1] + past_length, dtype=torch.long, device=device
    )
    position_ids = position_ids.unsqueeze(0).view(-1, input_shape[-1])

    head_mask = self.get_head_mask(head_mask, self.config.num_layers)

    if inputs_embeds is None:
        inputs_embeds = self.wte(input_ids)
    position_embeds = self.wpe(position_ids)
    hidden_states = inputs_embeds + position_embeds

    # TODO(cade) not sure what this does
    hidden_states = self.drop(hidden_states)

    # Not used in model_start, but need it later.
    output_shape = input_shape + (hidden_states.size(-1),)

    presents = () if use_cache else None
    all_self_attentions = () if output_attentions else None
    all_hidden_states = () if output_hidden_states else None
    for i, (block, layer_past) in enumerate(zip(self.h, past_key_values)):
        outputs = block(
            hidden_states,
            layer_past=layer_past,
            attention_mask=attention_mask,
            head_mask=head_mask[i],
            use_cache=use_cache,
            output_attentions=output_attentions,
        )

        hidden_states = outputs[0]
    return hidden_states, output_shape


def custom_forward_pass_last_pp(self, *args, **kwargs):
    output_attentions = False
    output_hidden_states = False
    use_cache = False
    return_dict = False
    inputs_embeds = None
    attention_mask = None
    head_mask = None

    hidden_states = kwargs.pop("hidden_states")
    output_shape = kwargs.pop("output_shape")

    # hack, didn't realize the class was GPTNeoForCausalLM
    lm_head = self.lm_head
    self = self.transformer

    # _lightrails_blocks = kwargs.pop("_lightrails_blocks")
    # old_h = self.h
    # self.h, unused_h = extract_blocks(old_h, _lightrails_blocks, verbose=True)
    ## TODO delete unused unused_h
    ## TODO cache model modification
    # print(f'custom_forward_pass_last_pp blocks: {len(self.h)}')

    past_length = 0
    past_key_values = tuple([None] * len(self.h))

    head_mask = self.get_head_mask(head_mask, self.config.num_layers)

    for i, (block, layer_past) in enumerate(zip(self.h, past_key_values)):
        outputs = block(
            hidden_states,
            layer_past=layer_past,
            attention_mask=attention_mask,
            head_mask=head_mask[i],
            use_cache=use_cache,
            output_attentions=output_attentions,
        )

        hidden_states = outputs[0]

    hidden_states = self.ln_f(hidden_states)

    hidden_states = hidden_states.view(output_shape)
    lm_head_output = lm_head(hidden_states)
    return lm_head_output


def custom_forward_pass_intermediate_pp(self, *args, **kwargs):
    output_attentions = False
    output_hidden_states = False
    use_cache = False
    return_dict = False
    inputs_embeds = None
    attention_mask = None
    head_mask = None

    hidden_states = kwargs.pop("hidden_states")

    # hack, didn't realize the class was GPTNeoForCausalLM
    self = self.transformer

    past_length = 0
    past_key_values = tuple([None] * len(self.h))

    head_mask = self.get_head_mask(head_mask, self.config.num_layers)

    for i, (block, layer_past) in enumerate(zip(self.h, past_key_values)):
        outputs = block(
            hidden_states,
            layer_past=layer_past,
            attention_mask=attention_mask,
            head_mask=head_mask[i],
            use_cache=use_cache,
            output_attentions=output_attentions,
        )

        hidden_states = outputs[0]

    return hidden_states


def create_custom_forward_pass(forward_pass_type, block_only_partition):
    if forward_pass_type == "first":
        return functools.partialmethod(
            custom_forward_pass_first_pp, _lightrails_blocks=block_only_partition
        )
    if forward_pass_type == "last":
        return functools.partialmethod(
            custom_forward_pass_last_pp, _lightrails_blocks=block_only_partition
        )
    if forward_pass_type == "intermediate":
        return functools.partialmethod(
            custom_forward_pass_intermediate_pp, _lightrails_blocks=block_only_partition
        )

    assert False
    # return [custom_forward_pass_first_pp, custom_forward_pass_last_pp][pp_rank]


def modify_model_and_replace_forward_pass(
    gpt_neo_model, num_blocks, pp_rank, partitions
):
    assert type(gpt_neo_model) == GPTNeoForCausalLM

    partition = partitions[pp_rank]
    block_only_partition = [
        block for block in partition if block >= 0 and block < num_blocks
    ]
    # print("partition", partition)
    if -1 in partition:
        forward_pass_type = "first"
    elif num_blocks in partition:
        forward_pass_type = "last"
    else:
        forward_pass_type = "intermediate"

    # print(
    #     f"pp_rank {pp_rank} is type {forward_pass_type}, with blocks {block_only_partition}"
    # )

    remove_blocks_from_model(gpt_neo_model.transformer, block_only_partition)
    custom_fwd_pass = create_custom_forward_pass(
        forward_pass_type,
        block_only_partition,
    )

    gpt_neo_model.forward = custom_fwd_pass.__get__(
        gpt_neo_model,
        gpt_neo_model.__class__,
    )


def get_tokenizer():
    checkpoint = "EleutherAI/gpt-neo-125m"
    tokenizer = AutoTokenizer.from_pretrained(checkpoint, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"
    return tokenizer


def build_model(
    num_pp_rank=12,
):
    checkpoint = "EleutherAI/gpt-neo-125m"
    config = AutoConfig.from_pretrained(checkpoint)

    tokenizer = AutoTokenizer.from_pretrained(checkpoint, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.padding_side = "left"

    device_map = {
        "transformer": _get_device_name(),
        "lm_head": _get_device_name(),
    }

    model = AutoModelForCausalLM.from_config(config)
    model = dispatch_model(model, device_map=device_map)

    blocks = resolve_module(model, "transformer.h")
    num_blocks = len(blocks)

    def partition(num_blocks=num_blocks, num_pp_rank=num_pp_rank):
        pp_rank_to_block = defaultdict(list)
        next_pp_rank = 0
        for block in range(num_blocks):
            pp_rank_to_block[next_pp_rank].append(block)

            if len(pp_rank_to_block[next_pp_rank]) >= (num_blocks // num_pp_rank):
                next_pp_rank += 1

        pp_rank_to_block[0].append(-1)
        pp_rank_to_block[num_pp_rank - 1].append(num_blocks)

        # Improve readability
        for _, blocks in pp_rank_to_block.items():
            blocks.sort()

        return pp_rank_to_block

    pp_rank_to_block = partition()
    print(pp_rank_to_block)

    pp_ranks = [model]
    while len(pp_ranks) < num_pp_rank:
        cpy = copy.deepcopy(model)
        pp_ranks.append(cpy)

    out = []
    for pp_rank, model in enumerate(pp_ranks):
        modify_model_and_replace_forward_pass(
            model, num_blocks, pp_rank=pp_rank, partitions=pp_rank_to_block
        )
        out.append(model)
    pp_ranks = out
    return pp_ranks, tokenizer


class FirstStageModelWrapper(torch.nn.Module):
    def __init__(self, first_stage_model, last_stage_model, tokenizer):
        super(FirstStageModelWrapper, self).__init__()
        self.first_stage_model = first_stage_model
        self.last_stage_model = last_stage_model
        self.processors = LogitsProcessorList()
        self.processors.append(LogitNormalization())
        self.tokenizer = tokenizer

    def forward_input_str(self, input_sequence):
        input_tokens = self.tokenizer(
            input_sequence,
            return_tensors="pt",
            truncation=True,
            padding="max_length",  # or "max_length"
            max_length=128,
        )
        input_ids = input_tokens["input_ids"].to(self.first_stage_model.device)
        return self.forward_input_ids(input_ids)

    def forward_input_ids(self, input_ids):
        self.input_ids = input_ids
        hidden_states, _ = self.first_stage_model.forward(
            input_ids=input_ids,
            return_dict=False,
            output_attentions=False,
            output_hidden_states=False,
        )
        return [hidden_states, input_ids]

    def forward(self, input):
        print(input.shape)
        if isinstance(input, str):
            return self.forward_input_str(input)
        else:
            return self.forward_input_ids(input)


class ModelWrapper(torch.nn.Module):
    def __init__(self, model):
        super(ModelWrapper, self).__init__()
        self.model = model

    def forward(self, tensors):
        hidden_states = tensors[0]
        input_ids = tensors[1]
        # print(f"forwarding {hidden_states.shape}")
        return [
            self.model.forward(
                hidden_states=hidden_states, output_shape=hidden_states.shape
            ),
            input_ids,
        ]


class LastStageModelWrapper(torch.nn.Module):
    def __init__(self, model, tokenizer):
        super(LastStageModelWrapper, self).__init__()
        self.model = model
        self.tokenizer = tokenizer
        self.processors = LogitsProcessorList()
        self.processors.append(LogitNormalization())

    def forward(self, tensors):
        hidden_states = tensors[0]
        input_ids = tensors[1]
        # print(f"forwarding {hidden_states.shape}")
        lm_logits = self.model.forward(
            hidden_states=hidden_states, output_shape=hidden_states.shape
        )

        next_token_logits = lm_logits[:, -1, :]
        next_token_scores = self.processors(input_ids, next_token_logits)
        probs = nn.functional.softmax(next_token_scores, dim=-1)
        next_tokens = torch.multinomial(probs, num_samples=1).squeeze(1)
        input_ids = torch.cat([input_ids[:, 1:], next_tokens[:, None]], dim=-1)
        decoded = self.tokenizer.decode(next_tokens)
        return input_ids, decoded


def build_pipeleine_stage_model(stage, pp_ranks, tokenizer):
    if stage == 0:
        return FirstStageModelWrapper(pp_ranks[stage], pp_ranks[-1], tokenizer)
    if stage == len(pp_ranks) - 1:
        return LastStageModelWrapper(pp_ranks[stage], tokenizer)
    return ModelWrapper(pp_ranks[stage])


def load_module(i, num_stages):
    pp_ranks, tokenizer = build_model(num_pp_rank=num_stages)
    return build_pipeleine_stage_model(i, pp_ranks, tokenizer)


def gen_logical_plan(
    batch_size=1, num_stages=12, context_length=128, embedding_size=768
):
    logical_plan = []

    for i in range(num_stages):
        if i == 0:
            schedule = InputSchedule(downstream_rank=1)
        elif i == num_stages - 1:
            schedule = OutputSchedule(
                upstream_rank=i - 1,
            )
        else:
            schedule = ExecuteSchedule(
                upstream_rank=i - 1,
                downstream_rank=(i + 1) % num_stages,
            )
        data_loader_builder = None

        if i == 0 or i == num_stages - 1:
            data_loader_builder = lambda: BlockBatchLoader()
        partion = ModuleParition(
            partition_index=i,
            module_loader=lambda i=i: load_module(i, num_stages),
            input_tensor_shape=[
                (batch_size, context_length, embedding_size),
                (batch_size, context_length),
            ],
            input_tensor_dtype=torch.float32,
            schedule=schedule,
            data_loader_builder=data_loader_builder,
        )
        logical_plan.append(partion)
    return logical_plan


EOS = 50256


def create_pipelined_model(requires_gpu=False, batch_size=10):
    num_stages = 2
    context_length = 128
    physical_planner = SimplePhysicalPlanner()
    pg = placement_group([{"CPU": 1}] * num_stages, strategy="PACK")
    if requires_gpu:
        pg = placement_group([{"CPU": 1, "GPU": 1}] * num_stages, strategy="PACK")
    logical_plan = gen_logical_plan(
        num_stages=num_stages, context_length=context_length, batch_size=batch_size
    )

    coordinator = Coordinator(
        logical_plan=logical_plan,
        pg=pg,
        planner=physical_planner,
        requires_gpu=requires_gpu,
    )
    return coordinator


class Inferencer(object):
    def __init__(self, requires_gpu=False, batch_size=10) -> None:
        self._coordinator = create_pipelined_model(
            requires_gpu=requires_gpu, batch_size=batch_size
        )
        self._coordinator.start()
        self._first_stage = self._coordinator.first_stage()
        self._last_stage = self._coordinator.last_stage()
        self._tokenizer = get_tokenizer()

        self._counter = 0
        self._queue = deque()
        self._batch_size = batch_size
        self.pend_user_request = {}
        self.pend_engine_request = {}
        self.finished_requests = {}
        self.lock = threading.Lock()

    def generate(self, input):
        input_tokens = self._tokenizer(
            input,
            return_tensors="pt",
            truncation=True,
            padding="max_length",  # or "max_length"
            max_length=128,
        )
        input_ids = input_tokens["input_ids"]
        return self._enqueue(input_ids)

    def _get_request_id(self):
        self._counter += 1
        return self._counter

    def _enqueue(self, input_ids):
        with self.lock:
            request_id = self._get_request_id()
            self._queue.append((request_id, input_ids))
            return request_id

    def start(self):
        self.run_send()
        self.run_pull()

    def get_result(self, request_id, block=False):
        if not block:
            with self.lock:
                return self.finished_requests.get(request_id, None)

        while True:
            with self.lock:
                if request_id in self.finished_requests:
                    return self.finished_requests[request_id]
            time.sleep(0.01)

    def run_send(self):
        self.run_thread = threading.Thread(target=self._send_batches)
        self.run_thread.start()

    def _send_batches(self):
        sleep = False
        while True:
            batch = []
            request_ids = []
            if sleep:
                time.sleep(0.01)

            with self.lock:
                if len(self._queue) < self._batch_size:
                    sleep = True
                    continue
                sleep = False
                for _ in range(self._batch_size):
                    request_id, input_ids = self._queue.popleft()
                    batch.append(input_ids)
                    request_ids.append(request_id)

            engine_id = ray.get(
                self._first_stage.push_batch.remote(torch.cat(batch, dim=0))
            )

            with self.lock:
                for request_id in request_ids:
                    self.pend_user_request[request_id] = engine_id
                    self.pend_engine_request[engine_id] = request_ids

    def run_pull(self):
        self.pull_thread = threading.Thread(target=self._pull_results)
        self.pull_thread.start()

    def _pull_results(self):
        while True:
            engine_id, output, _ = ray.get(self._last_stage.pop_batch.remote())

            with self.lock:
                for i in range(self._batch_size):
                    output_ids = output[0][i].view(1, -1).int()
                    request_id = self.pend_engine_request[engine_id][i]
                    # finished
                    if output_ids[0][-1] == EOS or output_ids[0][0] != EOS:
                        output = self._tokenizer.decode(output_ids[0])
                        self.finished_requests[request_id] = output
                    else:
                        self._queue.append((request_id, output_ids))

                    del self.pend_user_request[request_id]
                del self.pend_engine_request[engine_id]
