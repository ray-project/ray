import pandas as pd
import numpy as np
import asyncio

from vllm import LLM
import vllm

from transformers import AutoTokenizer

class Tokenizer:
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)

    def __call__(self, batch: pd.DataFrame) -> dict:
        prompts = batch['prompt'].tolist()

        tokenized = self.tokenizer(prompts)
        return {'prompt': prompts, 'input_ids': tokenized['input_ids']}

class vLLMSyncWrapper:
    def __init__(
        self,
        model_path: str,
        mode: str = "classify",
        output_column: str = "probs",
        max_decode_tokens: int = 100,
        ignore_eos: bool = False,
        std_dev: float = 1.0,
        uniform: bool = False,
        skewed: bool = False,
    ):
        self.model_path = model_path
        self.mode = mode
        self.output_column = output_column
        self.max_decode_tokens = max_decode_tokens
        self.ignore_eos = ignore_eos
        self.std_dev = std_dev
        self.uniform = uniform
        self.skewed = skewed
        
        # Initialize LLM with appropriate task
        task = "classify" if mode == "classify" else None
        self.llm = LLM(
            model=self.model_path,
            enforce_eager=True,
            max_model_len=512,
            task=task,
        )

    def __call__(self, batch: pd.DataFrame) -> dict:
        input_ids = batch['input_ids'].tolist()
        
        prompts = [
            vllm.inputs.data.TokensPrompt(
                prompt_token_ids=token_id_list.tolist(),
            ) for token_id_list in input_ids
        ]

        if self.mode == "classify":
            result = self.llm.classify(
                prompts=prompts,
                pooling_params=vllm.PoolingParams(
                    truncate_prompt_tokens=-1,
                    task="classify",
                ),
            )
            output = {
                self.output_column: [out.outputs.probs for out in result]
            }
        elif self.mode == "generate":
            if self.skewed:
                # Skewed mode: exactly 5 requests get 1000, rest get 10
                num_prompts = len(prompts)
                # Select exactly 5 random indices to get 1000
                indices_1000 = np.random.choice(num_prompts, size=min(5, num_prompts), replace=False)
                sampled_max_tokens = [10] * num_prompts
                for idx in indices_1000:
                    sampled_max_tokens[idx] = 1000
            elif self.uniform:
                # Uniform sampling from (1, 1000)
                sampled_max_tokens = [
                    int(np.random.randint(1, 1001)) for _ in prompts
                ]
            else:
                # Normal distribution sampling
                sampled_max_tokens = [
                    int(np.clip(
                        int(np.random.normal(
                            loc=self.max_decode_tokens,
                            scale=self.std_dev
                        )),
                        1,
                        2000
                    )) for _ in prompts
                ]

            sampling_params_list = [
                vllm.SamplingParams(
                    max_tokens=max_tokens,
                    ignore_eos=self.ignore_eos,
                    temperature=1.0,
                    top_p=1.0,
                ) for max_tokens in sampled_max_tokens
            ]
            
            result = self.llm.generate(
                prompts=prompts,
                sampling_params=sampling_params_list,
            )
            output = {
                self.output_column: [out.outputs[0].text for out in result]
            }
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
        
        return output


class vLLMAsyncWrapper:
    def __init__(
        self,
        model_path: str,
        mode: str = "classify",
        output_column: str = "probs",
    ):
        self.model_path = model_path
        self.mode = mode
        self.output_column = output_column
        self.request_id = 0
        
        # Initialize AsyncLLMEngine with appropriate task
        engine_args = vllm.AsyncEngineArgs(
            model=self.model_path,
            enforce_eager=True,
            max_model_len=512,
            task="classify" if mode == "classify" else None,
        )
        self.engine = vllm.AsyncLLMEngine.from_engine_args(engine_args)

    async def __call__(self, batch: pd.DataFrame) -> dict:
        input_ids = batch['input_ids'].tolist()
        
        # Process all requests concurrently
        tasks = []
        for token_id_list in input_ids:
            prompt = vllm.inputs.data.TokensPrompt(
                prompt_token_ids=token_id_list.tolist(),
            )
            
            pooling_params = vllm.PoolingParams(
                truncate_prompt_tokens=-1,
                task="classify",
            )
            
            request_id = str(self.request_id)
            self.request_id += 1
            
            # Create async task for each request
            task = self._process_single_request(request_id, prompt, pooling_params)
            tasks.append(task)
        
        # Wait for all requests to complete
        results = await asyncio.gather(*tasks)
        
        output = {
            self.output_column: [result for result in results]
        }
        return output
    
    async def _process_single_request(
        self,
        request_id: str,
        prompt: vllm.inputs.data.TokensPrompt,
        pooling_params: vllm.PoolingParams,
    ):
        """Process a single classification request asynchronously."""
        stream = self.engine.encode(
            request_id=request_id,
            prompt=prompt,
            pooling_params=pooling_params,
            truncate_prompt_tokens=pooling_params.truncate_prompt_tokens,
        )
        
        # Consume the stream until the request is finished
        async for request_output in stream:
            if request_output.finished:
                return request_output
        
        raise RuntimeError(
            "[vLLM] The request is not finished. This should not happen."
        )