from typing import Any
import gymnasium as gym
from ray.rllib.env.env_context import EnvContext

import transformers
import datasets
import numpy as np
import torch
from ray.rllib.utils.spaces.repeated import Repeated
import gymnasium.spaces as sp


def generate_response(
    model:torch.nn.Module, 
    *, 
    input_ids: torch.tensor, 
    max_length:int, 
    eos_token_id: int
):
    """Generate a response using the model."""
    generated_sequence = []
    probs_list = []
    model_in = torch.clone(input_ids)
    with torch.no_grad():
        for i in range(max_length):
            # Get the logits using the forward() method
            logits = model(model_in).logits

            # Get the logits of the last token
            next_token_logits = logits[:, -1, :]

            # Apply a softmax function to the logits to get the probabilities
            probs = next_token_logits.softmax(dim=-1)

            # Sample the next token from the probability distribution
            next_token = torch.multinomial(probs, num_samples=1)

            # Append the probabilities and the generated token
            generated_sequence.append(next_token)
            probs_list.append(probs)

            # Update the input_ids with the generated token
            model_in = torch.cat([model_in, next_token], dim=-1)

            if next_token.item() == eos_token_id:
                break

        # Decode and print the generated sequence
        generated_tokens = torch.cat(generated_sequence, dim=-1)

    # Stack the probabilities tensor
    probs_tensor = torch.concat(probs_list, dim=0)

    return {
        "sequence": torch.cat([input_ids, generated_tokens], dim=-1),
        "probs": probs_tensor[None],
        "n_input_tokens": input_ids.shape[-1],
        "n_generated_tokens": generated_tokens.shape[-1],
    }

def compute_approx_kl(
    probs: torch.Tensor,
    probs_base: torch.Tensor,
) -> torch.Tensor:

    log_ratio = (probs / probs_base).log()
    approx_kl = probs * log_ratio
    approx_kl = approx_kl.sum(dim=-1).mean()
    return approx_kl


class RM:

    def __init__(self) -> None:
        pass

    def __call__(self, sequences, attention_mask, response_mask=None) -> Any:
        # sequences: [instruction + response]
        # response_mask: [0] * len(instruction) + [1] * len(response) 
        # response_mask tells which tokens from input are responses and which ones are 
        # instructions.
        pass

class ShortestAnswerRM(RM):

    def __init__(self, max_length: int) -> None:
        super().__init__()
        self.max_length = max_length

    def __call__(self, sequences: np.ndarray, attention_mask: np.ndarray=None,       
                 response_mask=None) -> Any:
        # We want to find the shortest answer, so we want to minimize the length of 
        # tokens of the response.
        print(response_mask)
        if response_mask is not None:
            return -np.sum(response_mask * attention_mask, -1) / self.max_length
        else:
            # If there is no response mask, we also include instruction in our count
            return -np.sum(attention_mask) / self.max_length
    

class RLHFEnv(gym.Env):
    """An env that scores the generated texts using a pre-trained reward function."""

    def __init__(self, config: EnvContext = None):
        
        # the KL coefficient
        self.kl_coeff = config["kl_coeff"]
        self.max_generation_length = config["max_generation_length"]

        # This is the base tokenizer
        self.tokenizer = transformers.AutoTokenizer.from_pretrained(config["tokenizer_path"])

        # This is the SFT model 
        self.sft_model = transformers.GPT2LMHeadModel.from_pretrained(
            config["sft_model_path"]
        )

        # This is the reward model, it's either a neural network or a callable function 
        # that takes in a batch of generated text tokens + response mask and returns a 
        # batch of rewards.
        # TODO(Kourosh): Later allow users to pass in their own reward model factory.
        self.reward_model = ShortestAnswerRM(max_length=self.max_generation_length)

        # Prompt dataset
        # We need to load the prompt dataset loader and randomly sample a prompt from 
        # it upon reset.
        # TODO (Kourosh): Assuming we only support HF datasets for now:
        self.prompt_dataset = datasets.load_dataset(
            config["prompt_dataset_path"], 
            split=config["prompt_dataset_split"]
        )
        self.dsize = len(self.prompt_dataset)


        vocab_size = self.tokenizer.vocab_size
        model_max_length = self.tokenizer.model_max_length

        self.action_space = sp.Box(
            0.0, 1.0, shape=(vocab_size,), dtype=np.float16
        )

        self.observation_space = sp.Dict({
            "input_ids": Repeated(sp.Discrete(vocab_size), max_len=model_max_length),
            "attention_mask": Repeated(sp.Discrete(2), max_len=model_max_length),
        })


    def reset(self, *, seed=None, options=None):
        if seed:
            np.random.seed(seed)
        index = np.random.randint(self.dsize)
        prompt = self.prompt_dataset[index]["prompt"]
        prompt_tokens = self.tokenizer(prompt, return_tensors="np")

        return prompt_tokens, {}

    def step(self, action):
        # action is a dict with the following keys:
        # input_ids: [b, NP+NR], prompt + generated response from the actor LLM
        # response_mask: [b, NP+NR], [0] * NP + [1] * NR To separate response from prompt
        # attention_mask: [b, NP+NR]
        # probs: [b, NR, vocab_size], the probabilities of each token in the vocab for each token in the input

        input_ids= action["input_ids"]
        response_mask = action["response_mask"]
        attention_mask = action["attention_mask"]
        probs = action["probs"]

        n_response_tokens = probs.shape[1]

        r_align = self.reward_model(input_ids, attention_mask, response_mask)
        r_align = r_align.item()

        # compute the probs from the sft model for the same number of tokens
        input_ids = torch.tensor(input_ids, dtype=torch.long)
        sft_output = generate_response(
            self.sft_model, 
            input_ids=input_ids, 
            max_length=n_response_tokens, 
            eos_token_id=self.tokenizer.eos_token_id
        )
        
        probs = torch.tensor(probs, dtype=torch.float32)
        r_kl = compute_approx_kl(probs, sft_output["probs"]).item()

        reward = r_align - self.kl_coeff * r_kl

        # Produce a random reward when we reach the goal.
        return self.observation_space.sample(), reward, True, False, {"r_align": r_align, "r_kl": r_kl, "n_response_tokens": n_response_tokens}