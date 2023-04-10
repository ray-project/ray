from typing import Any
import gymnasium as gym
from ray.rllib.env.env_context import EnvContext

import transformers
import datasets
import numpy as np
import torch
from ray.rllib.utils.spaces.repeated import Repeated
import gymnasium.spaces as sp
import tree


def generate_response(
    model: torch.nn.Module, 
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

            # if not probs_list:
            #     prev_probs = logits[:, :-1, :].softmax(dim=-1)
            #     probs_list.extend(prev_probs.unbind(1))

            # Sample the next token from the probability distribution
            next_token = torch.multinomial(probs, num_samples=1)

            # Append the probabilities and the generated token
            generated_sequence.append(next_token)
            # probs_list.append(probs)

            # Update the input_ids with the generated token
            model_in = torch.cat([model_in, next_token], dim=-1)

            if next_token.item() == eos_token_id:
                break

        # Decode and print the generated sequence
        generated_tokens = torch.cat(generated_sequence, dim=-1)

    # Stack the probabilities tensor --> this resulted in N - 1 probs missing the last one, to get that we have to do another forward pass, so we may as well do one round in the end to compute all the probs.
    # probs_tensor = torch.concat(probs_list, dim=0)

    probs_tensor = model(model_in).logits.softmax(dim=-1)

    return {
        "sequence": torch.cat([input_ids, generated_tokens], dim=-1),
        "probs": probs_tensor,
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
        # The maximum length of the generated text
        # This is just something that we use to normalize the reward for this 
        # particular reward function which minimizes the length of the generated text.
        self.max_generation_length = config["max_generation_length"]

        # This is the base tokenizer
        self.tokenizer = transformers.AutoTokenizer.from_pretrained(
            config["tokenizer_path"]
        )

        # This is the SFT model, used for computing the kl-divergence penalties.
        self.sft_model = transformers.GPT2LMHeadModel.from_pretrained(
            config["sft_model_path"]
        )

        # This is the reward model, it's either a LLM or a callable function 
        # that takes in a batch of generated text tokens + response mask and returns a 
        # batch of rewards.
        # TODO(Kourosh): Later allow users to pass in their own reward model factory.
        self.reward_model = ShortestAnswerRM(max_length=self.max_generation_length)

        # Prompt dataset
        # We need to load the prompt dataset and randomly sample a prompt from 
        # it upon reset.
        # TODO (Kourosh): Assuming we only support HF datasets for now:
        self.prompt_dataset = datasets.load_dataset(
            config["prompt_dataset_path"], 
            split=config["prompt_dataset_split"]
        )
        self.dsize = len(self.prompt_dataset)


        vocab_size = self.tokenizer.vocab_size
        model_max_length = self.tokenizer.model_max_length

        self.action_space = sp.Dict({
            "sequence": Repeated(sp.Discrete(vocab_size), max_len=model_max_length),
            "attention_mask": Repeated(sp.Discrete(2), max_len=model_max_length),
            "response_mask": Repeated(sp.Discrete(2), max_len=model_max_length),
            "probs": Repeated(
                sp.Box(0, 1, shape=(vocab_size,)), max_len=model_max_length
            ),
        })

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
        # remove the batch dimension since we can only do one sentence generation at a 
        # time.
        prompt_tokens = tree.map_structure(lambda x: x[0], prompt_tokens)

        return prompt_tokens, {}

    def step(self, action):
        # action is a dict with the following keys:
        # sequence: [b, NP+NR], prompt + generated response from the actor LLM
        # response_mask: [b, NP+NR], [0] * NP + [1] * NR To separate response from prompt
        # attention_mask: [b, NP+NR]
        # probs: [b, NR, vocab_size], the probabilities of each token in the vocab for each token in the input

        sequence = action["sequence"]
        response_mask = action["response_mask"]
        attention_mask = action["attention_mask"]
        probs = action["probs"]

        n_response_tokens = response_mask.sum()

        r_align = self.reward_model(sequence, attention_mask, response_mask)
        r_align = r_align.item()

        # Compute the probs from the sft model for the same number of tokens
        sequence = torch.tensor(sequence, dtype=torch.long)[None] # add batch dim
        sft_output = generate_response(
            self.sft_model, 
            input_ids=sequence, 
            max_length=n_response_tokens, 
            eos_token_id=self.tokenizer.eos_token_id
        )
        
        probs = torch.tensor(probs, dtype=torch.float32)[None] # add batch dim
        # only compute kl on the response tokens
        r_kl = compute_approx_kl(
            probs[:, -n_response_tokens:], 
            sft_output["probs"][:, -n_response_tokens:]
        ).item()

        reward = r_align - self.kl_coeff * r_kl

        info = {
            "r_align": r_align, 
            "r_kl": r_kl, 
            "n_response_tokens": n_response_tokens
        }

        # Produce a random reward when we reach the goal.
        return self.observation_space.sample(), reward, True, False, info