from typing import List, Optional
from enum import Enum
from pydantic import BaseModel, Field
import argparse


class DistributionType(str, Enum):
    CONSTANT = "constant"
    UNIFORM = "uniform"
    EXPONENTIAL = "exponential"
    NORMAL = "normal"


class TokensDistributionType(str, Enum):
    CONSTANT = "constant"
    UNIFORM = "uniform"
    EXPONENTIAL = "exponential"


class LoadTestConfig(BaseModel):
    provider: Optional[str] = Field(
        None,
        description="Which flavor of API to use. If not specified, we'll try to guess based on the URL and /v1/models output",
    )

    model: Optional[str] = Field(
        None,
        description="The model to use for generating text. If not specified we will pick the first model from the service as returned by /v1/models",
    )

    chat: bool = Field(True, description="Use /v1/chat/completions API")

    prompt_tokens: int = Field(
        512,
        description="Length of the prompt in tokens",
    )

    prompt_chars: Optional[int] = Field(
        None,
        description="Length of the prompt in characters",
    )

    prompt_text: Optional[str] = Field(
        None,
        description="Prompt text to use instead of generating one. It can be a file reference starting with an ampersand, e.g. `@prompt.txt`",
    )

    prompt_randomize: bool = Field(
        False,
        description="Include a few random numbers in the generated prompt to avoid caching",
    )

    max_tokens: int = Field(
        64,
        description="Max number of tokens to generate. If max_tokens_distribution is non-constant this is going to be the mean",
    )

    max_tokens_cap: Optional[int] = Field(
        None,
        description="If max_tokens_distribution is non-constant, this truncates the distribition at the specified limit",
    )

    max_tokens_distribution: TokensDistributionType = Field(
        TokensDistributionType.CONSTANT,
        description="How to sample max_tokens on each request",
    )

    max_tokens_range: float = Field(
        0.3,
        description="Specifies the width of the distribution. Specified value `alpha` is relative to `max_tokens`",
    )

    stream: bool = Field(True, description="Use the streaming API")

    api_key: Optional[str] = Field(
        None,
        description="Auth for the API",
    )

    temperature: float = Field(0.1, description="Temperature parameter for the API")

    logprobs: Optional[int] = Field(
        None,
        description="Whether to ask for logprobs, it makes things slower for some providers but is necessary for token count in streaming",
    )

    summary_file: Optional[str] = Field(
        None,
        description="Append the line with the summary to the specified CSV file",
    )

    qps: Optional[float] = Field(
        None,
        description="Enabled 'fixed QPS' mode where requests are issues at the specified rate regardless of how long the processing takes",
    )

    qps_distribution: DistributionType = Field(
        DistributionType.CONSTANT,
        description="Must be used with qps. Specifies how to space out requests",
    )

    burst: Optional[float] = Field(
        None,
        description="Makes requests to arrive in bursts every specified number of seconds",
    )

    tokenizer: Optional[str] = Field(
        None,
        description="Specify HF tokenizer to use for validating the output of the model",
    )

    show_response: bool = Field(
        False,
        description="Print the result of each generation",
    )

    prompt_cache_max_len: int = Field(
        0,
        description="Maximum length of the prompt cache to use",
    )

    header: List[str] = Field(
        default_factory=list,
        description="Arbitrary headers to add to the inference request",
    )

    n: int = Field(
        1,
        description="How many sequences to generate (makes sense to use with non-zero temperature)",
    )

    host: Optional[str] = Field(
        default=None,
        description="Host to load test in the following format: http://10.21.32.33",
    )

    reset_stats: bool = Field(
        default=True,
        description="Determines if stats should be reset once hatching is complete",
    )

    users: int = Field(
        default=None,
        description="Number of concurrent users to spawn for benchmarking.",
    )

    run_time: str = Field(
        default="30s",
        description="The runtime it is in form of Ns, Nm, or Nh, for seconds, minutes, and hours.",
    )

    def to_namespace(self) -> argparse.Namespace:
        """
        Convert the model to an argparse.Namespace object
        """
        return argparse.Namespace(**self.dict())
