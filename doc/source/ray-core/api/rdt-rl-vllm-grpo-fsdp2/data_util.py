from datasets import load_dataset
import re
from typing import Any

import ray


def load_gsm8k_dataset(split: str, sample_count: int) -> list[tuple[str, str]]:
    dataset = load_dataset("gsm8k", "main")
    rows = dataset[split]
    items: list[tuple[str, str]] = []
    for row in rows:
        items.append((row["question"], row["answer"]))
        if len(items) >= sample_count:
            break
    return items


def extract_solution(solution_str, method="strict"):
    if method not in ["strict", "flexible"]:
        raise ValueError("method must be 'strict' or 'flexible'")

    if method == "strict":
        # This also tests the formatting of the model output.
        solution = re.search("#### (\\-?[0-9\\.\\,]+)", solution_str)
        if solution is None:
            final_answer = None
        else:
            final_answer = solution.group(0)
            final_answer = (
                final_answer.split("#### ")[1].replace(",", "").replace("$", "")
            )
    elif method == "flexible":
        answer = re.findall("(\\-?[0-9\\.\\,]+)", solution_str)
        final_answer = None
        if len(answer) == 0:
            # No reward if there is no answer.
            pass
        else:
            invalid_str = ["", "."]
            # Find the last number that is not '.'
            for final_answer in reversed(answer):
                if final_answer not in invalid_str:
                    break
    return final_answer


def compute_score(
    solution_str, ground_truth, method="flexible", format_score=0.0, score=1.0
):
    """The scoring function for GSM8k."""
    answer = extract_solution(solution_str=solution_str, method=method)
    if answer is None:
        return 0
    else:
        if answer == ground_truth:
            return score
        else:
            return format_score


@ray.remote
class Scorer:
    """Evaluates responses and assigns rewards by comparing GSM8k numeric answers."""

    def __init__(self, replay_buffer) -> None:
        self.replay_buffer = replay_buffer

    @ray.method(tensor_transport="nixl")  # CPU-CPU RDT
    def score_group(self, group: dict[str, Any]) -> None:
        """Score a group of responses and add rewards."""
        group["gold_answer"] = extract_solution(group["gold_answer"])

        rewards = []
        for response_str in group["responses"]:
            reward = compute_score(response_str, group["gold_answer"])
            rewards.append(reward)

        group["rewards"] = rewards
        self.replay_buffer.put.remote(group)
