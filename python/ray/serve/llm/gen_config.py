"""Stub for the removed Serve LLM config generator.

The interactive RayLLM config generator is no longer supported and this command
will be removed in a future Ray version. See https://recipes.vllm.ai/ for current
guidance on serving LLMs.
"""
import sys

_MESSAGE = (
    "The Serve LLM config generator is no longer supported and this command "
    "will be removed in a future Ray version. "
    "See https://recipes.vllm.ai/ for current guidance on serving LLMs."
)


def main():
    print(_MESSAGE, file=sys.stderr)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
