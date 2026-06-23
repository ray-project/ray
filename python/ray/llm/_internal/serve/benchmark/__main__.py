"""CLI entry point for the multi-turn OpenAI-compatible HTTP benchmark.

Example: python -m ray.llm._internal.serve.benchmark --help
"""
from ray.llm._internal.serve.benchmark.cli import main

main()
