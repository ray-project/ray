"""
Simple Distributed Tool Execution Demo (No API Keys Required)
==============================================================

Demonstrates Ray Agentic's distributed tool execution without requiring
external API keys. Uses the mock adapter to show how tools run on different
nodes with heterogeneous resources.

Usage:
    python simple_distributed_demo.py
"""

import ray
from ray.agentic.experimental import AgentSession
from ray.agentic.experimental.adapters import _MockAdapter


@ray.remote(num_cpus=4)
def process_data(dataset: str):
    """Simulate heavy data processing."""
    return f"Processed {dataset}: 1M rows analyzed"


@ray.remote(memory=8 * 1024**3)
def analyze_logs(time_range: str):
    """Simulate log analysis requiring large memory."""
    return f"Analyzed {time_range}: Found 15 critical errors"


@ray.remote(num_gpus=1)
def generate_image(prompt: str):
    """Simulate GPU-based image generation."""
    return f"Generated image: {prompt}.png"


@ray.remote
def search_database(query: str):
    """Simulate database search."""
    return f"Found 42 results for '{query}'"


def main():
    # Initialize Ray
    ray.init(ignore_reinit_error=True)

    # Create agent session with mock adapter (no API key needed)
    adapter = _MockAdapter()
    session = AgentSession.remote(session_id="demo", adapter=adapter)

    # Define tools with bound arguments
    tools = [
        process_data.bind("sales_q4_2024.csv"),
        analyze_logs.bind("last 24 hours"),
        generate_image.bind("revenue dashboard"),
        search_database.bind("enterprise customers"),
    ]

    # Execute
    message = "Analyze Q4 data, check logs, generate dashboard, and search customers"
    result = ray.get(session.run.remote(message, tools=tools))

    print(f"Agent: {result['content']}")
    print("\nTool Results:")
    for tool_result in result.get("tool_results", []):
        print(f"  - {tool_result}")

    ray.shutdown()


if __name__ == "__main__":
    main()
