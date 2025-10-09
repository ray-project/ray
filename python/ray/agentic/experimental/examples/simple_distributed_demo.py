"""
Simple Distributed Tool Execution Demo (No API Keys Required)
==============================================================

Demonstrates Ray Agentic's distributed tool execution without requiring
external API keys. Uses the mock adapter to show how tools run on different
nodes with heterogeneous resources.

Usage:
    python simple_distributed_demo.py
"""

import time

import ray
from ray.agentic.experimental import AgentSession
from ray.agentic.experimental.adapters import _MockAdapter


# CPU-intensive tool
@ray.remote(num_cpus=4)
def process_data(dataset: str):
    """Simulate heavy data processing."""
    print(f"📊 [4-CPU Node] Processing {dataset}...")
    time.sleep(2)
    return f"Processed {dataset}: 1M rows analyzed"


# Memory-intensive tool
@ray.remote(memory=8 * 1024**3)
def analyze_logs(time_range: str):
    """Simulate log analysis requiring large memory."""
    print(f"🔍 [8GB-Memory Node] Analyzing logs for {time_range}...")
    time.sleep(1.5)
    return f"Analyzed {time_range}: Found 15 critical errors"


# GPU tool (simulated)
@ray.remote(num_gpus=1)
def generate_image(prompt: str):
    """Simulate GPU-based image generation."""
    print(f"🎨 [GPU Node] Generating image: {prompt}...")
    time.sleep(2)
    return f"Generated image: {prompt}.png"


# Lightweight API call
@ray.remote
def search_database(query: str):
    """Simulate database search."""
    print(f"🔎 [Worker Node] Searching: {query}...")
    time.sleep(1)
    return f"Found 42 results for '{query}'"


def main():
    print()
    print("=" * 70)
    print(" 🚀 Ray Agentic - Simple Distributed Demo")
    print("=" * 70)
    print()
    print("This demo shows tools running with different resource requirements.")
    print("Each tool automatically runs on nodes with appropriate resources.")
    print()
    print("Tools:")
    print("  📊 process_data      - 4 CPUs (heavy computation)")
    print("  🔍 analyze_logs      - 8GB RAM (memory-intensive)")
    print("  🎨 generate_image    - 1 GPU (model inference)")
    print("  🔎 search_database   - Default (lightweight)")
    print()
    print("=" * 70)
    print()

    # Initialize Ray
    print("🔧 Starting Ray...")
    ray.init(ignore_reinit_error=True)
    print("✅ Ray initialized")
    print()

    # Create agent session with mock adapter (no API key needed)
    print("📝 Creating agent session...")
    adapter = _MockAdapter()
    session = AgentSession.remote(session_id="demo", adapter=adapter)
    print("✅ Agent session created")
    print()

    # User message
    message = "Analyze Q4 data, check logs, generate dashboard, and search customers"
    print(f"💬 User: {message}")
    print()

    # Define tools with bound arguments
    tools = [
        process_data.bind("sales_q4_2024.csv"),
        analyze_logs.bind("last 24 hours"),
        generate_image.bind("revenue dashboard"),
        search_database.bind("enterprise customers"),
    ]

    print(f"🎯 Executing {len(tools)} distributed tools...")
    print()

    # Execute
    start = time.time()
    result = ray.get(session.run.remote(message, tools=tools))
    elapsed = time.time() - start

    print()
    print("=" * 70)
    print(f"✨ Completed in {elapsed:.2f} seconds")
    print("=" * 70)
    print()
    print("📋 Agent Response:")
    print(f"   {result['content']}")
    print()
    print("🔧 Tool Results:")
    for i, tool_result in enumerate(result.get("tool_results", []), 1):
        print(f"   {i}. {tool_result}")
    print()
    print("=" * 70)
    print(" 💡 Key Benefits")
    print("=" * 70)
    print()
    print("✅ Resource Heterogeneity:")
    print("   • Each tool runs on nodes with required resources")
    print("   • No resource contention between tools")
    print()
    print("✅ True Distribution:")
    print("   • Tools can run on different physical machines")
    print("   • Automatic scheduling based on availability")
    print()
    print("✅ Fault Tolerance:")
    print("   • Failed tools automatically retry")
    print("   • Worker crashes don't affect other tools")
    print()
    print("🔄 vs. Local Frameworks (LangGraph, CrewAI):")
    print("   • Local: All tools share same process/machine")
    print("   • Ray Agentic: Each tool gets dedicated resources")
    print()
    print("=" * 70)
    print()

    ray.shutdown()


if __name__ == "__main__":
    main()
