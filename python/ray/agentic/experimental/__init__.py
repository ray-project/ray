"""
Experimental distributed agent execution on Ray.

This module provides primitives for running LLM agents with distributed tool
execution across Ray clusters, enabling:

- **Distributed resource heterogeneity**: GPU tools on GPU nodes, CPU tools on CPU nodes
- **True isolation**: Each tool as separate Ray task with enforced resource limits
- **Cluster-wide execution**: Tools run across multiple machines automatically
- **Fault tolerance**: Built-in retries and error handling
- **Stateful sessions**: Ray actors maintain conversation state

Example:
    >>> import ray
    >>> from ray.agentic.experimental import AgentSession
    >>> from ray.agentic.experimental.adapters import LangGraphAdapter
    >>>
    >>> # Define tools with specific resource requirements
    >>> @ray.remote(num_gpus=1)
    >>> def generate_image(prompt: str):
    ...     # Runs on GPU node
    ...     return "image.png"
    >>>
    >>> @ray.remote(num_cpus=16)
    >>> def process_data(file: str):
    ...     # Runs on high-CPU node
    ...     return {"result": "processed"}
    >>>
    >>> # Create agent session
    >>> session = AgentSession.remote("user_123", LangGraphAdapter())
    >>>
    >>> # Agent automatically routes tools to appropriate nodes
    >>> result = ray.get(session.run.remote(
    ...     "Generate an image and process the data",
    ...     tools=[generate_image, process_data]
    ... ))

**DeveloperAPI:** This API is experimental and may change across minor Ray releases.
"""

from ray.agentic.experimental.adapters import AgentAdapter, LangGraphAdapter

# Import main classes
from ray.agentic.experimental.session import AgentSession

__all__ = [
    "AgentSession",
    "AgentAdapter",
    "LangGraphAdapter",
]
