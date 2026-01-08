"""
Conditional Routing Multi-Agent System

This example demonstrates conditional routing where:
1. Router Agent: Classifies user queries into categories
2. Specialized Agents: Handle queries based on their category

The workflow:
1. Router Agent receives a user query and classifies it
2. Query is routed to the appropriate specialized agent
3. Specialized agent processes and responds
"""

import asyncio
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import openai

import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.data.dataset import Dataset

class RouteCategory(str, Enum):
    weather = "weather"
    science = "science"
    unknown = "unknown"

# ========== LLM CONFIGS ==========
science_llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="Qwen/Qwen3-4B-Instruct-2507-FP8",
        model_source="Qwen/Qwen3-4B-Instruct-2507-FP8",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            num_replicas=1,
        )
    ),
    engine_kwargs=dict(
        max_model_len=32768,
        trust_remote_code=True,
        gpu_memory_utilization=0.8,
    ),
    accelerator_type="L4",
)

weather_llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="Qwen/Qwen2.5-1.5B-Instruct",
        model_source="Qwen/Qwen2.5-1.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            num_replicas=1,
        )
    ),
    engine_kwargs=dict(
        max_model_len=8192,
        trust_remote_code=True,
        gpu_memory_utilization=0.8,
    ),
    accelerator_type="L4",
)

unknown_llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="Qwen/Qwen2.5-0.5B-Instruct",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            num_replicas=1,
        )
    ),
    engine_kwargs=dict(
        max_model_len=4096,
        trust_remote_code=True,
        gpu_memory_utilization=0.8,
    ),
    accelerator_type="L4",
)

MODEL_IDS = {
    RouteCategory.weather: weather_llm_config.model_id,
    RouteCategory.science: science_llm_config.model_id,
    RouteCategory.unknown: unknown_llm_config.model_id,
}

# Use the least capable model for classification (router)
router_model_id = unknown_llm_config.model_id


# ========== SYSTEM PROMPTS ==========
ROUTER_AGENT_SYSTEM_PROMPT = """You are a query router. Your role is to classify user queries into categories.

Categories:
- weather: Questions about weather conditions, forecasts, temperature, climate
- science: Questions about science, physics, chemistry, biology, mathematics, technology
- unknown: Queries that don't clearly fit weather or science categories

Classify each query into exactly one of these three categories: weather, science, or unknown."""

WEATHER_AGENT_SYSTEM_PROMPT = """You are a weather specialist. Your role is to provide accurate, helpful weather information.

When responding:
- Provide clear weather forecasts
- Include relevant details like temperature, conditions, and location
- Be concise and informative"""

SCIENCE_AGENT_SYSTEM_PROMPT = """You are a science specialist. Your role is to explain scientific concepts clearly and accurately.

When responding:
- Explain concepts in accessible language
- Provide accurate scientific information
- Use examples when helpful"""

UNKNOWN_AGENT_SYSTEM_PROMPT = """You are a helpful assistant. Your role is to handle queries that don't fit into specific categories.

When responding:
- Be polite and helpful
- Acknowledge that the query couldn't be categorized
- Suggest how the user might rephrase their query
- Offer general assistance"""


SYSTEM_PROMPTS = {
    RouteCategory.weather: WEATHER_AGENT_SYSTEM_PROMPT,
    RouteCategory.science: SCIENCE_AGENT_SYSTEM_PROMPT,
    RouteCategory.unknown: UNKNOWN_AGENT_SYSTEM_PROMPT,
}

# ========== CONDITIONAL ROUTER CALLABLE ==========
class ConditionalRouter:
    """Async callable class for conditional routing through LLM agents."""

    def __init__(self, base_url: str = "http://localhost:8000/v1"):
        self.client = openai.AsyncOpenAI(base_url=base_url, api_key="dummy")

    async def _chat_completion(
        self,
        model_id: str,
        messages: List[Dict[str, str]],
        structured_outputs: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Make a chat completion request via OpenAI client.

        Args:
            model_id: The model ID to use for the request.
            messages: The chat messages.
            structured_outputs: Optional structured outputs configuration.
        """
        extra_body = {}
        if structured_outputs:
            extra_body["structured_outputs"] = structured_outputs

        response = await self.client.chat.completions.create(
            model=model_id,
            messages=messages,
            extra_body=extra_body if extra_body else None,
        )

        return response.choices[0].message.content

    async def _classify_query(self, query: str) -> RouteCategory:
        """Classify a query using the router agent with structured outputs."""

        router_prompt = f"""Analyze the user query below and determine its category.

Categories:
- weather: For questions about weather conditions, forecasts, temperature, climate
- science: For questions about science, physics, chemistry, biology, mathematics, technology
- unknown: If the category is unclear or doesn't fit weather or science

Query: {query}

Classify this query."""

        content = await self._chat_completion(
            model_id=router_model_id,
            messages=[
                {"role": "system", "content": ROUTER_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": router_prompt},
            ],
            structured_outputs={"choice": ["weather", "science", "unknown"]},
        )

        return RouteCategory(content.strip().lower())

    async def _generate_response(self, query: str, category: RouteCategory) -> str:
        """Generate a response from the specialized agent."""
        system_prompt = SYSTEM_PROMPTS[category]
        model_id = MODEL_IDS[category]

        return await self._chat_completion(
            model_id=model_id,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query},
            ],
        )

    async def _process_single_query(
        self, row_id: Any, query: Any
    ) -> Tuple[Any, str, str, str]:
        """Process a single query: classify and generate response."""
        # Convert numpy types to Python native types
        if hasattr(query, "item"):
            query = query.item()
        if hasattr(row_id, "item"):
            row_id = row_id.item()

        query_str = str(query)

        # Step 1: Classify the query
        category = await self._classify_query(query_str)

        # Step 2: Generate response from specialized agent
        response = await self._generate_response(query_str, category)

        return row_id, query_str, category.value, response

    async def __call__(
        self, batch: Dict[str, Any]
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a batch of rows through the conditional routing system.

        Args:
            batch: A dictionary of lists containing 'id' and 'query' keys.

        Yields:
            A dictionary of lists with 'id', 'query', 'category', and 'response'.
        """
        ids = batch["id"]
        queries = batch["query"]

        # Process all queries concurrently
        tasks = [
            self._process_single_query(row_id, query)
            for row_id, query in zip(ids, queries)
        ]
        results = await asyncio.gather(*tasks)

        # Unpack results
        result_ids, result_queries, result_categories, result_responses = zip(
            *results
        ) if results else ([], [], [], [])

        yield {
            "id": list(result_ids),
            "query": list(result_queries),
            "category": list(result_categories),
            "response": list(result_responses),
        }

# ========== SYNTHETIC DATASET ==========
def create_synthetic_dataset() -> Dataset:
    """Create a synthetic dataset of user queries."""
    queries = [
        "Explain quantum physics simply.",
        "Hello world!",
        "Will it rain tomorrow in San Francisco?",
        "What is the theory of relativity?",
        "Tell me about the weather in New York.",
    ]

    data = [{"id": i, "query": query} for i, query in enumerate(queries)]
    return ray.data.from_items(data)


# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    print("=" * 80)
    print("Conditional Routing Multi-Agent System")
    print("=" * 80)
    print()

    try:
        # Build and deploy all 3 LLM applications using build_openai_app
        print("Deploying LLM applications (3 different models)...")
        all_llm_configs = [science_llm_config, weather_llm_config, unknown_llm_config]
        app = build_openai_app({"llm_configs": all_llm_configs})
        serve.run(app, blocking=False)
        print("✓ LLM deployments:")
        print(f"  - Science agent: {science_llm_config.model_id}")
        print(f"  - Weather agent: {weather_llm_config.model_id}")
        print(f"  - Unknown agent: {unknown_llm_config.model_id}")
        print()

        print("=" * 80)
        print("Deployment ready at http://localhost:8000/v1")
        print("Available models:")
        for config in all_llm_configs:
            print(f"  - {config.model_id}")
        print()

        # Create dataset
        print("Creating queries dataset...")
        dataset = create_synthetic_dataset()
        print(f"Dataset created with {dataset.count()} queries")
        print()

        # Process using Ray Data map_batches with async callable class
        results = dataset.map_batches(
            ConditionalRouter,
            batch_size=5,
            compute=ray.data.ActorPoolStrategy(size=1),
            # GPUs are consumed by the Ray Serve applications hosting the LLMs not the Ray Data actors
            num_cpus=1,
        )

        results.show(limit=10)

    finally:
        # Clean up Ray Serve applications
        print()
        print("Shutting down Ray Serve applications...")
        serve.shutdown()
        print("✓ All applications shut down")