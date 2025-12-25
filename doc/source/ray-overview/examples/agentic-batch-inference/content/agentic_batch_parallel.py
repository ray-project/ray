"""
Parallel Multi-Agent System

This example demonstrates parallel agent coordination where:
1. Plan Agent: Breaks down complex questions into focused subtasks
2. Weather Agent: Analyzes weather-related aspects
3. Science Agent: Analyzes science-related aspects

The workflow:
1. Plan Agent receives a question and breaks it into 2 subtasks
2. Two Explore Agents work in parallel on different aspects
3. Plan Agent synthesizes exploration results into a comprehensive answer
"""

import asyncio
import json
import re
from typing import Any, AsyncIterator, Dict, List, Tuple

import openai

import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.data.dataset import Dataset

# ========== LLM CONFIGS ==========
plan_llm_config = LLMConfig(
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
)

science_llm_config = LLMConfig(
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
)

LLM_CONFIGS = {
    "plan": plan_llm_config,
    "weather": weather_llm_config,
    "science": science_llm_config,
}

# ========== SYSTEM PROMPTS ==========
PLAN_AGENT_SYSTEM_PROMPT = """Break questions into 2 short task phrases (weather, science). Output only task descriptions, not explanations. Synthesize briefly."""

WEATHER_AGENT_SYSTEM_PROMPT = """Analyze weather aspects. Be brief."""

SCIENCE_AGENT_SYSTEM_PROMPT = """Analyze science aspects. Be brief."""


# ========== PARALLEL COORDINATOR CALLABLE ==========
class ParallelCoordinator:
    """Async callable class for parallel agent coordination."""

    def __init__(self, base_url: str = "http://localhost:8000/v1"):
        self.client = openai.AsyncOpenAI(base_url=base_url, api_key="dummy")
        self.plan_model_id = plan_llm_config.model_id
        self.weather_model_id = weather_llm_config.model_id
        self.science_model_id = science_llm_config.model_id

    async def _chat_completion(
        self,
        model_id: str,
        messages: List[Dict[str, str]],
    ) -> str:
        """Make a chat completion request via OpenAI client."""
        response = await self.client.chat.completions.create(
            model=model_id,
            messages=messages,
        )
        return response.choices[0].message.content

    async def _break_down_question(self, question: str) -> Tuple[str, str]:
        """Break down a question into weather and science subtasks.
        
        Raises:
            ValueError: If breakdown fails or subtasks are invalid.
        """
        breakdown_prompt = f"""{question}

Split into 2 short tasks (not explanations):

JSON:
{{
    "weather_subtask": "Analyze [weather aspect]",
    "science_subtask": "Examine [science aspect]"
}}"""

        content = await self._chat_completion(
            model_id=self.plan_model_id,
            messages=[
                {"role": "system", "content": PLAN_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": breakdown_prompt},
            ],
        )

        # Parse JSON from response
        json_match = re.search(r"\{.*\}", content, re.DOTALL)
        if json_match:
            try:
                parsed = json.loads(json_match.group())
                weather_subtask = parsed.get("weather_subtask", "").strip()
                science_subtask = parsed.get("science_subtask", "").strip()
                
                # Validate both subtasks are present and non-empty
                if not weather_subtask or not science_subtask:
                    raise ValueError(
                        f"Invalid breakdown: missing subtasks. "
                        f"Weather: {bool(weather_subtask)}, Science: {bool(science_subtask)}"
                    )
                
                return (weather_subtask, science_subtask)
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to parse JSON from breakdown response: {e}")

        # Fail fast if no valid JSON found
        raise ValueError(f"Failed to extract valid JSON from breakdown response for question: {question}")

    async def _explore_weather(self, subtask: str, question: str) -> str:
        """Explore the weather-related subtask."""
        prompt = f"""Subtask: {subtask}

Question: {question}

Brief analysis:"""

        return await self._chat_completion(
            model_id=self.weather_model_id,
            messages=[
                {"role": "system", "content": WEATHER_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )

    async def _explore_science(self, subtask: str, question: str) -> str:
        """Explore the science-related subtask."""
        prompt = f"""Subtask: {subtask}

Question: {question}

Brief analysis:"""

        return await self._chat_completion(
            model_id=self.science_model_id,
            messages=[
                {"role": "system", "content": SCIENCE_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )

    async def _synthesize_results(
        self, question: str, weather_result: str, science_result: str
    ) -> str:
        """Synthesize the exploration results into a comprehensive answer."""
        synthesis_prompt = f"""Question: {question}

Weather: {weather_result}

Science: {science_result}

Brief answer:"""

        return await self._chat_completion(
            model_id=self.plan_model_id,
            messages=[
                {"role": "system", "content": PLAN_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": synthesis_prompt},
            ],
        )

    async def _process_single_question(
        self, row_id: Any, question: Any
    ) -> Tuple[Any, str, str, str, str, str, str, str]:
        """Process a single question through the parallel agent system.
        
        Returns:
            Tuple with (row_id, question, weather_subtask, science_subtask, 
                       weather_result, science_result, final_answer, error)
            If error occurs, error field contains error message, others are empty strings.
        """
        # Convert numpy types to Python native types
        if hasattr(question, "item"):
            question = question.item()
        if hasattr(row_id, "item"):
            row_id = row_id.item()

        question_str = str(question)

        try:
            # Step 1: Break down the question (fail fast here if invalid)
            weather_subtask, science_subtask = await self._break_down_question(question_str)

            # Step 2: Explore in parallel
            weather_result, science_result = await asyncio.gather(
                self._explore_weather(weather_subtask, question_str),
                self._explore_science(science_subtask, question_str),
            )

            # Step 3: Synthesize results
            final_answer = await self._synthesize_results(
                question_str, weather_result, science_result
            )

            return (
                row_id,
                question_str,
                weather_subtask,
                science_subtask,
                weather_result,
                science_result,
                final_answer,
                "",  # No error
            )
        except Exception as e:
            # Fail fast for this row only
            error_msg = f"Failed to process row {row_id}: {str(e)}"
            return (
                row_id,
                question_str,
                "",  # Empty on error
                "",  # Empty on error
                "",  # Empty on error
                "",  # Empty on error
                "",  # Empty on error
                error_msg,  # Error message
            )

    async def __call__(self, batch: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """Process a batch of rows through the parallel agent system.

        Args:
            batch: A dictionary of lists containing 'id' and 'question' keys.

        Yields:
            A dictionary of lists with results from all agents.
            Failed rows have error messages in the 'error' field.
        """
        ids = batch["id"]
        questions = batch["question"]

        # Process all questions concurrently
        tasks = [
            self._process_single_question(row_id, question)
            for row_id, question in zip(ids, questions)
        ]
        results = await asyncio.gather(*tasks)

        (
            result_ids,
            result_questions,
            result_weather_subtasks,
            result_science_subtasks,
            result_weather_findings,
            result_science_findings,
            result_answers,
            result_errors,
        ) = zip(*results)

        yield {
            "id": list(result_ids),
            "question": list(result_questions),
            "weather_subtask": list(result_weather_subtasks),
            "science_subtask": list(result_science_subtasks),
            "weather_findings": list(result_weather_findings),
            "science_findings": list(result_science_findings),
            "answer": list(result_answers),
            "error": list(result_errors),
        }


# ========== SYNTHETIC DATASET ==========
def create_synthetic_dataset() -> Dataset:
    """Create a synthetic dataset of questions with weather and science aspects."""
    questions = [
        "How do El Niño weather patterns interact with ocean chemistry to affect marine ecosystems and coastal weather systems?",
        "What is the relationship between atmospheric pressure changes during storms and the underlying thermodynamic principles that drive precipitation formation?",
        "How do seasonal temperature variations in different climate zones relate to the Earth's axial tilt and orbital mechanics?",
        "What mechanisms connect the formation of thunderstorms to the electrical properties of water molecules and atmospheric charge distribution?",
        "How do jet stream patterns influence weather systems, and what are the fluid dynamics principles that govern these high-altitude wind currents?",
    ]

    data = [{"id": i, "question": question} for i, question in enumerate(questions)]
    return ray.data.from_items(data)


# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    print("=" * 80)
    print("Parallel Multi-Agent System")
    print("=" * 80)
    print()

    try:
        # Build and deploy all 3 LLM applications using build_openai_app
        print("Deploying LLM applications (3 different models)...")
        all_llm_configs = [plan_llm_config, weather_llm_config, science_llm_config]
        app = build_openai_app({"llm_configs": all_llm_configs})
        serve.run(app, blocking=False)
        print("✓ LLM deployments:")
        print(f"  - Plan agent: {plan_llm_config.model_id}")
        print(f"  - Weather agent: {weather_llm_config.model_id}")
        print(f"  - Science agent: {science_llm_config.model_id}")
        print()

        print("=" * 80)
        print("Deployment ready at http://localhost:8000/v1")
        print("Available models:")
        for config in all_llm_configs:
            print(f"  - {config.model_id}")
        print()

        # Create dataset
        print("Creating questions dataset...")
        dataset = create_synthetic_dataset()
        print(f"Dataset created with {dataset.count()} questions")
        print()

        # Process using Ray Data map_batches with async callable class
        results = dataset.map_batches(
            ParallelCoordinator,
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
