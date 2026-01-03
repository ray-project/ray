"""Agentic Multi-Turn Rollout with Tool Calling.

This example demonstrates multi-turn agentic tool calling where the agent
repeatedly invokes tools until its goal is completed.

Workflow:
    1. LLM Service: Deployed via build_openai_app for generating tool calls
    2. Tool Service: Deployed as a FastAPI HTTP endpoint for executing tools
    3. Multi-Turn Loop: Agent calls tools until completion or max turns

Multi-turn process:
    1. Initial LLM call with user query and available tools
    2. Extract tool calls from LLM response
    3. Execute tools via HTTP endpoint
    4. Add tool results to conversation history
    5. Repeat until no more tool calls or max turns reached
"""

import asyncio
import json
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx
import openai
from fastapi import FastAPI
from pydantic import BaseModel

import ray
from ray import serve
from ray.data.dataset import Dataset
from ray.serve.llm import LLMConfig, build_openai_app

# ============================================================================
# Constants
# ============================================================================

# Available time slots
ALL_SLOTS = [
    "09:00", "10:00", "11:00", "12:00",
    "13:00", "14:00", "15:00", "16:00", "17:00",
]

"""Simulated calendar state - list of booked times."""
BOOKED_SLOTS: List[str] = ["10:00", "14:00"]


SYSTEM_PROMPT = """\
You are a helpful calendar assistant with access to tools for managing appointments.

IMPORTANT: When the user asks to book an appointment, you MUST use the 
book_appointment tool. Do NOT just say you will book something - actually 
call the tool.

Available tools:
- check_availability: Check if a specific time slot is available
- list_available_slots: List all available slots
- book_appointment: Book an appointment at a specific time

If a requested slot is not available, find an alternative and book it."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "check_availability",
            "description": "Check if a specific time slot is available",
            "parameters": {
                "type": "object",
                "properties": {
                    "time": {
                        "type": "string",
                        "description": "Time in HH:MM format (24-hour)",
                    },
                },
                "required": ["time"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "book_appointment",
            "description": "Book an appointment at a specific time",
            "parameters": {
                "type": "object",
                "properties": {
                    "time": {
                        "type": "string",
                        "description": "Time in HH:MM format (24-hour)",
                    },
                    "title": {
                        "type": "string",
                        "description": "Title of the appointment",
                    },
                },
                "required": ["time", "title"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_available_slots",
            "description": "List all available time slots",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
]

# ============================================================================
# LLM Configuration
# ============================================================================

LLM_CONFIG = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen3-4B-Instruct-2507-FP8",
        "model_source": "Qwen/Qwen3-4B-Instruct-2507-FP8",
    },
    deployment_config={
        "autoscaling_config": {"num_replicas": 1},
    },
    engine_kwargs={
        "max_model_len": 32768,
        "trust_remote_code": True,
        "gpu_memory_utilization": 0.9,
        "enable_auto_tool_choice": True,
        "tool_call_parser": "hermes",
    },
    accelerator_type="L4",
)

# ============================================================================
# Calendar Tool Service
# ============================================================================

calendar_app = FastAPI()


class CheckAvailabilityRequest(BaseModel):
    """Request model for checking availability."""

    time: str


class BookAppointmentRequest(BaseModel):
    """Request model for booking an appointment."""

    time: str
    title: str


@calendar_app.post("/check_availability")
async def check_availability(request: CheckAvailabilityRequest) -> Dict[str, str]:
    """Check if a time slot is available.

    Args:
        request: The availability check request.

    Returns:
        Result indicating if the slot is available.
    """
    if request.time in BOOKED_SLOTS:
        return {"result": f"Time slot {request.time} is NOT available."}
    return {"result": f"Time slot {request.time} is available."}


@calendar_app.post("/book_appointment")
async def book_appointment(request: BookAppointmentRequest) -> Dict[str, str]:
    """Book an appointment at a specific time.

    Args:
        request: The booking request with time and title.

    Returns:
        Result indicating success or failure.
    """
    if request.time in BOOKED_SLOTS:
        return {"result": f"ERROR: Cannot book {request.time} - already booked."}
    BOOKED_SLOTS.append(request.time)
    return {"result": f"Successfully booked '{request.title}' at {request.time}."}


@calendar_app.get("/list_available_slots")
async def list_available_slots() -> Dict[str, str]:
    """List all available time slots.

    Returns:
        Comma-separated list of available slots.
    """
    available = [slot for slot in ALL_SLOTS if slot not in BOOKED_SLOTS]
    return {"result": f"Available slots: {', '.join(available)}"}


@calendar_app.get("/state")
async def get_state() -> Dict[str, Any]:
    """Get the current calendar state.

    Returns:
        Dictionary with booked and available slots.
    """
    available = [slot for slot in ALL_SLOTS if slot not in BOOKED_SLOTS]
    return {"booked": sorted(BOOKED_SLOTS), "available": available}


@serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 1})
@serve.ingress(calendar_app)
class CalendarToolService:
    """Ray Serve deployment wrapping the FastAPI calendar service to enable flexible scaling."""
    pass


# ============================================================================
# Agentic Tool Caller
# ============================================================================


class AgenticToolCaller:
    """Async callable class for multi-turn agentic tool calling.

    This class processes queries through a multi-turn loop, calling an LLM
    that can invoke tools until the task is complete.

    Attributes:
        llm_client: OpenAI-compatible async client for LLM calls.
        tool_base_url: Base URL for the tool service.
        max_turns: Maximum number of conversation turns.
        model_id: The model ID to use for LLM calls.
        http_client: Async HTTP client for tool calls.
    """

    def __init__(
        self,
        llm_base_url: str = "http://localhost:8000/v1",
        tool_base_url: str = "http://localhost:8000/calendar",
        max_turns: int = 5,
    ) -> None:
        """Initialize the AgenticToolCaller.

        Args:
            llm_base_url: Base URL for the LLM service.
            tool_base_url: Base URL for the tool service.
            max_turns: Maximum conversation turns before stopping.
        """
        self.llm_client = openai.AsyncOpenAI(
            base_url=llm_base_url, api_key="dummy"
        )
        self.tool_base_url = tool_base_url
        self.max_turns = max_turns
        self.model_id = LLM_CONFIG.model_id
        self.http_client = httpx.AsyncClient(timeout=60.0)

    async def _generate(
        self,
        messages: List[Dict[str, Any]],
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Any:
        """Call the LLM with messages and optional tools.

        Args:
            messages: Conversation history.
            tools: Tool definitions for function calling.

        Returns:
            The LLM response object.
        """
        return await self.llm_client.chat.completions.create(
            model=self.model_id,
            messages=messages,
            tools=tools,
            tool_choice="auto" if tools else None,
        )

    async def _execute_tool(self, tool_call: Any) -> str:
        """Execute a tool call via HTTP.

        Args:
            tool_call: The tool call object from the LLM response.

        Returns:
            The tool execution result as a string.
        """
        tool_name = tool_call.function.name
        try:
            tool_args = json.loads(tool_call.function.arguments)
        except json.JSONDecodeError:
            return f"Error: Invalid JSON arguments for {tool_name}"

        # Route to appropriate endpoint with validation
        if tool_name == "check_availability":
            time = tool_args.get("time")
            if not time:
                return "Error: 'time' is required for check_availability"
            response = await self.http_client.post(
                f"{self.tool_base_url}/{tool_name}",
                json={"time": time},
            )
        elif tool_name == "book_appointment":
            time, title = tool_args.get("time"), tool_args.get("title")
            if not time or not title:
                return "Error: 'time' and 'title' required for book_appointment"
            response = await self.http_client.post(
                f"{self.tool_base_url}/{tool_name}",
                json={"time": time, "title": title},
            )
        elif tool_name == "list_available_slots":
            response = await self.http_client.get(
                f"{self.tool_base_url}/{tool_name}"
            )
        else:
            return f"Error: Unknown tool '{tool_name}'"

        response.raise_for_status()
        return response.json().get("result", "")

    async def _process_query(
        self, row_id: Any, query: Any
    ) -> Tuple[Any, str, int, int, str]:
        """Process a single query through the multi-turn agentic loop.

        Args:
            row_id: The row identifier.
            query: The user query to process.

        Returns:
            Tuple of (row_id, query, turns, tool_calls, final_response).
        """
        # Convert numpy types to Python native types
        if hasattr(query, "item"):
            query = query.item()
        if hasattr(row_id, "item"):
            row_id = row_id.item()

        query_str = str(query)
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": query_str},
        ]
        total_tool_calls = 0
        final_response = ""

        for turn in range(1, self.max_turns + 1):
            response = await self._generate(messages, TOOLS)
            message = response.choices[0].message
            content = message.content or ""
            tool_calls = message.tool_calls or []

            # Build assistant message
            assistant_msg: Dict[str, Any] = {
                "role": "assistant",
                "content": content,
            }
            if tool_calls:
                assistant_msg["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in tool_calls
                ]
            # Add assistant message to conversation history. This is essential for multi-turn conversations.
            messages.append(assistant_msg)

            # Done if no tool calls
            if not tool_calls:
                final_response = content
                return row_id, query_str, turn, total_tool_calls, final_response

            # Execute each tool call
            for tool_call in tool_calls:
                result = await self._execute_tool(tool_call)
                messages.append({
                    "role": "tool",
                    "content": str(result),
                    "tool_call_id": tool_call.id,
                })
                total_tool_calls += 1

            final_response = content

        return row_id, query_str, self.max_turns, total_tool_calls, final_response

    async def __call__(
        self, batch: Dict[str, Any]
    ) -> AsyncIterator[Dict[str, Any]]:
        """Process a batch of rows through the agentic tool calling system.

        Args:
            batch: Dictionary with 'id' and 'query' lists.

        Yields:
            Dictionary with processed results.
        """
        tasks = [
            self._process_query(row_id, query)
            for row_id, query in zip(batch["id"], batch["query"])
        ]
        results = await asyncio.gather(*tasks)

        if results:
            ids, queries, turns, tool_calls, responses = zip(*results)
        else:
            ids, queries, turns, tool_calls, responses = [], [], [], [], []

        yield {
            "id": list(ids),
            "query": list(queries),
            "turns": list(turns),
            "tool_calls": list(tool_calls),
            "response": list(responses),
        }


# ============================================================================
# Dataset and Utilities
# ============================================================================


def create_synthetic_dataset() -> Dataset:
    """Create a synthetic dataset with booking queries.

    Returns:
        Ray Dataset with sample booking queries.
    """
    queries = [
        "Book me an appointment for a team meeting at 11:00.",
        "What times are available? Book the latest available slot.",
        "Find an available slot and book it for 'Project Review'.",
    ]
    return ray.data.from_items([
        {"id": i, "query": query} for i, query in enumerate(queries)
    ])


def get_calendar_state(
    base_url: str = "http://localhost:8000/calendar",
) -> Dict[str, List[str]]:
    """Get the current calendar state via HTTP.

    Args:
        base_url: Base URL for the calendar service.

    Returns:
        Dictionary with 'booked' and 'available' slot lists.
    """
    response = httpx.get(f"{base_url}/state", timeout=10.0)
    response.raise_for_status()
    return response.json()


def display_calendar_comparison(
    initial_state: Dict[str, List[str]],
    final_state: Dict[str, List[str]],
) -> None:
    """Display the initial and final calendar states.

    Args:
        initial_state: Calendar state before processing.
        final_state: Calendar state after processing.
    """
    print()
    print("=" * 60)
    print("CALENDAR STATE COMPARISON")
    print("=" * 60)
    print()
    print("BEFORE:")
    print(f"  Booked:    {', '.join(initial_state.get('booked', []))}")
    print(f"  Available: {', '.join(initial_state.get('available', []))}")
    print()
    print("AFTER:")
    print(f"  Booked:    {', '.join(final_state.get('booked', []))}")
    print(f"  Available: {', '.join(final_state.get('available', []))}")
    print()

    initial_booked = set(initial_state.get("booked", []))
    final_booked = set(final_state.get("booked", []))
    newly_booked = final_booked - initial_booked
    if newly_booked:
        print(f"NEW BOOKINGS: {', '.join(sorted(newly_booked))}")
    print("=" * 60)


# ============================================================================
# Main Execution
# ============================================================================


def main() -> None:
    """Run the agentic tool calling example."""
    print("=" * 80)
    print("Agentic Multi-Turn Tool Calling System")
    print("=" * 80)
    print()

    try:
        # Deploy calendar tool service
        print("Deploying Calendar Tool Service...")
        serve.run(
            CalendarToolService.bind(),
            name="calendar_tool_app",
            route_prefix="/calendar",
            blocking=False,
        )
        print("✓ Calendar Tool Service deployed")
        print()

        # Deploy LLM application
        print("Deploying LLM application...")
        llm_app = build_openai_app({"llm_configs": [LLM_CONFIG]})
        serve.run(llm_app, blocking=False)
        print(f"✓ LLM deployed: {LLM_CONFIG.model_id}")
        print()

        # Capture initial calendar state
        initial_state = get_calendar_state()

        # Create and process dataset
        print("Creating queries dataset...")
        dataset = create_synthetic_dataset()
        print(f"Dataset created with {dataset.count()} queries")
        print()

        print("Processing queries through agentic tool calling...")
        results = dataset.map_batches(
            AgenticToolCaller,
            batch_size=1,
            compute=ray.data.ActorPoolStrategy(size=1),
            num_cpus=1,
            fn_constructor_kwargs=dict(
                llm_base_url="http://localhost:8000/v1",
                tool_base_url="http://localhost:8000/calendar",
                max_turns=5,
            ),
        )

        results.show(limit=10)

        # Display comparison
        final_state = get_calendar_state()
        display_calendar_comparison(initial_state, final_state)

    finally:
        print()
        print("Shutting down Ray Serve applications...")
        serve.shutdown()
        print("✓ All applications shut down")


if __name__ == "__main__":
    main()