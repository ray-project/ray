"""Agent session actor for stateful agent execution."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.agentic.experimental.adapters import AgentAdapter

logger = logging.getLogger(__name__)


@DeveloperAPI
@ray.remote
class AgentSession:
    """
    Stateful Ray actor for a single agent session.

    An AgentSession maintains conversation state across multiple interactions
    and delegates agent execution to a framework-specific adapter (e.g.,
    LangGraph, CrewAI). Tools can be executed as distributed Ray tasks with
    heterogeneous resource requirements.

    Example:
        >>> import ray
        >>> from ray.agentic.experimental import AgentSession
        >>> from ray.agentic.experimental.adapters import LangGraphAdapter
        >>>
        >>> # Create agent session
        >>> session = AgentSession.remote(
        ...     session_id="user_123",
        ...     adapter=LangGraphAdapter(model="gpt-4o")
        ... )
        >>>
        >>> # Run agent
        >>> result = ray.get(session.run.remote("Tell me a joke"))
        >>> print(result["content"])
        >>>
        >>> # Get conversation history
        >>> history = ray.get(session.get_history.remote())

    **DeveloperAPI:** This API may change across minor Ray releases.
    """

    def __init__(self, session_id: str, adapter: "AgentAdapter"):
        """
        Initialize agent session.

        Args:
            session_id: Unique identifier for this session (e.g., user ID)
            adapter: Framework adapter implementing AgentAdapter interface
        """
        self.session_id = session_id
        self.adapter = adapter
        self.messages: List[Dict[str, Any]] = []

        logger.info(f"Initialized AgentSession: {session_id}")

    async def run(
        self, message: str, tools: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute agent with a message and optional distributed tools.

        This method:
        1. Adds user message to conversation history
        2. Delegates to adapter for agent execution
        3. Adds agent response to conversation history
        4. Returns response

        Tools are Ray remote functions that can execute on nodes with
        appropriate resources (GPU, CPU, memory). The adapter handles
        converting Ray remote functions to framework-specific tool formats.

        Args:
            message: User message to process
            tools: Optional list of Ray remote functions for tool execution.
                   Each tool should be a Ray remote function decorated with
                   @ray.remote and optionally resource requirements.

        Returns:
            Response dictionary with at least a 'content' key containing
            the agent's response text. May include additional metadata.

        Example:
            >>> @ray.remote(num_gpus=1)
            >>> def generate_image(prompt: str):
            ...     return "image.png"
            >>>
            >>> result = ray.get(session.run.remote(
            ...     "Create an image",
            ...     tools=[generate_image]
            ... ))
        """
        logger.debug(
            f"Session {self.session_id}: Processing message: {message[:50]}..."
        )

        # Add user message to history
        self.messages.append({"role": "user", "content": message})

        # Execute agent via adapter
        try:
            response = await self.adapter.run(
                message=message, messages=self.messages, tools=tools or []
            )

            # Add assistant response to history
            if "content" not in response:
                raise ValueError(
                    "Adapter must return dict with 'content' key, "
                    f"got keys: {response.keys()}"
                )

            self.messages.append({"role": "assistant", "content": response["content"]})

            logger.debug(
                f"Session {self.session_id}: Response generated ({len(response['content'])} chars)"
            )

            return response

        except Exception as e:
            logger.error(f"Session {self.session_id}: Error during execution: {e}")
            raise

    def get_history(self) -> List[Dict[str, Any]]:
        """
        Get full conversation history.

        Returns:
            List of message dictionaries, each with 'role' and 'content' keys.
            Roles are 'user' or 'assistant'.

        Example:
            >>> history = ray.get(session.get_history.remote())
            >>> for msg in history:
            ...     print(f"{msg['role']}: {msg['content']}")
        """
        return self.messages

    def get_session_id(self) -> str:
        """
        Get session identifier.

        Returns:
            Session ID string
        """
        return self.session_id

    def clear_history(self) -> None:
        """
        Clear conversation history.

        This removes all messages but maintains the session and adapter.
        Use this to start a fresh conversation without creating a new session.
        """
        logger.info(f"Session {self.session_id}: Clearing history")
        self.messages = []
