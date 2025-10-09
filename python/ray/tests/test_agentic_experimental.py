"""Tests for ray.agentic.experimental module."""

import pytest

import ray
from ray.agentic.experimental import AgentAdapter, AgentSession
from ray.agentic.experimental.adapters import _MockAdapter as MockAdapter


@pytest.fixture
def ray_start():
    """Start Ray for testing."""
    ray.init(ignore_reinit_error=True, num_cpus=4)
    yield
    ray.shutdown()


def test_agent_session_creation(ray_start):
    """Test basic agent session creation."""
    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test_123", adapter=adapter)

    # Verify session was created
    assert session is not None

    # Verify we can get session ID
    session_id = ray.get(session.get_session_id.remote())
    assert session_id == "test_123"


def test_agent_session_run_without_tools(ray_start):
    """Test agent execution without tools."""
    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Run agent with simple message
    result = ray.get(session.run.remote("Hello, how are you?"))

    # Verify response structure
    assert "content" in result
    assert "Hello, how are you?" in result["content"]


def test_agent_session_run_with_tools(ray_start):
    """Test agent execution with Ray remote tools."""

    @ray.remote
    def test_tool_1():
        return "result_1"

    @ray.remote
    def test_tool_2():
        return "result_2"

    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Run agent with tools
    result = ray.get(
        session.run.remote("Test message", tools=[test_tool_1, test_tool_2])
    )

    # Verify response includes tool results
    assert "content" in result
    assert "tool_results" in result
    assert len(result["tool_results"]) == 2
    assert "result_1" in result["tool_results"]
    assert "result_2" in result["tool_results"]


def test_agent_session_conversation_history(ray_start):
    """Test conversation history is maintained."""
    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Send multiple messages
    ray.get(session.run.remote("First message"))
    ray.get(session.run.remote("Second message"))
    ray.get(session.run.remote("Third message"))

    # Get history
    history = ray.get(session.get_history.remote())

    # Verify history structure
    assert len(history) == 6  # 3 user + 3 assistant messages
    assert history[0]["role"] == "user"
    assert history[0]["content"] == "First message"
    assert history[1]["role"] == "assistant"
    assert history[2]["role"] == "user"
    assert history[2]["content"] == "Second message"


def test_agent_session_clear_history(ray_start):
    """Test clearing conversation history."""
    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Send messages
    ray.get(session.run.remote("Message 1"))
    ray.get(session.run.remote("Message 2"))

    # Clear history
    ray.get(session.clear_history.remote())

    # Verify history is empty
    history = ray.get(session.get_history.remote())
    assert len(history) == 0


def test_distributed_tool_execution(ray_start):
    """Test tools execute as distributed Ray tasks."""

    @ray.remote(num_cpus=1)
    def cpu_tool():
        import os

        return f"Executed on PID {os.getpid()}"

    @ray.remote(num_cpus=1)
    def another_cpu_tool():
        import os

        return f"Executed on PID {os.getpid()}"

    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Execute with multiple tools
    result = ray.get(session.run.remote("Test", tools=[cpu_tool, another_cpu_tool]))

    # Verify tools executed (potentially on different workers)
    assert "tool_results" in result
    assert len(result["tool_results"]) == 2
    # Both should have executed and returned PID info
    assert all("PID" in str(r) for r in result["tool_results"])


def test_adapter_must_return_content_key(ray_start):
    """Test that adapter must return dict with 'content' key."""

    class BadAdapter(AgentAdapter):
        """Adapter that returns invalid response."""

        async def run(self, message, messages, tools):
            return {"response": "missing content key"}

    adapter = BadAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # This should raise ValueError
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get(session.run.remote("Test"))

    # Verify error message mentions 'content' key
    assert "content" in str(exc_info.value).lower()


def test_tool_with_resource_requirements(ray_start):
    """Test tools with specific resource requirements."""

    @ray.remote(num_cpus=2, memory=1024 * 1024 * 1024)
    def resource_intensive_tool():
        return "Processed with 2 CPUs and 1GB memory"

    adapter = MockAdapter()
    session = AgentSession.remote(session_id="test", adapter=adapter)

    # Execute tool with resource requirements
    result = ray.get(session.run.remote("Test", tools=[resource_intensive_tool]))

    # Verify tool executed successfully
    assert "tool_results" in result
    assert len(result["tool_results"]) == 1
    assert "Processed" in result["tool_results"][0]


def test_multiple_sessions_isolated(ray_start):
    """Test that multiple sessions maintain separate state."""
    adapter = MockAdapter()

    # Create two sessions
    session1 = AgentSession.remote(session_id="user_1", adapter=adapter)
    session2 = AgentSession.remote(session_id="user_2", adapter=adapter)

    # Send different messages to each
    ray.get(session1.run.remote("Message to session 1"))
    ray.get(session2.run.remote("Message to session 2"))

    # Verify histories are separate
    history1 = ray.get(session1.get_history.remote())
    history2 = ray.get(session2.get_history.remote())

    assert len(history1) == 2  # 1 user + 1 assistant
    assert len(history2) == 2  # 1 user + 1 assistant
    assert history1[0]["content"] == "Message to session 1"
    assert history2[0]["content"] == "Message to session 2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
