import pytest
import ray
from ray import serve
from ray.llm._internal.serve.utils.dispatch import dispatch

# Define a simple deployment for testing
@serve.deployment(num_replicas=2)
class MockLLMDeployment:
    def __init__(self):
        self.reset_count = 0
        self.id = id(self)

    async def reset_prefix_cache(self):
        self.reset_count += 1
        return self.id, self.reset_count

    async def get_reset_count(self):
        return self.id, self.reset_count
        
    async def echo(self, msg, repeat=1):
        return f"{self.id}:{msg * repeat}"

@pytest.fixture(scope="module")
def serve_instance():
    # Start ray and serve once for the module
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()
    ray.shutdown()

@pytest.fixture
def mock_handle(serve_instance):
    # Ensure deployment is up and running.
    # serve.run waits for the deployment to be ready by default unless _blocking=False.
    handle = serve.run(MockLLMDeployment.bind(), name="mock_llm_app")
    return handle

@pytest.mark.asyncio
async def test_dispatch_basic(mock_handle):
    """Test basic dispatch without combine."""
    # We can use get_reset_count which doesn't modify state
    results = dispatch(mock_handle, "get_reset_count")
    
    assert len(results) == 2
    # Verify we got unique IDs back
    ids = set(r[0] for r in results)
    assert len(ids) == 2

@pytest.mark.asyncio
async def test_dispatch_with_combine(mock_handle):
    """Test dispatch with a combine function."""
    # First, increment count so we have something to sum
    dispatch(mock_handle, "reset_prefix_cache")
    
    def sum_counts(results):
        # results is list of (id, count)
        return sum(r[1] for r in results)
        
    # Get counts using dispatch and combine
    total_count = dispatch(mock_handle, "get_reset_count", combine=sum_counts)
    
    # We have 2 replicas.
    # If this is the first test run, counts are 1 each -> sum 2.
    # If other tests ran, counts might be higher.
    # But it must be at least 2 (since we just called reset_prefix_cache).
    assert total_count >= 2
    assert isinstance(total_count, int)

@pytest.mark.asyncio
async def test_dispatch_args_kwargs(mock_handle):
    """Test dispatch passing args and kwargs."""
    results = dispatch(mock_handle, "echo", args=("hello",), kwargs={"repeat": 2})
    
    assert len(results) == 2
    for r in results:
        # Format is "id:msg"
        msg_part = r.split(":")[1]
        assert msg_part == "hellohello"

@pytest.mark.asyncio
async def test_dispatch_callable_args(mock_handle):
    """Test dispatch with callable args generator."""
    
    def arg_gen(replica):
        # replica has unique_id or similar
        return (f"msg-{replica.unique_id}",)
        
    results = dispatch(mock_handle, "echo", args=arg_gen)
    
    assert len(results) == 2
    msgs = set()
    for r in results:
        msg_part = r.split(":")[1]
        msgs.add(msg_part)
    
    assert len(msgs) == 2
    for msg in msgs:
        assert msg.startswith("msg-")
