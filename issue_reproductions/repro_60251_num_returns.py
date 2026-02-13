"""
Reproduction for GitHub Issue #60251:
[Core] Incorrect Error Message When Task Return 1 value and num_returns > 1

When a @ray.remote task is declared with num_returns=2 but returns a single
scalar value, users get a confusing TypeError instead of a helpful message.

Expected: "Task returned 1 objects, but num_returns=2."
Actual:   "TypeError: object of type 'int' has no len()"
"""
import ray

ray.init()

@ray.remote(num_returns=2)
def f():
    return 1  # Returns single int, but num_returns=2 expects a tuple/list of 2

try:
    x, y = ray.get(f.remote())
    print("BUG NOT REPRODUCED: Got values without error")
except TypeError as e:
    if "has no len()" in str(e):
        print(f"BUG REPRODUCED: Got confusing TypeError: {e}")
        print("Expected a clear message like: 'Task returned 1 objects, but num_returns=2'")
    else:
        print(f"Got unexpected TypeError: {e}")
except ValueError as e:
    if "Task returned" in str(e) and "num_returns" in str(e):
        print(f"BUG FIXED: Got proper error message: {e}")
    else:
        print(f"Got unexpected ValueError: {e}")
except Exception as e:
    print(f"Got unexpected error ({type(e).__name__}): {e}")
finally:
    ray.shutdown()
