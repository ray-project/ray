import ray
import warnings

ray.init()

# Test case that should trigger warning
@ray.remote(num_returns=0)
def returns_value():
    print("Function executing")
    return 123  # This should trigger warning

# Test case that shouldn't trigger warning
@ray.remote(num_returns=0)
def returns_none():
    print("Function executing")
    return None  # This is fine

# Capture warnings to verify
with warnings.catch_warnings(record=True) as w:
    returns_value.remote()
    returns_none.remote()
    
    # Verify warning was raised for returns_value
    assert len(w) == 1
    assert "num_returns=0" in str(w[0].message)