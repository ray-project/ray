"""
Reproduction for GitHub Issue #59582:
[Core] Inconsistent context manager behavior between ray.init modes

The issue claims that:
- Local mode (ray.init()): exiting context shuts down Ray entirely,
  making the context non-reenterable
- Client mode (ray.init("ray://...")): exiting context leaves cluster
  running and context IS reenterable

We test local mode behavior here (client mode requires a separate Ray server).
"""
import ray
import sys

print("=" * 60)
print("Test 1: RayContext (local mode) - context manager behavior")
print("=" * 60)

# Start Ray and get the context
ctx = ray.init()
print(f"Context type: {type(ctx).__name__}")
print(f"Ray is initialized: {ray.is_initialized()}")

# Use as context manager
print("\nEntering context manager (first time)...")
with ctx:
    print(f"  Inside context: ray.is_initialized() = {ray.is_initialized()}")
    nodes = ray.nodes()
    print(f"  Number of nodes: {len(nodes)}")

print(f"After exiting context: ray.is_initialized() = {ray.is_initialized()}")

# Try to reenter the same context
print("\nAttempting to reenter the same context (second time)...")
try:
    with ctx:
        print(f"  Inside context: ray.is_initialized() = {ray.is_initialized()}")
        nodes = ray.nodes()
        print(f"  Number of nodes: {len(nodes)}")
    print("  Context was reenterable!")
    reenterable = True
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {e}")
    reenterable = False

print()
print("=" * 60)
print("Test 2: Verify shutdown happened on first __exit__")
print("=" * 60)
print(f"ray.is_initialized() = {ray.is_initialized()}")

if not ray.is_initialized() and not reenterable:
    print("\nBUG CONFIRMED: ray.init() context manager shuts down Ray on __exit__")
    print("and the context is NOT reenterable.")
    print()
    print("This differs from Ray Client mode (ray://...) where:")
    print("  - __exit__ only disconnects the client, not the cluster")
    print("  - The context IS reenterable via _context_to_restore swapping")
    print()
    print("The behavioral inconsistency is real, but the maintainer argues")
    print("this is by design: local mode owns the cluster (so shuts it down),")
    print("while client mode connects to a remote cluster (so only disconnects).")
elif ray.is_initialized():
    print("\nContext manager did NOT shut down Ray - behavior may have changed.")
else:
    print("\nRay shut down but context was reenterable - unexpected behavior.")

# Clean up
if ray.is_initialized():
    ray.shutdown()
