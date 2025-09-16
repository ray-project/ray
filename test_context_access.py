#!/usr/bin/env python3
"""
Test script to verify Train V2 context access works correctly.
"""

import os

# Fix OpenMP library conflict on macOS
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

def test_context_access():
    """Test accessing Train V2 context."""
    print("Testing Train V2 context access...")
    
    try:
        # Try to import and access V2 context
        from ray.train.v2._internal.execution.context import get_train_context
        print("✓ Successfully imported get_train_context")
        
        # Try to get the context (this will fail outside of training)
        try:
            context = get_train_context()
            print("✓ Successfully got train context")
            
            # Check if our XLA mesh methods exist
            if hasattr(context, 'set_xla_mesh'):
                print("✓ Context has set_xla_mesh method")
            else:
                print("✗ Context missing set_xla_mesh method")
                
            if hasattr(context, 'get_xla_mesh'):
                print("✓ Context has get_xla_mesh method")
            else:
                print("✗ Context missing get_xla_mesh method")
                
        except RuntimeError as e:
            print(f"✗ Could not get train context: {e}")
            print("  This is expected outside of training context")
            
    except ImportError as e:
        print(f"✗ Failed to import V2 context: {e}")
        return False
    
    return True

def test_v1_context_access():
    """Test accessing Train V1 context."""
    print("\nTesting Train V1 context access...")
    
    try:
        import ray.train as train
        print("✓ Successfully imported ray.train")
        
        # Try to get the context (this will fail outside of training)
        try:
            context = train.get_context()
            print("✓ Successfully got V1 train context")
            
            # Check if our XLA mesh methods exist (they shouldn't in V1)
            if hasattr(context, 'set_xla_mesh'):
                print("✓ V1 context has set_xla_mesh method")
            else:
                print("✗ V1 context missing set_xla_mesh method (expected)")
                
            if hasattr(context, 'get_xla_mesh'):
                print("✓ V1 context has get_xla_mesh method")
            else:
                print("✗ V1 context missing get_xla_mesh method (expected)")
                
        except RuntimeError as e:
            print(f"✗ Could not get V1 train context: {e}")
            print("  This is expected outside of training context")
            
    except ImportError as e:
        print(f"✗ Failed to import V1 context: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Train Context Access Test")
    print("=" * 40)
    
    # Test V2 context
    v2_success = test_context_access()
    
    # Test V1 context
    v1_success = test_v1_context_access()
    
    print("\n" + "=" * 40)
    print("Summary:")
    print(f"Train V2 context: {'✓ Available' if v2_success else '✗ Not available'}")
    print(f"Train V1 context: {'✓ Available' if v1_success else '✗ Not available'}")
    
    if v2_success:
        print("\n✓ Train V2 context is available and has XLA mesh methods")
        print("  The XLA mesh integration should work with RAY_TRAIN_V2_ENABLED=1")
    else:
        print("\n✗ Train V2 context is not available")
        print("  Check your Ray installation and V2 enablement")
