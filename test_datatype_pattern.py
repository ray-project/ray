"""Test DataType pattern matching logic"""
import sys
from pathlib import Path

# Add the python directory to the path to use local development version
sys.path.insert(0, str(Path(__file__).parent / "python"))

from ray.data.datatype import DataType
from ray.data.stats import _matches_dtype

print("Testing DataType pattern matching...")

# Test 1: Create temporal pattern
print("\n1. Creating temporal pattern...")
temporal_pattern = DataType.temporal_()
print(f"   Temporal pattern: {temporal_pattern}")
print(f"   Pattern category: {temporal_pattern._pattern_category}")
assert (
    temporal_pattern._pattern_category == "temporal"
), "Should have temporal pattern category"
print("   ✅ Temporal pattern created correctly")

# Test 2: Create specific temporal types
print("\n2. Creating specific temporal types...")
timestamp_type = DataType.temporal_("timestamp", unit="s")
print(f"   Timestamp: {timestamp_type}")
date32_type = DataType.temporal_("date32")
print(f"   Date32: {date32_type}")
time64_type = DataType.temporal_("time64", unit="ns")
print(f"   Time64: {time64_type}")
duration_type = DataType.temporal_("duration", unit="ms")
print(f"   Duration: {duration_type}")
print("   ✅ Specific temporal types created correctly")

# Test 3: Test is_temporal_type() method
print("\n3. Testing is_temporal_type() method...")
assert timestamp_type.is_temporal_type(), "timestamp should be temporal"
assert date32_type.is_temporal_type(), "date32 should be temporal"
assert time64_type.is_temporal_type(), "time64 should be temporal"
assert duration_type.is_temporal_type(), "duration should be temporal"
print("   ✅ is_temporal_type() works correctly")

# Test 4: Test that non-temporal types return False
print("\n4. Testing non-temporal types...")
int_type = DataType.int64()
string_type = DataType.string()
list_type = DataType.list_(DataType.int64())
assert not int_type.is_temporal_type(), "int64 should not be temporal"
assert not string_type.is_temporal_type(), "string should not be temporal"
assert not list_type.is_temporal_type(), "list should not be temporal"
print("   ✅ Non-temporal types correctly return False")

# Test 5: Test pattern matching with temporal types
print("\n5. Testing pattern matching...")
# Should match temporal types
assert _matches_dtype(
    timestamp_type, temporal_pattern
), "Pattern should match timestamp"
assert _matches_dtype(date32_type, temporal_pattern), "Pattern should match date32"
assert _matches_dtype(time64_type, temporal_pattern), "Pattern should match time64"
assert _matches_dtype(duration_type, temporal_pattern), "Pattern should match duration"
print("   ✅ Pattern matches temporal types")

# Test 6: Test pattern does NOT match non-temporal types
print("\n6. Testing pattern exclusion...")
assert not _matches_dtype(int_type, temporal_pattern), "Pattern should not match int64"
assert not _matches_dtype(
    string_type, temporal_pattern
), "Pattern should not match string"
assert not _matches_dtype(list_type, temporal_pattern), "Pattern should not match list"
print("   ✅ Pattern correctly excludes non-temporal types")

# Test 7: Test other pattern categories
print("\n7. Testing other pattern categories...")
list_pattern = DataType.list_()
assert (
    list_pattern._pattern_category == "list"
), "list pattern should have 'list' category"
assert _matches_dtype(list_type, list_pattern), "list pattern should match list type"
assert not _matches_dtype(
    int_type, list_pattern
), "list pattern should not match int type"

struct_pattern = DataType.struct()
struct_type = DataType.struct([("x", DataType.int64())])
assert (
    struct_pattern._pattern_category == "struct"
), "struct pattern should have 'struct' category"
assert _matches_dtype(
    struct_type, struct_pattern
), "struct pattern should match struct type"
assert not _matches_dtype(
    int_type, struct_pattern
), "struct pattern should not match int type"

map_pattern = DataType.map_()
map_type = DataType.map_(DataType.string(), DataType.int64())
assert map_pattern._pattern_category == "map", "map pattern should have 'map' category"
assert _matches_dtype(map_type, map_pattern), "map pattern should match map type"
assert not _matches_dtype(
    int_type, map_pattern
), "map pattern should not match int type"
print("   ✅ All pattern categories work correctly")

# Test 8: Test pattern equality
print("\n8. Testing pattern equality...")
temporal_pattern2 = DataType.temporal_()
assert temporal_pattern == temporal_pattern2, "Same temporal patterns should be equal"
assert hash(temporal_pattern) == hash(
    temporal_pattern2
), "Same patterns should have same hash"

list_pattern2 = DataType.list_()
assert (
    temporal_pattern != list_pattern2
), "Different pattern categories should not be equal"
assert hash(temporal_pattern) != hash(
    list_pattern2
), "Different patterns should have different hashes"
print("   ✅ Pattern equality and hashing work correctly")

print("\n" + "=" * 60)
print("✅ All tests passed! Pattern matching is working correctly!")
print("=" * 60)
