#!/usr/bin/env python3
"""
Throwaway script to generate and write wide schema datasets to S3.
This pre-generates datasets so the actual benchmark only needs to read/process them.
"""

import argparse
import numpy as np
import ray
import pyarrow as pa
import pandas as pd


def generate_long_column_name(col_idx: int, prefix: str = "col") -> str:
    """Generate a 1000-character column name."""
    base_name = f"{prefix}_{col_idx}_"
    # Fill remaining characters with repeating pattern to reach exactly 1000 chars
    remaining_chars = 500 - len(base_name)
    if remaining_chars > 0:
        # Create a repeating pattern using alphanumeric characters
        pattern = "abcdefghijklmnopqrstuvwxyz0123456789_"
        filler = (pattern * ((remaining_chars // len(pattern)) + 1))[:remaining_chars]
        return base_name + filler
    else:
        # If base_name is already >= 1000 chars, truncate to exactly 1000
        return base_name[:1000]


def create_simple_data(num_rows: int, num_columns: int) -> list:
    """Create simple int64 columns with 1000-character column names."""
    print(f"Creating simple dataset with {num_rows} rows and {num_columns} columns...")
    
    # Create a single sample record first
    print("Creating sample record...")
    sample_record = {}
    for col_idx in range(num_columns):
        long_col_name = generate_long_column_name(col_idx, "col")
        base_value = int(np.random.randint(0, 100000, dtype=np.int64))
        sample_record[long_col_name] = base_value
    
    # Repeat the sample record for all rows
    print(f"Repeating sample record {num_rows:,} times...")
    return [sample_record for _ in range(num_rows)]


def create_tensor_data(num_rows: int, num_columns: int) -> list:
    """Create mixed fixed and variable tensor columns with 1000-character column names."""
    print(f"Creating tensor dataset with {num_rows} rows and {num_columns} columns...")
    
    # Create a single sample record first
    print("Creating sample record...")
    sample_record = {}
    for col_idx in range(num_columns):
        long_col_name = generate_long_column_name(col_idx, "tensor")
        if col_idx % 2 == 0:
            # Fixed 2x2 float32 tensors
            tensor = np.random.random((2, 2)).astype(np.float32)
        else:
            # Variable shaped tensors (1x1 to 5x5)
            size1, size2 = np.random.randint(1, 6, 2)
            tensor = np.random.random((size1, size2)).astype(np.float32)
        sample_record[long_col_name] = tensor
    
    # Repeat the sample record for all rows
    print(f"Repeating sample record {num_rows:,} times...")
    return [sample_record.copy() for _ in range(num_rows)]


def create_object_data(num_rows: int, num_columns: int) -> list:
    """Create Python object columns with 1000-character column names."""
    print(f"Creating object dataset with {num_rows} rows and {num_columns} columns...")
    
    # Create a single sample record first
    print("Creating sample record...")
    sample_record = {}
    for col_idx in range(num_columns):
        long_col_name = generate_long_column_name(col_idx, "obj")
        obj_type = np.random.randint(0, 4)
        base_val = np.random.randint(0, 1000)
        
        if obj_type == 0:
            obj = {"key": f"value_{base_val}"}
        elif obj_type == 1:
            obj = [1, 2, 3, base_val]
        elif obj_type == 2:
            obj = set([1, 2, 3, base_val])
        else:
            obj = f"object_string_{base_val}"
        
        sample_record[long_col_name] = obj
    
    # Repeat the sample record for all rows
    print(f"Repeating sample record {num_rows:,} times...")
    return [sample_record.copy() for _ in range(num_rows)]


def create_nested_struct_data(num_rows: int, num_columns: int) -> list:
    """Create nested struct columns with 1000-character column names."""
    print(f"Creating nested struct dataset with {num_rows} rows and {num_columns} columns...")
    
    # Create a single sample record first
    print("Creating sample record...")
    sample_record = {}
    for col_idx in range(num_columns):
        long_col_name = generate_long_column_name(col_idx, "struct")
        base_x = int(np.random.randint(0, 1000))
        base_str = np.random.randint(0, 100)
        base_z = float(np.random.random() * 1000)
        
        struct = {
            "x": base_x,
            "y": f"string_{base_str}",
            "z": base_z
        }
        sample_record[long_col_name] = struct
    
    # Repeat the sample record for all rows
    print(f"Repeating sample record {num_rows:,} times...")
    return [sample_record.copy() for _ in range(num_rows)]


def estimate_bytes_per_row(data_type: str, num_columns: int) -> int:
    """Estimate bytes per row for each data type."""
    if data_type == "primitives":
        # int64 = 8 bytes per column
        return num_columns * 8
    elif data_type == "tensors":
        # Mix of fixed 2x2 float32 (16 bytes) and variable ~3x3 float32 (36 bytes)
        # Average: ~26 bytes per column
        return num_columns * 26
    elif data_type == "objects":
        # Python objects (pickled) - estimate ~150 bytes per column
        return num_columns * 150
    elif data_type == "nested_structs":
        # int32 (4) + string (~20) + float64 (8) = ~32 bytes per column
        return num_columns * 32
    else:
        return num_columns * 8  # default


def calculate_rows_for_target_size(data_type: str, num_columns: int, target_gb: float) -> int:
    """Calculate number of rows needed to reach target size in GB."""
    target_bytes = target_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    bytes_per_row = estimate_bytes_per_row(data_type, num_columns)
    return int(target_bytes // bytes_per_row)


def main():
    parser = argparse.ArgumentParser(description="Generate wide schema datasets for benchmarking")
    parser.add_argument("--target-size-gb", type=float, default=5.0, help="Target size in GB per dataset")
    parser.add_argument("--num-columns", type=int, default=5000, help="Number of columns per dataset")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Chunk size for writing")
    
    args = parser.parse_args()
    
    # Initialize Ray
    ray.init()
    
    base_path = "s3://ray-benchmark-data-internal-us-west-2/wide_schema"
    
    # Data generators (removed mixed)
    generators = {
        "primitives": create_simple_data,
        "tensors": create_tensor_data,
        "objects": create_object_data,
        "nested_structs": create_nested_struct_data,
    }
    
    for data_type, generator_func in generators.items():
        print(f"\n=== Generating {data_type} dataset ===")
        
        # Calculate rows needed for target size
        target_rows = calculate_rows_for_target_size(data_type, args.num_columns, args.target_size_gb)
        bytes_per_row = estimate_bytes_per_row(data_type, args.num_columns)
        estimated_size_gb = (target_rows * bytes_per_row) / (1024 * 1024 * 1024)
        
        print(f"Target size: {args.target_size_gb:.1f} GB")
        print(f"Calculated rows needed: {target_rows:,}")
        print(f"Estimated bytes per row: {bytes_per_row:,}")
        print(f"Estimated final size: {estimated_size_gb:.2f} GB")
        
        output_path = f"{base_path}/{data_type}"
        
        # Generate data in chunks to manage memory, then union and write as one dataset
        chunk_datasets = []
        total_rows_written = 0
        chunk_num = 0
        
        print("Generating data chunks...")
        while total_rows_written < target_rows:
            current_chunk_size = min(args.chunk_size, target_rows - total_rows_written)
            
            print(f"Generating chunk {chunk_num + 1} with {current_chunk_size} rows...")
            data = generator_func(current_chunk_size, args.num_columns)
            
            # Convert to Ray Dataset 
            print(f"Done chunk {chunk_num + 1} with {current_chunk_size} rows...")
            df = pd.DataFrame(data)

            print(f"Done Creating DF {chunk_num + 1} with {current_chunk_size} rows...")
            chunk_ds = ray.data.from_pandas([df])
            print(f"Done From Pandas {chunk_num + 1} with {current_chunk_size} rows...")
            chunk_datasets.append(chunk_ds)
            
            total_rows_written += current_chunk_size
            chunk_num += 1
            
            print(f"Generated {total_rows_written:,}/{target_rows:,} rows")
        
        # Union all chunks into one dataset
        print("Unioning chunks...")
        if len(chunk_datasets) == 1:
            final_ds = chunk_datasets[0]
        else:
            # Use union to combine datasets
            final_ds = chunk_datasets[0].union(*chunk_datasets[1:])
        
        # Calculate max_rows_per_file to ensure we get at least 10 files
        # Target: at least 10 files, so each file should have at most target_rows/10 rows
        max_rows_per_file = max(1000, target_rows // 20)  # Use //20 to get ~20 files, minimum 1000 rows
        
        print(f"Writing to {output_path} with max_rows_per_file={max_rows_per_file:,}...")
        # Write the complete dataset with controlled file sizing
        final_ds.write_parquet(
            output_path,
            # compression="snappy",
            max_rows_per_file=max_rows_per_file,
            mode="overwrite",
            try_create_dir=True
        )
        
        # Estimate number of files created
        estimated_files = max(1, target_rows // max_rows_per_file)
        print(f"✅ Completed {data_type} dataset: {output_path}")
        print(f"   Estimated {estimated_files} parquet files (min_rows_per_file={max_rows_per_file:,})")
    
    print(f"\n🎉 All datasets generated successfully!")
    print(f"Base path: {base_path}")
    print(f"Target size per dataset: {args.target_size_gb:.1f} GB")
    print(f"Columns per dataset: {args.num_columns:,}")
    
    ray.shutdown()


if __name__ == "__main__":
    main()
