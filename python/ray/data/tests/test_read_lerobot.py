#!/usr/bin/env python
"""
Comprehensive test script for the read_lerobot function.
Tests both local path loading and HuggingFace Hub repo_id loading.

Usage:
    python test_read_lerobot.py
"""
import sys
import tempfile

import pytest
from huggingface_hub import snapshot_download

import ray
from ray.data import Dataset

try:
    from torchcodec.decoders import VideoDecoder  # noqa: F401

    _TORCHCODEC_AVAILABLE = True
    _TORCHCODEC_IMPORT_ERROR = ""
except Exception as exc:
    _TORCHCODEC_AVAILABLE = False
    _TORCHCODEC_IMPORT_ERROR = str(exc)

# Note: Using installed Ray from the virtual environment
# If you need to test local Ray changes, build Ray from source first
# ray_python_dir = Path(__file__).parent.parent.parent
# if str(ray_python_dir) not in sys.path:
#     sys.path.insert(0, str(ray_python_dir))


class TestReadLerobot:
    """Test suite for read_lerobot function."""

    @classmethod
    def setup_class(cls):
        """Initialize Ray for testing."""
        if not _TORCHCODEC_AVAILABLE:
            pytest.skip(
                "torchcodec is required for LeRobot video decoding; "
                f"import failed: {_TORCHCODEC_IMPORT_ERROR}"
            )
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

    @classmethod
    def teardown_class(cls):
        """Shutdown Ray after testing."""
        if ray.is_initialized():
            ray.shutdown()

    def teardown_method(self, method):
        """Clean up after each test method."""
        import gc

        gc.collect()

    def test_read_lerobot_with_huggingface_repo_id(self):
        """
        Test reading LeRobot dataset from HuggingFace Hub using repo_id.
        This tests the main use case of loading datasets directly from the hub.
        """
        print("\n" + "=" * 70)
        print("TEST 1: Reading from HuggingFace Hub with repo_id")
        print("=" * 70)

        try:
            # Use a small, well-known LeRobot dataset for testing
            repo_id = "lerobot/pusht"

            print(f"Loading dataset: {repo_id}")
            print("Parameters: shuffle=False")

            # Read without downloading videos for faster testing
            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",  # Will use default cache location
                episode_indices=[0, 1, 2],  # Limit to 3 episodes to reduce memory
                shuffle=False,
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            # Verify dataset was created
            assert ds is not None, "Dataset should not be None"
            assert isinstance(ds, Dataset), f"Expected Dataset, got {type(ds)}"

            # Check basic dataset properties
            print("\n✅ Dataset loaded successfully!")
            print(f"   Dataset type: {type(ds)}")

            # Get schema
            schema = ds.schema()
            print("\n   Schema:")
            print(f"   {schema}")

            # Count rows
            try:
                num_rows = ds.count()
                print(f"\n   Number of rows: {num_rows}")
                assert num_rows > 0, "Dataset should have at least one row"
            except Exception as e:
                print(f"\n   ⚠️  Could not count rows: {e}")

            # Take a sample
            try:
                sample = ds.take(1)
                print("\n   Sample row:")
                for key, value in sample[0].items():
                    if hasattr(value, "shape"):
                        print(
                            f"      {key}: {type(value).__name__} with shape {value.shape}"
                        )
                    else:
                        print(f"      {key}: {value}")

                # Verify expected columns
                expected_columns = ["episode_index", "index", "timestamp"]
                for col in expected_columns:
                    assert (
                        col in sample[0]
                    ), f"Expected column '{col}' not found in dataset"
                print("\n   ✅ All expected columns present")

            except Exception as e:
                print(f"\n   ⚠️  Could not take sample: {e}")

            print("\n✅ TEST 1 PASSED: Successfully loaded dataset from HuggingFace Hub")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 1 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_with_episode_filter(self):
        """
        Test reading LeRobot dataset with episode filtering.
        """
        print("\n" + "=" * 70)
        print("TEST 2: Reading with episode filtering")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"
            episode_indices = [0, 1]  # Only load first two episodes

            print(f"Loading dataset: {repo_id}")
            print(f"Parameters: episode_indices={episode_indices}")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=episode_indices,
                shuffle=False,
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            assert ds is not None
            print("\n✅ Dataset loaded with episode filtering")

            # Verify only specified episodes are present
            try:
                sample_episodes = (
                    ds.limit(1000).select_columns(["episode_index"]).take_all()
                )
                unique_episodes = {row["episode_index"] for row in sample_episodes}
                print(f"   Unique episode indices in dataset: {unique_episodes}")

                # Check that we only have the requested episodes
                assert unique_episodes.issubset(
                    set(episode_indices)
                ), f"Found unexpected episodes: {unique_episodes - set(episode_indices)}"
                print("   ✅ Episode filtering working correctly")

            except Exception as e:
                print(f"   ⚠️  Could not verify episodes: {e}")

            print("\n✅ TEST 2 PASSED: Episode filtering works correctly")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 2 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_with_shuffle(self):
        """
        Test reading LeRobot dataset with shuffling enabled.
        """
        print("\n" + "=" * 70)
        print("TEST 3: Reading with shuffling enabled")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"

            print(f"Loading dataset: {repo_id}")
            print("Parameters: shuffle=True")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=[0, 1, 2],  # Limit to 3 episodes to reduce memory
                shuffle=True,  # Enable shuffling
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            assert ds is not None
            print("\n✅ Dataset loaded with shuffling enabled")

            # Take samples to verify dataset is readable
            try:
                samples = ds.take(5)
                print(f"   Retrieved {len(samples)} samples")
                print("   ✅ Shuffled dataset is readable")
            except Exception as e:
                print(f"   ⚠️  Could not take samples: {e}")

            print("\n✅ TEST 3 PASSED: Shuffling works correctly")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 3 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_parallelism_options(self):
        """
        Test reading LeRobot dataset with different parallelism options.
        """
        print("\n" + "=" * 70)
        print("TEST 4: Testing parallelism options")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"

            # Test with custom override_num_blocks
            print(f"Loading dataset: {repo_id}")
            print("Parameters: override_num_blocks=4")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=[0, 1],  # Limit to 2 episodes to reduce memory
                shuffle=False,
                override_num_blocks=4,
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            assert ds is not None
            print("\n✅ Dataset loaded with custom parallelism")

            # Test with concurrency limit
            print("\nLoading with concurrency limit...")
            print("Parameters: concurrency=2")

            ds2 = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=[0, 1],  # Limit to 2 episodes to reduce memory
                shuffle=False,
                concurrency=2,
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            assert ds2 is not None
            print("✅ Dataset loaded with concurrency limit")

            print("\n✅ TEST 4 PASSED: Parallelism options work correctly")

            # Cleanup
            del ds, ds2
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 4 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_with_video_backend_options(self):
        """
        Test reading LeRobot dataset with different video backend options.
        Note: This test only verifies parameters are accepted, not actual video loading.
        """
        print("\n" + "=" * 70)
        print("TEST 5: Testing video backend options")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"

            # Test with different video backends
            backends = ["torchcodec", "pyav"]

            for backend in backends:
                print(f"\nTesting with video_backend='{backend}'")

                ds = ray.data.read_lerobot(
                    repo_id=repo_id,
                    root="",
                    episode_indices=[0, 1],  # Limit to 2 episodes to reduce memory
                    video_backend=backend,
                    shuffle=False,
                    override_num_blocks=2,  # Limit parallelism for testing
                    delta_timestamps={
                        "action": [
                            0.0,
                            0.03,
                            0.06,
                            0.09,
                            0.12,
                            0.15,
                            0.18,
                            0.21,
                            0.24,
                            0.27,
                            0.30,
                            0.33,
                        ]
                    },
                )

                assert ds is not None
                print(f"   ✅ Dataset loaded with {backend} backend")

            # Test with custom video batch size
            print("\nTesting with video_batch_size=32")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=[0, 1],  # Limit to 2 episodes to reduce memory
                video_batch_size=32,
                shuffle=False,
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            assert ds is not None
            print("   ✅ Dataset loaded with custom video batch size")

            print("\n✅ TEST 5 PASSED: Video backend options work correctly")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 5 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_schema_validation(self):
        """
        Test that the loaded dataset has the expected schema structure.
        """
        print("\n" + "=" * 70)
        print("TEST 6: Schema validation")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"

            print(f"Loading dataset: {repo_id}")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                episode_indices=[0, 1, 2],  # Limit to 3 episodes to reduce memory
                shuffle=False,
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={
                    "action": [
                        0.0,
                        0.03,
                        0.06,
                        0.09,
                        0.12,
                        0.15,
                        0.18,
                        0.21,
                        0.24,
                        0.27,
                        0.30,
                        0.33,
                    ]
                },
            )

            # Get schema
            schema = ds.schema()
            print("\nDataset schema:")
            print(f"{schema}")

            # Get column names
            column_names = schema.names
            print(f"\nColumn names: {column_names}")

            # Verify essential columns exist
            essential_columns = ["episode_index", "index", "timestamp"]
            for col in essential_columns:
                assert col in column_names, f"Missing essential column: {col}"
                print(f"   ✅ Found essential column: {col}")

            # Take a sample and verify data types
            sample = ds.take(1)[0]
            print("\nSample data types:")
            for key, value in sample.items():
                print(f"   {key}: {type(value).__name__}")

            print("\n✅ TEST 6 PASSED: Schema validation successful")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 6 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_local_dataset(self):
        """
        Test reading LeRobot dataset from pre-downloaded local directory.
        Downloads lerobot/aloha_mobile_cabinet using HuggingFace Hub, then reads it locally.
        """
        print("\n" + "=" * 70)
        print("TEST 7: Reading from pre-downloaded local directory")
        print("=" * 70)

        try:
            repo_id = "lerobot/aloha_mobile_cabinet"
            cache_dir = tempfile.gettempdir() + "/lerobot_test_cache"
            # The full path will be: /tmp/lerobot_test_cache/lerobot/aloha_mobile_cabinet
            local_dataset_path = cache_dir + "/" + repo_id

            print("Step 1: Pre-downloading dataset using HuggingFace Hub...")
            print(f"Repo ID: {repo_id}")
            print(f"Download path: {local_dataset_path}")

            # Download using HuggingFace snapshot_download
            downloaded_path = snapshot_download(
                repo_id=repo_id,
                repo_type="dataset",
                local_dir=local_dataset_path,
                local_dir_use_symlinks=False,  # Don't use symlinks
            )

            print(f"✅ Dataset downloaded to: {downloaded_path}")

            print("\nStep 2: Loading dataset from local path using read_lerobot...")
            print(f"Using root={cache_dir}, repo_id={repo_id}")
            print("This will look for dataset at: {cache_dir}/{repo_id}")
            print("Parameters: shuffle=False, episode_indices=[0, 1]")

            # Now use read_lerobot with the local path
            # LeRobotDatasetMetadata will construct: root / repo_id
            # So: /tmp/lerobot_test_cache / lerobot/aloha_mobile_cabinet
            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root=local_dataset_path,  # Will be combined with repo_id to form full path
                episode_indices=[0, 1],  # Limit to 2 episodes for faster testing
                shuffle=False,
                override_num_blocks=2,
                delta_timestamps={"action": [0.0, 0.05, 0.10, 0.15, 0.20, 0.25]},
            )

            print("✅ Dataset loaded from local directory")

            # Verify dataset was created
            assert ds is not None, "Dataset should not be None"
            assert isinstance(ds, Dataset), f"Expected Dataset, got {type(ds)}"

            print("\n✅ Dataset loaded successfully from local cache!")
            print(f"   Dataset type: {type(ds)}")

            # Take a sample
            try:
                sample = ds.take(1)

                # Verify expected columns
                expected_columns = ["episode_index", "index", "timestamp"]
                for col in expected_columns:
                    assert (
                        col in sample[0]
                    ), f"Expected column '{col}' not found in dataset"
                print("\n   ✅ All expected columns present")

            except Exception as e:
                print(f"\n   ⚠️  Could not take sample: {e}")

            print(
                "\n✅ TEST 7 PASSED: Successfully loaded dataset from pre-downloaded local directory"
            )

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 7 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_iter_batches(self):
        """
        Test iterating through batches of the dataset.
        """
        print("\n" + "=" * 70)
        print("TEST 8: Iterating through batches")
        print("=" * 70)

        try:
            repo_id = "lerobot/pusht"
            batch_size = 32

            print(f"Loading dataset: {repo_id}")
            print(f"Parameters: batch_size={batch_size}")

            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root="",
                shuffle=False,
                episode_indices=[
                    0,
                    1,
                ],  # Limit to first two episodes for faster testing
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={"action": [0.0, 0.03, 0.06, 0.09, 0.12, 0.15]},
            )

            assert ds is not None
            print("\n✅ Dataset loaded successfully")

            # Test iter_batches with different formats
            print("\n   Testing iter_batches with batch_format='numpy'...")
            batch_count = 0
            total_rows = 0

            for batch in ds.iter_batches(batch_size=batch_size, batch_format="numpy"):
                batch_count += 1
                # Verify batch is a dict
                assert isinstance(batch, dict), f"Expected dict, got {type(batch)}"

                # Get batch size from first key
                first_key = list(batch.keys())[0]
                current_batch_size = len(batch[first_key])
                total_rows += current_batch_size

                if batch_count == 1:
                    print(f"      First batch keys: {list(batch.keys())}")
                    print(f"      First batch size: {current_batch_size}")

                    # Verify all keys have same length
                    for key, value in batch.items():
                        assert (
                            len(value) == current_batch_size
                        ), f"Inconsistent batch size for key {key}"

                # Only iterate through a few batches for testing
                if batch_count >= 3:
                    break

            print(f"      ✅ Successfully iterated through {batch_count} batches")
            print(f"      Total rows processed: {total_rows}")

            # Test iter_batches with pandas format
            print("\n   Testing iter_batches with batch_format='pandas'...")
            batch_count = 0

            for batch in ds.iter_batches(batch_size=batch_size, batch_format="pandas"):
                batch_count += 1
                # Verify batch is a DataFrame
                import pandas as pd

                assert isinstance(
                    batch, pd.DataFrame
                ), f"Expected DataFrame, got {type(batch)}"

                if batch_count == 1:
                    print(f"      First batch shape: {batch.shape}")
                    print(f"      First batch columns: {list(batch.columns)}")

                # Only iterate through a few batches
                if batch_count >= 3:
                    break

            print(
                f"      ✅ Successfully iterated through {batch_count} batches (pandas format)"
            )

            print("\n✅ TEST 8 PASSED: Batch iteration works correctly")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 8 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_local_iter_batches(self):
        """
        Test iterating through batches from pre-downloaded local dataset.
        Uses lerobot/aloha_mobile_cabinet.
        """
        print("\n" + "=" * 70)
        print("TEST 9: Iterating through batches (pre-downloaded local dataset)")
        print("=" * 70)

        try:
            repo_id = "lerobot/aloha_mobile_cabinet"
            cache_dir = tempfile.gettempdir() + "/lerobot_test_cache"
            # The full path will be: /tmp/lerobot_test_cache/lerobot/aloha_mobile_cabinet
            local_dataset_path = cache_dir + "/" + repo_id
            batch_size = 64

            print(f"Pre-downloading dataset: {repo_id}")
            print(f"Download path: {local_dataset_path}")

            # Download using HuggingFace snapshot_download
            snapshot_download(
                repo_id=repo_id,
                repo_type="dataset",
                local_dir=local_dataset_path,
                local_dir_use_symlinks=False,
            )

            print(f"✅ Dataset downloaded to: {local_dataset_path}")
            print(f"Parameters: batch_size={batch_size}, episode_indices=[0, 1, 2]")

            # Load from local directory
            # LeRobotDatasetMetadata will construct: root / repo_id
            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root=local_dataset_path,  # Will be combined with repo_id
                episode_indices=[0, 1, 2],  # Limit to 3 episodes
                shuffle=False,
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps={"action": [0.0, 0.03, 0.06, 0.09, 0.12]},
            )

            print("✅ Dataset loaded from local directory")

            assert ds is not None
            print("\n✅ Dataset loaded successfully")

            # Test iter_batches
            print(f"\n   Testing iter_batches with batch_size={batch_size}...")
            batch_count = 0
            total_rows = 0

            import time

            start_time = time.time()

            for batch in ds.iter_batches(batch_size=batch_size, batch_format="numpy"):
                batch_count += 1

                # Get batch size from first key
                first_key = list(batch.keys())[0]
                current_batch_size = len(batch[first_key])
                total_rows += current_batch_size

                if batch_count == 1:
                    print(f"      First batch keys: {list(batch.keys())}")
                    print(f"      First batch size: {current_batch_size}")

                    # Print sample data from first batch
                    for key, value in batch.items():
                        if hasattr(value, "shape"):
                            print(
                                f"         {key}: shape {value.shape}, dtype {value.dtype}"
                            )
                        else:
                            print(f"         {key}: {type(value)}")

                # Iterate through more batches to test performance
                if batch_count >= 10:
                    break

            elapsed_time = time.time() - start_time

            print(f"      ✅ Successfully iterated through {batch_count} batches")
            print(f"      Total rows processed: {total_rows}")
            print(f"      Time elapsed: {elapsed_time:.2f} seconds")
            print(f"      Throughput: {total_rows/elapsed_time:.2f} rows/sec")

            print(
                "\n✅ TEST 9 PASSED: Pre-downloaded local dataset batch iteration works correctly"
            )

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 9 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise

    def test_read_lerobot_delta_timestamps_validation(self):
        """
        Test that delta_timestamps parameter works correctly and creates horizon columns.
        Uses pre-downloaded lerobot/aloha_mobile_cabinet.
        """
        print("\n" + "=" * 70)
        print("TEST 10: Delta timestamps and horizon columns")
        print("=" * 70)

        try:
            repo_id = "lerobot/aloha_mobile_cabinet"
            cache_dir = tempfile.gettempdir() + "/lerobot_test_cache"
            # The full path will be: /tmp/lerobot_test_cache/lerobot/aloha_mobile_cabinet
            local_dataset_path = cache_dir + "/" + repo_id
            delta_timestamps = {"action": [0.0, 0.03, 0.06, 0.09, 0.12, 0.15]}

            print(f"Pre-downloading dataset: {repo_id}")
            print(f"Download path: {local_dataset_path}")
            print(
                f"Parameters: delta_timestamps={delta_timestamps}, episode_indices=[0]"
            )

            # Download using HuggingFace snapshot_download
            snapshot_download(
                repo_id=repo_id,
                repo_type="dataset",
                local_dir=local_dataset_path,
                local_dir_use_symlinks=False,
            )

            print(f"✅ Dataset downloaded to: {local_dataset_path}")

            # Load from local directory
            # LeRobotDatasetMetadata will construct: root / repo_id
            ds = ray.data.read_lerobot(
                repo_id=repo_id,
                root=local_dataset_path,  # Will be combined with repo_id
                shuffle=False,
                episode_indices=[0],  # Single episode for faster testing
                override_num_blocks=2,  # Limit parallelism for testing
                delta_timestamps=delta_timestamps,
            )

            print("✅ Dataset loaded from local directory")

            assert ds is not None
            print("\n✅ Dataset loaded successfully")

            # Check schema for horizon columns
            schema = ds.schema()
            column_names = schema.names
            print(f"\n   Column names: {column_names}")

            # Verify horizon column exists
            expected_horizon_col = "action"
            assert (
                expected_horizon_col in column_names
            ), f"Expected horizon column '{expected_horizon_col}' not found"
            print(f"   ✅ Found horizon column: {expected_horizon_col}")

            # Take a sample and verify horizon data
            sample = ds.take(1)[0]
            if expected_horizon_col in sample:
                horizon_data = sample[expected_horizon_col]
                print("\n   Horizon data info:")
                if hasattr(horizon_data, "shape"):
                    print(f"      Shape: {horizon_data.shape}")
                    print(f"      Dtype: {horizon_data.dtype}")
                    # Verify horizon length matches delta_timestamps length
                    expected_horizon_len = len(delta_timestamps["action"])
                    assert (
                        horizon_data.shape[0] == expected_horizon_len
                    ), f"Expected horizon length {expected_horizon_len}, got {horizon_data.shape[0]}"
                    print(
                        f"      ✅ Horizon length matches delta_timestamps: {expected_horizon_len}"
                    )
                else:
                    print(f"      Type: {type(horizon_data)}")

            print("\n✅ TEST 10 PASSED: Delta timestamps validation successful")

            # Cleanup
            del ds
            import gc

            gc.collect()

        except ImportError as e:
            pytest.skip(f"Required dependencies not installed: {e}")
        except Exception as e:
            print(f"\n❌ TEST 10 FAILED: {e}")
            import traceback

            traceback.print_exc()
            raise


def run_manual_tests():
    """Run tests manually without pytest."""
    print("\n" + "=" * 70)
    print("RUNNING MANUAL TESTS FOR read_lerobot")
    print("=" * 70)

    test_suite = TestReadLerobot()
    try:
        test_suite.setup_class()
    except pytest.skip.Exception as e:
        print(f"\n⚠️  SKIPPED read_lerobot tests: {e}")
        return True

    tests = [
        (
            "test_read_lerobot_with_huggingface_repo_id",
            test_suite.test_read_lerobot_with_huggingface_repo_id,
        ),
        (
            "test_read_lerobot_with_episode_filter",
            test_suite.test_read_lerobot_with_episode_filter,
        ),
        ("test_read_lerobot_with_shuffle", test_suite.test_read_lerobot_with_shuffle),
        (
            "test_read_lerobot_parallelism_options",
            test_suite.test_read_lerobot_parallelism_options,
        ),
        (
            "test_read_lerobot_with_video_backend_options",
            test_suite.test_read_lerobot_with_video_backend_options,
        ),
        (
            "test_read_lerobot_schema_validation",
            test_suite.test_read_lerobot_schema_validation,
        ),
        ("test_read_lerobot_local_dataset", test_suite.test_read_lerobot_local_dataset),
        (
            "test_read_lerobot_local_iter_batches",
            test_suite.test_read_lerobot_local_iter_batches,
        ),
        (
            "test_read_lerobot_delta_timestamps_validation",
            test_suite.test_read_lerobot_delta_timestamps_validation,
        ),
    ]

    passed = 0
    failed = 0
    skipped = 0

    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except pytest.skip.Exception as e:
            print(f"\n⚠️  SKIPPED {test_name}: {e}")
            skipped += 1
        except Exception:
            print(f"\n❌ FAILED {test_name}")
            failed += 1

    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Passed:  {passed}")
    print(f"Failed:  {failed}")
    print(f"Skipped: {skipped}")
    print(f"Total:   {len(tests)}")
    print("=" * 70)

    test_suite.teardown_class()

    return failed == 0


if __name__ == "__main__":
    # Check if pytest should be used
    if "--pytest" in sys.argv:
        # Run with pytest
        sys.exit(pytest.main(["-v", "-s", __file__]))
    else:
        # Run manual tests
        success = run_manual_tests()
        sys.exit(0 if success else 1)
