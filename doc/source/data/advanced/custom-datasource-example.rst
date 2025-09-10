.. _custom_datasource:

Advanced: Custom Datasource Development
=======================================

**Keywords:** custom datasource, file format extension, advanced Ray Data, datasource development, custom connectors

This comprehensive guide shows you how to extend Ray Data to support custom file formats and data sources that aren't natively supported. Learn to build production-ready custom datasources with proper error handling, optimization, and integration patterns.

**What you'll learn:**

* Implement custom file readers using FileBasedDatasource with performance optimization
* Build custom data writers with RowBasedFileDatasink and BlockBasedFileDatasink
* Add comprehensive error handling and fault tolerance to custom datasources
* Integrate custom datasources with Ray Data's distributed execution and streaming model
* Production deployment strategies and testing approaches for custom datasources
* Performance optimization and resource management for custom implementations
* Maintenance and versioning strategies for custom datasource development

**Business Value of Custom Datasources:**

**Enterprise Integration Benefits:**
* **Legacy system connectivity**: Connect Ray Data to proprietary enterprise data formats
* **Performance optimization**: Implement format-specific optimizations not available in generic readers
* **Compliance requirements**: Handle specialized data formats required for regulatory compliance
* **Competitive advantage**: Process unique data sources that provide business differentiation

**Technical Capability Benefits:**
* **Format specialization**: Optimize for specific data characteristics and access patterns
* **Resource efficiency**: Implement custom resource management for specialized workloads
* **Integration flexibility**: Connect with external systems using custom protocols and authentication
* **Future-proofing**: Extend Ray Data capabilities as new data sources and formats emerge

**When to Build Custom Datasources**

Consider building a custom datasource when:
- **Unsupported formats**: You need to process proprietary or specialized file formats
- **Custom protocols**: Your data sources use custom authentication or access patterns
- **Performance optimization**: You need format-specific optimizations not available in generic readers
- **Enterprise integration**: You need to integrate with legacy or specialized enterprise systems

:::note
**Alternative Approach for Simple Custom Processing**
For most use cases, you can use :func:`~ray.data.read_binary_files` with :meth:`~ray.data.Dataset.map` for custom processing instead of building a full datasource. Build custom datasources only when you need:
- **Performance optimization**: Format-specific optimizations not possible with generic readers
- **Complex integration**: Custom authentication, connection pooling, or protocol handling
- **Enterprise requirements**: Specialized error handling, audit logging, or compliance features
- **Reusability**: Shareable connectors for use across multiple projects and teams
:::

**Custom Datasource Development Decision Framework:**

:::list-table
   :header-rows: 1

- - **Requirement**
  - **Use read_binary_files + map**
  - **Build Custom Datasource**
  - **Key Considerations**
- - **Simple format parsing**
  - ✅ Recommended
  - ❌ Unnecessary complexity
  - Use map() for straightforward parsing
- - **Performance optimization needed**
  - ❌ Limited optimization
  - ✅ Format-specific optimization
  - Custom datasource enables specialized optimization
- - **Complex authentication**
  - ❌ Limited auth support
  - ✅ Full auth integration
  - Custom datasource handles complex auth patterns
- - **Enterprise integration**
  - ❌ Basic functionality
  - ✅ Enterprise features
  - Custom datasource supports audit, compliance, monitoring
- - **Reusable across projects**
  - ❌ Code duplication
  - ✅ Shareable component
  - Custom datasource enables reuse and standardization

:::

**Example Use Cases:**
- Medical imaging formats (DICOM, NIfTI)
- Scientific data formats (HDF5, NetCDF)
- Proprietary enterprise formats
- Streaming protocols and real-time data sources

Read data from files
--------------------

.. tip::
    If you're not contributing to Ray Data, you don't need to create a
    :class:`~ray.data.Datasource`. Instead, you can call
    :func:`~ray.data.read_binary_files` and decode files with
    :meth:`~ray.data.Dataset.map`.

The core abstraction for reading files is :class:`~ray.data.datasource.FileBasedDatasource`.
It provides file-specific functionality on top of the
:class:`~ray.data.Datasource` interface.

To subclass :class:`~ray.data.datasource.FileBasedDatasource`, implement the constructor
and ``_read_stream``.

Implement the constructor
=========================

**Constructor Implementation Best Practices**

The constructor initializes your custom datasource and configures file handling behavior. Follow these patterns for robust implementation:

.. code-block:: python

    class CustomImageDatasource(FileBasedDatasource):
        """Custom datasource for specialized image formats."""
        
        def __init__(
            self,
            paths: Union[str, List[str]],
            *,
            file_extensions: Optional[List[str]] = None,
            **file_based_datasource_kwargs,
        ):
            # Validate input parameters
            if isinstance(paths, str):
                paths = [paths]
            
            # Set supported file extensions
            if file_extensions is None:
                file_extensions = [".custom", ".proprietary"]
            
            # Initialize parent class with configuration
            super().__init__(
                paths=paths,
                file_extensions=file_extensions,
                **file_based_datasource_kwargs,
            )
            
            # Initialize any custom configuration
            self._custom_config = self._initialize_custom_settings()

**Key Implementation Considerations:**
- **Input validation**: Validate constructor parameters for robustness
- **File extension filtering**: Specify supported extensions to avoid processing invalid files
- **Configuration management**: Initialize any format-specific configuration
- **Error handling**: Handle invalid paths or configuration gracefully

Call the superclass constructor and specify the files you want to read. Optionally, specify valid file extensions. Ray Data ignores files with other extensions.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __datasource_constructor_start__
    :end-before: __datasource_constructor_end__

Implement ``_read_stream``
==========================

**Stream Reading Implementation Patterns**

The ``_read_stream`` method is the core of your custom datasource. It's a generator that yields blocks of data from files, enabling Ray Data's distributed processing capabilities.

**Implementation Requirements:**
- **Generator function**: Must yield blocks of data, not return them
- **Block format**: Yield data in Ray Data's expected block format
- **Error handling**: Handle file reading errors gracefully
- **Memory efficiency**: Process files in chunks to avoid memory issues
- **Schema consistency**: Ensure consistent schema across all yielded blocks

**Advanced Implementation Example:**

.. code-block:: python

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read custom file format with comprehensive error handling."""
        import numpy as np
        from PIL import Image
        
        try:
            # Read custom format header
            header = self._read_custom_header(f)
            
            # Validate file integrity
            if not self._validate_file_format(header):
                raise ValueError(f"Invalid file format: {path}")
            
            # Read image data in chunks for memory efficiency
            while True:
                chunk = f.read(1024*1024)  # 1MB chunks
                if not chunk:
                    break
                
                # Process chunk and convert to standard format
                processed_data = self._process_custom_format(chunk)
                
                # Convert to Ray Data block format
                block_data = {
                    "image": processed_data,
                    "metadata": self._extract_metadata(header),
                    "path": path
                }
                
                # Yield block for Ray Data processing
                yield pd.DataFrame([block_data])
                
        except Exception as e:
            # Comprehensive error handling
            error_msg = f"Failed to read {path}: {str(e)}"
            logger.error(error_msg)
            
            # Yield error information for debugging
            yield pd.DataFrame([{
                "error": error_msg,
                "path": path,
                "error_type": type(e).__name__
            }])

**Error Handling Best Practices:**
- **Graceful degradation**: Continue processing other files if one fails
- **Detailed logging**: Log specific error information for debugging
- **Error reporting**: Include error details in output for troubleshooting
- **Recovery strategies**: Implement fallback processing when possible

``_read_stream`` is a generator that yields one or more blocks of data from a file.

**Block Format and Data Types**

Blocks are Ray Data's internal abstraction for collections of rows. They can be:
- **PyArrow tables**: Optimal for structured data with schema
- **pandas DataFrames**: Good for mixed data types and complex processing
- **Dictionaries of NumPy arrays**: Efficient for numerical and tensor data

**Performance Optimization Techniques**

**Chunk-Based Reading for Large Files:**

.. code-block:: python

    def _read_stream_optimized(self, file: "FileMetadata", **reader_kwargs):
        """Optimized reading with configurable chunk sizes."""
        
        # Configure chunk size based on file size and memory constraints
        file_size = file.size_bytes
        optimal_chunk_size = min(file_size // 10, 10 * 1024 * 1024)  # Max 10MB chunks
        
        with file.open() as f:
            while True:
                chunk = f.read(optimal_chunk_size)
                if not chunk:
                    break
                
                # Process chunk efficiently
                processed_chunk = self._fast_process(chunk)
                yield self._create_block(processed_chunk)

**Schema Optimization:**

.. code-block:: python

    def _optimize_schema(self, data):
        """Optimize data types for better performance."""
        
        # Use efficient data types
        if "id" in data.columns:
            data["id"] = data["id"].astype("int32")  # Instead of int64
        
        if "timestamp" in data.columns:
            data["timestamp"] = pd.to_datetime(data["timestamp"])
        
        # Convert string columns to categorical when appropriate
        if "category" in data.columns:
            data["category"] = data["category"].astype("category")
        
        return data

**Memory Management:**

.. code-block:: python

    def _memory_efficient_processing(self, file_data):
        """Process data with memory efficiency."""
        
        # Process in smaller batches to control memory usage
        batch_size = 1000
        for i in range(0, len(file_data), batch_size):
            batch = file_data[i:i + batch_size]
            
            # Process batch and immediately yield to free memory
            processed_batch = self._process_batch(batch)
            yield processed_batch
            
            # Explicit garbage collection for large files
            if i % 10000 == 0:  # Every 10 batches
                import gc
                gc.collect()

**Production Deployment Considerations**

**Error Recovery and Resilience:**

.. code-block:: python

    def _production_read_stream(self, file: "FileMetadata", **reader_kwargs):
        """Production-ready implementation with comprehensive error handling."""
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Attempt to read file
                for block in self._read_file_blocks(file):
                    yield block
                break  # Success - exit retry loop
                
            except (IOError, ConnectionError) as e:
                retry_count += 1
                if retry_count >= max_retries:
                    # Log final failure and yield error information
                    logger.error(f"Failed to read {file.path} after {max_retries} retries: {e}")
                    yield self._create_error_block(file.path, str(e))
                else:
                    # Wait before retry with exponential backoff
                    import time
                    wait_time = 2 ** retry_count
                    time.sleep(wait_time)
                    logger.warning(f"Retrying {file.path} (attempt {retry_count + 1})")

**Integration with Ray Data Features:**

.. code-block:: python

    def _integrate_with_ray_features(self):
        """Integrate custom datasource with Ray Data features."""
        
        # Support for column pruning
        def supports_column_pruning(self):
            return True
        
        # Support for predicate pushdown
        def supports_filter_pushdown(self):
            return True
        
        # Implement schema inference
        def infer_schema(self, file_paths):
            # Read sample files to infer schema
            sample_data = self._read_sample_files(file_paths[:5])
            return self._extract_schema(sample_data)

**Testing and Validation Patterns**

**Unit Testing Your Custom Datasource:**

.. code-block:: python

    import pytest
    import tempfile
    import os
    from ray.data.datasource import FileBasedDatasource

    class TestCustomDatasource:
        """Comprehensive testing for custom datasource."""
        
        def test_basic_reading(self):
            """Test basic file reading functionality."""
            # Create test file
            with tempfile.NamedTemporaryFile(suffix=".custom", delete=False) as f:
                # Write test data
                test_data = b"custom format test data"
                f.write(test_data)
                test_file_path = f.name
            
            try:
                # Test datasource
                ds = CustomImageDatasource([test_file_path])
                dataset = ray.data.read_datasource(ds)
                
                # Validate results
                rows = dataset.take_all()
                assert len(rows) > 0
                assert "image" in rows[0]
                
            finally:
                os.unlink(test_file_path)
        
        def test_error_handling(self):
            """Test error handling for invalid files."""
            # Test with non-existent file
            ds = CustomImageDatasource(["nonexistent.custom"])
            
            # Should handle gracefully
            dataset = ray.data.read_datasource(ds)
            rows = dataset.take_all()
            
            # Should contain error information
            assert any("error" in row for row in rows)
        
        def test_performance_characteristics(self):
            """Test performance with different file sizes."""
            import time
            
            # Test with small and large files
            small_file = self._create_test_file(size_mb=1)
            large_file = self._create_test_file(size_mb=100)
            
            # Measure performance
            start_time = time.time()
            small_ds = ray.data.read_datasource(CustomImageDatasource([small_file]))
            small_ds.materialize()
            small_duration = time.time() - start_time
            
            # Performance should scale reasonably
            assert small_duration < 30  # Should complete quickly

**Production Deployment Checklist:**

- [ ] **Error handling**: Comprehensive error handling for all failure modes
- [ ] **Performance testing**: Validated with realistic file sizes and volumes
- [ ] **Memory efficiency**: Tested with memory-constrained environments
- [ ] **Schema consistency**: Verified consistent schema across all files
- [ ] **Distributed testing**: Tested with multi-node Ray clusters
- [ ] **Integration testing**: Verified with downstream Ray Data operations
- [ ] **Monitoring integration**: Logging and metrics for production observability

Blocks are Ray Data's internal abstraction for collections of rows. They can be PyArrow tables, pandas DataFrames, or dictionaries of NumPy arrays.

Don't create a block directly. Instead, add rows of data to a
`DelegatingBlockBuilder <https://github.com/ray-project/ray/blob/23d3bfcb9dd97ea666b7b4b389f29b9cc0810121/python/ray/data/_internal/delegating_block_builder.py#L10>`_.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __read_stream_start__
    :end-before: __read_stream_end__

Read your data
==============

Once you've implemented ``ImageDatasource``, call :func:`~ray.data.read_datasource` to
read images into a :class:`~ray.data.Dataset`. Ray Data reads your files in parallel.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __read_datasource_start__
    :end-before: __read_datasource_end__

Write data to files
-------------------

.. note::
    The write interface is under active development and might change in the future. If
    you have feature requests,
    `open a GitHub Issue <https://github.com/ray-project/ray/issues/new?assignees=&labels=enhancement%2Ctriage&projects=&template=feature-request.yml&title=%5B%3CRay+component%3A+Core%7CRLlib%7Cetc...%3E%5D+>`_.

The core abstractions for writing data to files are :class:`~ray.data.datasource.RowBasedFileDatasink` and
:class:`~ray.data.datasource.BlockBasedFileDatasink`. They provide file-specific functionality on top of the
:class:`~ray.data.Datasink` interface.

If you want to write one row per file, subclass :class:`~ray.data.datasource.RowBasedFileDatasink`.
Otherwise, subclass :class:`~ray.data.datasource.BlockBasedFileDatasink`.

In this example, you write one image per file, so you subclass :class:`~ray.data.datasource.RowBasedFileDatasink`. To subclass :class:`~ray.data.datasource.RowBasedFileDatasink`, implement the constructor and :meth:`~ray.data.datasource.RowBasedFileDatasink.write_row_to_file`.

Implement the constructor
=========================

Call the superclass constructor and specify the folder to write to. Optionally, specify
a string representing the file format (for example, ``"png"``). Ray Data uses the
file format as the file extension.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __datasink_constructor_start__
    :end-before: __datasink_constructor_end__

Implement ``write_row_to_file``
===============================

``write_row_to_file`` writes a row of data to a file. Each row is a dictionary that maps
column names to values.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __write_row_to_file_start__
    :end-before: __write_row_to_file_end__

Write your data
===============

Once you've implemented ``ImageDatasink``, call :meth:`~ray.data.Dataset.write_datasink`
to write images to files. Ray Data writes to multiple files in parallel.

.. literalinclude:: doc_code/custom_datasource_example.py
    :language: python
    :start-after: __write_datasink_start__
    :end-before: __write_datasink_end__

Production Deployment and Testing
----------------------------------

**Custom Datasource Production Readiness**

Before deploying custom datasources to production, ensure comprehensive testing and validation:

.. code-block:: python

    # Production-ready custom datasource testing framework
    def test_custom_datasource_production_readiness():
        """Comprehensive testing for custom datasource production deployment."""
        
        # Test 1: Performance benchmarking
        def benchmark_datasource_performance():
            """Benchmark custom datasource vs alternatives."""
            import time
            
            # Test with different data sizes
            test_sizes = [1000, 10000, 100000]
            
            for size in test_sizes:
                # Generate test data
                test_data = generate_test_data(size)
                
                # Benchmark read performance
                start_time = time.time()
                dataset = ray.data.read_datasource(CustomDatasource(), paths=test_data)
                dataset.materialize()
                read_time = time.time() - start_time
                
                print(f"Read {size} records in {read_time:.2f} seconds")
                print(f"Throughput: {size / read_time:.0f} records/second")
        
        # Test 2: Error handling validation
        def test_error_handling():
            """Test datasource error handling capabilities."""
            # Test with corrupted data
            try:
                corrupted_data = create_corrupted_test_data()
                dataset = ray.data.read_datasource(CustomDatasource(), paths=corrupted_data)
                dataset.materialize()
            except Exception as e:
                print(f"Properly handled corrupted data: {str(e)}")
        
        # Test 3: Resource utilization
        def test_resource_utilization():
            """Test resource usage patterns."""
            import psutil
            
            # Monitor memory usage during processing
            initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            # Process large dataset
            large_dataset = ray.data.read_datasource(
                CustomDatasource(), 
                paths=generate_large_test_data()
            )
            large_dataset.materialize()
            
            peak_memory = psutil.Process().memory_info().rss / 1024 / 1024
            memory_usage = peak_memory - initial_memory
            
            print(f"Memory usage: {memory_usage:.1f} MB")
        
        # Run all tests
        benchmark_datasource_performance()
        test_error_handling()
        test_resource_utilization()

Contributing Custom Datasources to Ray Data
--------------------------------------------

**Community Contribution Guidelines**

If you've built a valuable custom datasource, consider contributing it to Ray Data to benefit the entire community.

**Contribution Readiness Checklist:**

**Code Quality Requirements:**
- [ ] **Comprehensive testing**: Unit tests, integration tests, and performance tests
- [ ] **Error handling**: Robust error handling for all failure scenarios  
- [ ] **Documentation**: Complete API documentation and usage examples
- [ ] **Performance optimization**: Optimized for production use cases
- [ ] **Code style**: Follows Ray Data coding standards and conventions

**Community Value Assessment:**
- [ ] **General applicability**: Useful for multiple users and use cases
- [ ] **Maintenance commitment**: Willing to maintain and support the datasource
- [ ] **Compatibility**: Works with Ray Data's architecture and roadmap
- [ ] **Performance**: Provides meaningful performance benefits over alternatives
- [ ] **Stability**: Thoroughly tested and production-ready

**Contribution Process:**
1. **Community discussion**: Discuss your datasource idea in Ray Data community channels
2. **Design review**: Share design and get feedback from Ray Data maintainers
3. **Implementation**: Develop following Ray Data contribution guidelines
4. **Testing**: Comprehensive testing including performance benchmarks
5. **Documentation**: Complete documentation and examples
6. **Pull request**: Submit PR with thorough description and justification
7. **Review process**: Work with maintainers through code review and feedback
8. **Maintenance**: Commit to ongoing maintenance and community support

Next Steps
----------

**For Custom Development:**
* **Build your datasource**: Follow this guide to implement your custom datasource
* **Test thoroughly**: Use comprehensive testing strategies for production readiness
* **Optimize performance**: Apply performance optimization techniques for your specific format

**For Community Contribution:**
* **Engage with community**: Discuss your datasource ideas in community channels
* **Follow contribution guidelines**: Review Ray Data contribution guidelines and standards
* **Submit contributions**: Share your datasource with the community through pull requests

**For Advanced Capabilities:**
* **Explore advanced features**: Learn about other advanced capabilities → :ref:`Advanced Features <advanced-features>`
* **Understand architecture**: Deepen architecture knowledge → :ref:`Architecture Overview <architecture-overview>`
* **Optimize performance**: Apply advanced optimization → :ref:`Performance Optimization <performance-optimization>`
* **Deploy to production**: Use production best practices → :ref:`Production Deployment <production-deployment>`
