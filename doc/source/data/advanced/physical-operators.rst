.. _physical-operators:

Physical Operators: Ray Data's Execution Engine
===============================================

**Keywords:** Ray Data physical operators, streaming execution, operator topology, task scheduling, distributed execution, performance optimization

This technical deep dive explains Ray Data's physical operator system based on codebase analysis, providing insights into how Ray Data achieves superior performance through sophisticated operator design and execution coordination.

**What you'll learn:**

* Physical operator architecture and implementation details
* Operator topology and task coordination mechanisms  
* Resource management and backpressure policies
* Performance optimization through operator design
* Practical troubleshooting using operator-level insights
* Production tuning strategies based on operator behavior

**Why Understanding Physical Operators Matters:**

**Performance Optimization Benefits:**
* **Resource allocation**: Make informed decisions about CPU, GPU, and memory allocation
* **Bottleneck identification**: Understand where performance constraints occur
* **Scaling strategies**: Design workloads that scale efficiently across cluster sizes
* **Cost optimization**: Optimize resource usage based on operator characteristics

**Production Operation Benefits:**
* **Troubleshooting**: Diagnose complex performance issues using operator insights
* **Monitoring**: Set up meaningful metrics and alerts for operator performance
* **Capacity planning**: Size clusters appropriately based on operator resource requirements
* **Workload design**: Structure pipelines to leverage operator strengths effectively

Physical Operator Architecture
------------------------------

**Core Operator Design**

Based on codebase analysis, Ray Data's execution is built on `PhysicalOperator` classes that implement sophisticated streaming execution:

**PhysicalOperator Base Class:**
```python
class PhysicalOperator(Operator):
    """Abstract class for physical operators.
    
    An operator transforms one or more input streams of RefBundles 
    into a single output stream of RefBundles.
    """
```

**Key Operator Types:**

**1. MapOperator (OneToOneOperator)**
- **Purpose**: Transforms data using user-defined functions
- **Implementation**: `MapOperator(OneToOneOperator, InternalQueueOperatorMixin)`
- **Execution modes**: TaskPoolMapOperator (tasks) vs ActorPoolMapOperator (actors)
- **Resource allocation**: Configurable CPU/GPU resources per operation

**2. AllToAllOperator**
- **Purpose**: Operations requiring global coordination (shuffle, sort, groupby)
- **Implementation**: Materializes data for global operations
- **Memory management**: Handles large-scale data redistribution
- **Performance**: Optimized for distributed coordination patterns

**3. InputDataBuffer**
- **Purpose**: Entry point for data into the streaming topology
- **Implementation**: Manages data loading and initial block creation
- **Optimization**: Handles parallel data loading and format conversion

Operator Execution Coordination
-------------------------------

**Topology Management**

The streaming executor builds and manages an operator topology:

**Topology Structure:**
```python
# Topology is a dict mapping operators to their execution state
Topology = Dict[PhysicalOperator, OpState]

class OpState:
    """Tracks execution state for each operator including:
    - Input/output queues (OpBufferQueue)
    - Resource usage and allocation
    - Execution progress and metrics
    - Scheduling status and backpressure state
    """
```

**Task Coordination System:**

**DataOpTask and MetadataOpTask:**
- **DataOpTask**: Handles actual block data processing with streaming generators
- **MetadataOpTask**: Handles metadata-only operations for optimization
- **Streaming generators**: `ObjectRefGenerator` provides asynchronous task execution
- **Callback system**: `output_ready_callback` and `task_done_callback` coordinate task completion

**Advanced Resource Management**

**ResourceManager and Allocation:**

**ReservationOpResourceAllocator:**
- **Resource budgets**: Dynamic resource allocation based on operator requirements
- **CPU/GPU optimization**: Intelligent allocation of heterogeneous resources
- **Memory tracking**: Monitors memory usage and prevents exhaustion
- **Locality awareness**: Considers data location for task placement

**Backpressure Policies:**

**1. ResourceBudgetBackpressurePolicy:**
```python
class ResourceBudgetBackpressurePolicy(BackpressurePolicy):
    """Backpressure based on resource budgets in ResourceManager."""
    
    def can_add_input(self, op: PhysicalOperator) -> bool:
        """Determines if operator can accept new inputs based on resource budget."""
        budget = self._resource_manager.get_budget(op)
        return op.incremental_resource_usage().satisfies_limit(budget)
```

**2. ConcurrencyCapBackpressurePolicy:**
- **Purpose**: Limits concurrent tasks per operator to prevent resource exhaustion
- **Implementation**: `num_tasks_running < concurrency_caps[op]`
- **Benefit**: Prevents task explosion and maintains system stability

**Performance Optimization Through Operator Design**

**Operator Fusion Optimization:**
- **Logical optimization**: `OperatorFusionRule` combines compatible map operators
- **Benefit**: Reduces serialization overhead and improves performance
- **Implementation**: Automatic fusion during physical plan creation

**Streaming Task Execution:**
- **Non-blocking execution**: Operators process data as it becomes available
- **Pipeline parallelism**: Multiple operators execute simultaneously
- **Memory efficiency**: Streaming execution with automatic memory management

**Intelligent Task Scheduling:**

**Eligible Operator Selection:**
```python
def get_eligible_operators(
    topology: Topology,
    backpressure_policies: List[BackpressurePolicy],
    *,
    ensure_liveness: bool,
) -> List[PhysicalOperator]:
    """Returns operators eligible for execution based on:
    1. Not completed
    2. Has input blocks available
    3. Can accept new inputs (not backpressured)
    4. Not currently throttled
    """
```

**Operator State Management:**
- **OpSchedulingStatus**: Tracks operator runnable state and resource limits
- **Dynamic updates**: Continuous state updates based on execution progress
- **Liveness guarantees**: Ensures at least one operator can make progress

Operator Performance Characteristics
------------------------------------

**Memory Efficiency:**
- **Zero-copy data flow**: RefBundles pass references, not data copies
- **Eager cleanup**: Ownership semantics enable immediate memory reclamation
- **Streaming execution**: Process datasets larger than cluster memory
- **Object store integration**: Automatic spilling for large datasets

**Resource Optimization:**
- **Heterogeneous compute**: Intelligent CPU/GPU allocation per operator
- **Dynamic resource budgets**: Adaptive resource allocation based on workload
- **Locality-aware scheduling**: Tasks scheduled near data to minimize network transfer
- **Backpressure coordination**: Automatic throttling prevents resource exhaustion

**Fault Tolerance:**
- **Task-level recovery**: Individual task failures don't affect pipeline execution
- **Operator isolation**: Failures isolated to specific operators
- **Graceful degradation**: System continues with partial failures
- **Error propagation**: Controlled error handling and recovery

Production Optimization Patterns
---------------------------------

**Operator Configuration Best Practices:**

**For CPU-Intensive Operations:**
- Use `TaskPoolMapOperator` for stateless transformations
- Configure appropriate `num_cpus` based on operation complexity
- Optimize batch sizes for CPU cache efficiency

**For GPU-Intensive Operations:**
- Use `ActorPoolMapOperator` with GPU allocation
- Configure `num_gpus` and `batch_size` for GPU memory optimization
- Implement GPU memory management in user functions

**For Memory-Intensive Operations:**
- Monitor `incremental_resource_usage()` for memory tracking
- Use streaming execution to handle large datasets
- Configure appropriate block sizes for memory efficiency

**Performance Monitoring:**
- **Operator metrics**: Track execution time, resource usage, and throughput
- **Backpressure monitoring**: Monitor backpressure policies for bottleneck identification
- **Resource utilization**: Track CPU/GPU utilization across operators

Hands-On Operator Analysis Exercises
------------------------------------

**Exercise 1: Operator Performance Profiling**

Apply your operator knowledge to analyze and optimize a real workload:

.. code-block:: python

    # Exercise: Profile operator performance in your workload
    import ray
    import time
    from ray.data.context import DataContext
    
    def exercise_operator_profiling():
        """Profile operator performance to understand bottlenecks."""
        
        # Enable detailed execution logging
        ctx = DataContext.get_current()
        ctx.execution_options.verbose_progress = True
        
        # Create test dataset
        dataset = ray.data.range(1000000)  # 1M records
        
        # Apply different operations and measure performance
        def profile_operation(operation_name, operation_func):
            """Profile a specific operation."""
            start_time = time.time()
            result = operation_func(dataset)
            result.materialize()  # Force execution
            end_time = time.time()
            
            print(f"{operation_name}: {end_time - start_time:.2f} seconds")
            return end_time - start_time
        
        # Profile different operator types
        map_time = profile_operation(
            "Map Operator", 
            lambda ds: ds.map(lambda x: x * 2)
        )
        
        filter_time = profile_operation(
            "Filter Operator",
            lambda ds: ds.filter(lambda x: x % 2 == 0)
        )
        
        groupby_time = profile_operation(
            "GroupBy Operator",
            lambda ds: ds.groupby(lambda x: x % 100).count()
        )
        
        # Analyze results and identify patterns
        print(f"Performance analysis:")
        print(f"- Map operator: {map_time:.2f}s (row-level processing)")
        print(f"- Filter operator: {filter_time:.2f}s (selective processing)")
        print(f"- GroupBy operator: {groupby_time:.2f}s (shuffle-heavy)")

**Exercise 2: Resource Allocation Optimization**

.. code-block:: python

    def exercise_resource_optimization():
        """Experiment with resource allocation for different operators."""
        
        # Test different batch sizes for map_batches operations
        def test_batch_sizes(dataset, batch_sizes):
            """Test performance with different batch sizes."""
            results = {}
            
            for batch_size in batch_sizes:
                start_time = time.time()
                result = dataset.map_batches(
                    lambda batch: batch * 2,
                    batch_size=batch_size
                )
                result.materialize()
                end_time = time.time()
                
                results[batch_size] = end_time - start_time
                print(f"Batch size {batch_size}: {end_time - start_time:.2f}s")
            
            return results
        
        # Test with different batch sizes
        dataset = ray.data.range(100000)
        batch_sizes = [100, 1000, 5000, 10000]
        performance_results = test_batch_sizes(dataset, batch_sizes)
        
        # Find optimal batch size
        optimal_batch_size = min(performance_results, key=performance_results.get)
        print(f"Optimal batch size for this workload: {optimal_batch_size}")

**Self-Assessment Questions:**

After completing the exercises, validate your operator understanding:

1. **Can you explain** why GroupBy operations typically require more memory than Map operations?
2. **Can you predict** which operators in your workload will be bottlenecks based on their characteristics?
3. **Can you optimize** resource allocation for different operator types?
4. **Can you design** pipelines that minimize operator-level performance issues?

Next Steps
----------

**Apply Operator Knowledge:**
- **Performance optimization**: Use operator insights for :ref:`Performance Optimization <performance-optimization>`
- **Production deployment**: Apply operator knowledge to :ref:`Best Practices <best_practices>`
- **Advanced usage**: Explore :ref:`Custom Datasource Development <custom-datasource-example>`
- **Community contribution**: Contribute operator improvements to Ray Data development
