"""Block format implementations — Arrow, Pandas, and table abstractions.

Modules:
    arrow_block: ArrowBlockAccessor, ArrowBlockBuilder
    pandas_block: PandasBlockAccessor, PandasBlockBuilder
    table_block: TableBlockAccessor, TableBlockBuilder (shared base)
    block_builder: Abstract BlockBuilder API
    delegating_block_builder: DelegatingBlockBuilder (auto-selects by data type)
    output_buffer: Output block sizing logic
    numpy_support: NumPy column validation and dtype helpers
    arrow_utils: Low-level PyArrow list-array helpers
"""
