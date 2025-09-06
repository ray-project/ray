Contributing to Ray Data SQL
============================

Contributions to Ray Data SQL are welcome. This guide helps you get started with contributing to this specific component of Ray Data.

.. contents::
   :depth: 2
   :local:

Getting started
===============

Ray Data SQL is an integral part of Ray Data that enables SQL querying capabilities on Ray Datasets. 
When contributing, you work with several key components:

.. vale off

* **Core SQL Engine** - ``ray/python/ray/data/sql/core.py``: Main query execution logic
* **SQL Parsing** - ``ray/python/ray/data/sql/parser.py``: Query parsing and optimization using SQLGlot
* **Execution Handlers** - ``ray/python/ray/data/sql/execution/``: Query execution strategies
* **Configuration** - ``ray/python/ray/data/sql/config.py``: Engine configuration and settings
* **Testing Framework** - ``ray/python/ray/data/sql/testing.py``: Comprehensive test utilities

.. vale on

Setting up your development environment
=======================================

1. **Fork and clone Ray**

   Follow the standard Ray development setup in the main contributing guide.

2. **Install SQL-specific dependencies**

   .. code-block:: bash

       pip install sqlglot>=20.0.0
       pip install pytest

3. **Verify your setup**

   .. code-block:: bash

       # Run SQL-specific tests
       python -m pytest python/ray/data/sql/tests/ -v
       
       # Run a quick integration test
       python -c "
       import ray
       from ray.data.sql import sql, register_table
       ds = ray.data.range(10)
       register_table('test', ds)
       result = sql('SELECT * FROM test LIMIT 5')
       print(f'Success: {result.count()} rows')
       "

Code Organization
=================

Ray Data SQL follows Ray's coding standards with some specific conventions:

Directory Structure
-------------------

.. code-block::

    ray/python/ray/data/sql/
    ├── __init__.py              # Public API exports
    ├── core.py                  # Main SQL engine
    ├── parser.py                # SQL parsing and optimization
    ├── config.py                # Configuration classes
    ├── abstractions.py          # Core data structures
    ├── utils.py                 # Utility functions
    ├── testing.py               # Test framework
    ├── execution/               # Query execution strategies
    │   ├── __init__.py
    │   ├── handlers.py          # Query execution handlers
    │   ├── analyzers.py         # Query analysis
    │   └── executor.py          # Main execution coordinator
    └── tests/                   # Test suite
        ├── __init__.py
        ├── test_basic.py
        ├── test_joins.py
        └── test_performance.py

API Stability Guidelines
========================

Ray Data SQL follows Ray's API stability annotations:

**@PublicAPI(stability="beta")** - Current Status
  All main SQL functions (``sql``, ``register_table``, etc.) are marked as beta.
  This means the API is stable for early users but may still evolve.

**@DeveloperAPI** - Advanced Features
  Engine internals and advanced configuration options are marked as DeveloperAPI.
  These are for advanced users building on top of Ray Data SQL.

Example API Annotations
-----------------------

.. testcode::

    from ray.util.annotations import PublicAPI, DeveloperAPI

    @PublicAPI(stability="beta")
    def sql(query: str) -> Dataset:
        """Execute SQL query on registered tables.
        
        This function provides the main interface for SQL queries
        and is considered stable for beta users.
        """
        pass

    @DeveloperAPI
    def get_engine() -> RaySQL:
        """Get the underlying SQL engine instance.
        
        This is an advanced API for developers who need direct
        access to the SQL engine internals.
        """
        pass

Testing Guidelines
==================

Ray Data SQL has comprehensive testing requirements:

Test Categories
---------------

1. **Unit Tests** - Test individual components in isolation
2. **Integration Tests** - Test SQL queries end-to-end  
3. **Performance Tests** - Benchmark query execution
4. **Compatibility Tests** - Test different SQL dialects

Writing Tests
-------------

Follow these patterns when writing tests:

.. testcode::

    import pytest
    import ray
    from ray.data.sql import sql, register_table, clear_tables

    class TestSQLFeature:
        def setup_method(self):
            """Set up test data before each test."""
            ray.init()
            self.test_data = ray.data.from_items([
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25}
            ])
            register_table("users", self.test_data)
        
        def teardown_method(self):
            """Clean up after each test."""
            clear_tables()
            ray.shutdown()
        
        def test_basic_select(self):
            """Test basic SELECT functionality."""
            result = sql("SELECT name FROM users")
            names = [row["name"] for row in result.take_all()]
            assert "Alice" in names
            assert "Bob" in names
        
        def test_error_handling(self):
            """Test error conditions."""
            with pytest.raises(ValueError, match="nonexistent_table"):
                sql("SELECT * FROM nonexistent_table")

Running Tests
-------------

.. code-block:: bash

    # Run all SQL tests
    python -m pytest python/ray/data/sql/tests/ -v

    # Run specific test file
    python -m pytest python/ray/data/sql/tests/test_basic.py -v

    # Run with SQL debugging enabled
    RAY_DATA_SQL_DEBUG=1 python -m pytest python/ray/data/sql/tests/test_joins.py -v -s

Documentation Standards
=======================

Ray Data SQL documentation follows Ray's documentation standards:

Docstring Format
----------------

Use Google-style docstrings with examples:

.. testcode::

    def register_table(name: str, dataset: Dataset) -> None:
        """Register a Ray Dataset as a SQL table.

        This allows the dataset to be queried using SQL syntax
        through the ``sql()`` function.

        Examples:
            .. testcode::

                import ray
                from ray.data.sql import register_table, sql

                # Create and register a dataset
                users = ray.data.from_items([
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"}
                ])
                register_table("users", users)

                # Query the registered table
                result = sql("SELECT name FROM users")
                print([row["name"] for row in result.take_all()])

            .. testoutput::

                ['Alice', 'Bob']

        Args:
            name: Table name to use in SQL queries. Must be a valid SQL identifier.
            dataset: Ray Dataset to register as a table.

        Raises:
            ValueError: If the table name is invalid or already exists.
        """

Code Examples in Documentation
------------------------------

All code examples in documentation must be testable using Ray's testing framework::

    .. testcode::

        # Code that gets executed during testing
        import ray
        from ray.data.sql import sql
        
        # Create a simple dataset and query it
        ds = ray.data.range(3)
        sql.register("numbers", ds)
        result = sql.execute("SELECT * FROM numbers WHERE id > 0")
        print(list(result.take_all()))

    .. testoutput::

        [{'id': 1}, {'id': 2}]

Common Contribution Areas
=========================

Here are areas where contributions are especially welcome:

SQL Feature Enhancements
-------------------------

* **Window Functions**: Implementing more advanced window operations
* **User-Defined Functions**: Adding support for custom SQL functions  
* **Advanced Joins**: Optimizing complex join operations
* **Subquery Optimization**: Improving nested query performance

Performance Improvements
------------------------

* **Query Optimization**: Enhancing the query optimizer
* **Memory Management**: Better handling of large result sets
* **Parallel Execution**: Improving query parallelization
* **Caching**: Adding intelligent result caching

SQL Dialect Support
-------------------

* **PostgreSQL Compatibility**: Extending PostgreSQL-specific features
* **MySQL Compatibility**: Adding MySQL-specific syntax support
* **BigQuery Compatibility**: Supporting BigQuery SQL patterns

Testing and Quality
-------------------

* **Edge Case Testing**: Finding and testing SQL edge cases
* **Performance Benchmarks**: Creating comprehensive benchmarks
* **Error Message Improvements**: Making error messages more helpful
* **Documentation Examples**: Adding real-world usage examples

Contribution Workflow
=====================

1. **Check Existing Issues**

   Look for relevant issues labeled with ``component:data`` and ``sql`` tags.

2. **Discuss Your Idea**

   For significant changes, open a GitHub issue or discuss in the Ray community Slack.

3. **Follow Ray's PR Process**

   Follow the standard Ray contribution workflow:
   
   * Fork and create a feature branch
   * Make your changes with tests
   * Run the linter: ``./scripts/format.sh``
   * Submit a pull request

4. **SQL-Specific PR Requirements**

   * Include SQL-specific tests
   * Update documentation if adding new features
   * Test with multiple SQL dialects if relevant
   * Include performance considerations in the PR description

Code Style Specifics
====================

SQL-Specific Conventions
------------------------

* **SQL Keywords**: Use uppercase for SQL keywords in examples (``SELECT``, ``FROM``, ``WHERE``)
* **Table Names**: Use lowercase with underscores (``user_data``, ``order_items``)
* **Error Messages**: Provide helpful context about SQL syntax errors
* **Configuration**: Make configuration options discoverable and well-documented

Example Code Style
------------------

.. testcode::

    # Good: Clear error handling with context
    def execute_query(query: str) -> Dataset:
        try:
            return self._engine.execute(query)
        except sqlglot.ParseError as e:
            raise ValueError(
                f"SQL syntax error in query: {query[:100]}...\n"
                f"Error: {str(e)}\n"
                f"Tip: Check your SQL syntax and table names."
            ) from e

    # Good: Descriptive function names and clear docstrings
    def extract_table_names_from_query(query: str) -> Set[str]:
        """Extract table names referenced in a SQL query.
        
        Uses SQLGlot parsing for robust table name extraction,
        with regex fallback for malformed queries.
        """

Community and Support
=====================

Getting Help
------------

* **Ray Community Slack**: Join the ``#ray-data`` channel for SQL-specific questions
* **GitHub Discussions**: Use the Ray discussions board for design questions
* **GitHub Issues**: Report bugs or request features with the ``component:data`` label

Becoming a Reviewer
-------------------

Active contributors to Ray Data SQL may be invited to become reviewers. 
Reviewers help ensure code quality and provide guidance to new contributors.

Recognition
-----------

Significant contributions to Ray Data SQL are recognized in:

* Ray release notes and changelogs
* Community blog posts and talks
* Ray contributor acknowledgments

Thank you for contributing to Ray Data SQL. Your contributions help make Ray's distributed data processing capabilities more accessible through familiar SQL interfaces. 