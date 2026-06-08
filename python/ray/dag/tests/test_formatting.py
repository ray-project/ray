import ray


def test_dag_formatting_nested():
    @ray.remote
    def my_func(x, y=None):
        pass

    # A DAG with nested dictionary, list, set, and DAGNode arguments
    node = my_func.bind(
        [1, my_func.bind(2)],
        y={
            "hyperparams": {"learning_rate": 0.01},
            "inner_list": [3, 4],
            "inner_set": {"z", "a", "c"},
            "nested_node": my_func.bind(5),
        },
    )

    formatted = str(node)

    # Assertions to ensure all keys, values, and structures are present
    assert "y: {" in formatted
    assert "hyperparams: {" in formatted
    assert "learning_rate: 0.01" in formatted
    assert "inner_list: [" in formatted
    assert "inner_set: {" in formatted

    # Verify deterministic sorting of sets
    # Wait, the indentation might be different. Let's just check the sequence
    assert "a," in formatted
    assert "c," in formatted
    assert "z," in formatted
    assert formatted.find("a,") < formatted.find("c,") < formatted.find("z,")

    assert "nested_node: (FunctionNode" in formatted
    assert "args=(" in formatted or "args=[" in formatted
    assert "kwargs={" in formatted


if __name__ == "__main__":
    test_dag_formatting_nested()
    print("Formatting tests passed successfully!")
