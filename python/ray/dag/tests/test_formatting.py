import ray


def test_dag_formatting_nested():
    @ray.remote
    def my_func(x, y=None):
        pass

    # A DAG with nested dictionary, list, and DAGNode arguments
    node = my_func.bind(
        [1, my_func.bind(2)],
        y={
            "hyperparams": {"learning_rate": 0.01},
            "inner_list": [3, 4],
            "nested_node": my_func.bind(5),
        },
    )

    formatted = str(node)

    # Assertions to ensure all keys, values, and structures are present
    assert "y: {" in formatted
    assert "hyperparams: {" in formatted
    assert "learning_rate: 0.01" in formatted
    assert "inner_list: [" in formatted
    assert "nested_node: (FunctionNode" in formatted
    assert "args=[" in formatted
    assert "kwargs={" in formatted


if __name__ == "__main__":
    test_dag_formatting_nested()
    print("Formatting tests passed successfully!")
