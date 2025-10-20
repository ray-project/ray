import pytest

from ray.data._internal.logical.interfaces.operator import Operator


def test_apply_transform_dag_consistency():
    a = Operator("A", [])
    b = Operator("B", [])
    c = Operator("C", [a, b])

    def transform_b(op: Operator) -> Operator:
        """Transform only operator B, leaving A unchanged."""
        if op.name == "B":
            return Operator("Transformed B", op.input_dependencies)
        return op

    c_transformed = c._apply_transform(transform_b)

    # This should create the DAG:
    #               A --|
    #                   |--> C
    #   B transformed --|

    assert c_transformed.name == "C"
    assert c_transformed.input_dependencies[0].output_dependencies[0].name == "C"
    assert c_transformed is c_transformed.input_dependencies[0].output_dependencies[0]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
