import os
import pytest
import sys
import tempfile

import ray


def test_basic_dag_with_names_plot():
    @ray.remote
    def a(*args, **kwargs):
        pass

    tmp1 = a.options(name="tmp1").bind()
    tmp2 = a.options(name="tmp2").bind()
    tmp3 = a.options(name="tmp3").bind(tmp1, tmp2)
    tmp4 = a.options(name="tmp4").bind()
    tmp5 = a.options(name="tmp5").bind(tmp4)
    tmp6 = a.options(name="tmp6").bind()
    dag = a.bind(tmp3, tmp5, tmp6)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.experimental.dag.plot(dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.experimental.dag.vis_utils.dag_to_dot(dag)
    to_string = graph.to_string()
    assert "tmp1 -> tmp3" in to_string
    assert "tmp2 -> tmp3" in to_string
    assert "tmp4 -> tmp5" in to_string
    assert "tmp3 -> a" in to_string
    assert "tmp5 -> a" in to_string
    assert "tmp6 -> a" in to_string


def test_basic_dag_without_names_plot():
    @ray.remote
    def a(*args, **kwargs):
        pass

    tmp1 = a.bind()
    tmp2 = a.bind()
    tmp3 = a.bind(tmp1, tmp2)
    tmp4 = a.bind()
    tmp5 = a.bind(tmp4)
    tmp6 = a.bind()
    dag = a.bind(tmp3, tmp5, tmp6)

    with tempfile.TemporaryDirectory() as tmpdir:
        to_file = os.path.join(tmpdir, "tmp.png")
        ray.experimental.dag.plot(dag, to_file)
        assert os.path.isfile(to_file)

    graph = ray.experimental.dag.vis_utils.dag_to_dot(dag)
    to_string = graph.to_string()
    assert "a_5 -> a_4" in to_string
    assert "a_2 -> a_1" in to_string
    assert "a_3 -> a_1" in to_string
    assert "a_4 -> a" in to_string
    assert "a_6 -> a" in to_string
    assert "a_1 -> a" in to_string


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
