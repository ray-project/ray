import pytest
import ray


def test_protobuf_compatibility(shutdown_only):
    protobuf_4_21_0 = {"pip": ["protobuf==4.21.0"]}
    protobuf_3_12_1 = {"pip": ["protobuf==3.12.1"]}

    ray.init()

    @ray.remote
    def load_ray():
        import google.protobuf

        # verfiy this no longer crashes. see
        # https://github.com/ray-project/ray/issues/25282
        import ray  # noqa

        return google.protobuf.__version__

    assert "4.21.0" == ray.get(load_ray.options(runtime_env=protobuf_4_21_0).remote())
    assert "3.12.1" == ray.get(load_ray.options(runtime_env=protobuf_3_12_1).remote())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
