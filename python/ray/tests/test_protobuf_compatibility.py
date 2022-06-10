import pytest
import ray


def test_protobuf_compatibility(shutdown_only):
    protobuf_4_21_1 = {"pip": ["protobuf==4.21.1"]}
    protobuf_3_20_0 = {"pip": ["protobuf==3.20.0"]}

    ray.init()

    @ray.remote
    def load_ray_tune():
        import google.protobuf

        # verfiy this no longer crashes. see
        # https://github.com/ray-project/ray/issues/25282
        import ray  # noqa

        return google.protobuf.__version__

    assert "4.21.1" == ray.get(
        load_ray_tune.options(runtime_env=protobuf_4_21_1).remote()
    )
    assert "3.20.0" == ray.get(
        load_ray_tune.options(runtime_env=protobuf_3_20_0).remote()
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
