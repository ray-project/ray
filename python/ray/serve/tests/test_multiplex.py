import pytest

from ray import serve


def test_multiplexed():
    """Test multiplexed API."""

    with pytest.raises(NotImplementedError):

        @serve.deployment
        class Model:
            @serve.multiplexed
            def get_model(self, model_id: str):
                pass


def test_get_multiplexed_model_id():
    """Test get_multiplexed_model_id API."""

    with pytest.raises(NotImplementedError):
        serve.get_multiplexed_model_id()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
