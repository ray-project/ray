import pytest

from ray.serve.common import InternalVersion


class TestUserConfigHash:
    def test_validation(self):
        # Code version must be a string.
        with pytest.raises(TypeError):
            InternalVersion(123, None)

        # Can't pass unhashable type as user config.
        with pytest.raises(TypeError):
            InternalVersion(123, set())

        # Can't pass nested unhashable type as user config.
        with pytest.raises(TypeError):
            InternalVersion(123, {"set": set()})

    def test_code_version(self):
        v1 = InternalVersion("1", None)
        v2 = InternalVersion("1", None)
        v3 = InternalVersion("2", None)

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_basic(self):
        v1 = InternalVersion("1", "1")
        v2 = InternalVersion("1", "1")
        v3 = InternalVersion("1", "2")

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_hashable(self):
        v1 = InternalVersion("1", ("1", "2"))
        v2 = InternalVersion("1", ("1", "2"))
        v3 = InternalVersion("1", ("1", "3"))

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_list(self):
        v1 = InternalVersion("1", ["1", "2"])
        v2 = InternalVersion("1", ["1", "2"])
        v3 = InternalVersion("1", ["1", "3"])

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_dict_keys(self):
        v1 = InternalVersion("1", {"1": "1"})
        v2 = InternalVersion("1", {"1": "1"})
        v3 = InternalVersion("1", {"2": "1"})

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_dict_vals(self):
        v1 = InternalVersion("1", {"1": "1"})
        v2 = InternalVersion("1", {"1": "1"})
        v3 = InternalVersion("1", {"1": "2"})

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_nested(self):
        v1 = InternalVersion("1", [{"1": "2"}, {"1": "2"}])
        v2 = InternalVersion("1", [{"1": "2"}, {"1": "2"}])
        v3 = InternalVersion("1", [{"1": "2"}, {"1": "3"}])

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)

    def test_user_config_nested_in_hashable(self):
        v1 = InternalVersion("1", ([{"1": "2"}, {"1": "2"}],))
        v2 = InternalVersion("1", ([{"1": "2"}, {"1": "2"}],))
        v3 = InternalVersion("1", ([{"1": "2"}, {"1": "3"}],))

        assert v1 == v2
        assert hash(v1) == hash(v2)
        assert v1 != v3
        assert hash(v1) != hash(v3)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
