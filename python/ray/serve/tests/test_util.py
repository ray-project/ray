import asyncio
import json
import os
from unittest.mock import patch

import numpy as np
import pytest
from fastapi.encoders import jsonable_encoder

import ray
from ray import serve
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray.serve._private.utils import (
    calculate_remaining_timeout,
    get_all_live_placement_group_names,
    get_current_actor_id,
    get_head_node_id,
    is_running_in_asyncio_loop,
    merge_dict,
    msgpack_deserialize,
    msgpack_serialize,
    override_runtime_envs_except_env_vars,
    serve_encoders,
    snake_to_camel_case,
)
from ray.serve.tests.common.remote_uris import (
    TEST_DAG_PINNED_URI,
    TEST_DEPLOY_GROUP_PINNED_URI,
)


def test_serialize():
    data = msgpack_serialize(5)
    obj = msgpack_deserialize(data)
    assert 5 == obj


def test_merge_dict():
    dict1 = {"pending": 1, "running": 1, "finished": 1}
    dict2 = {"pending": 4, "finished": 1}
    merge = merge_dict(dict1, dict2)
    assert merge["pending"] == 5
    assert merge["running"] == 1
    assert merge["finished"] == 2
    dict1 = None
    merge = merge_dict(dict1, dict2)
    assert merge["pending"] == 4
    assert merge["finished"] == 1
    try:
        assert merge["running"] == 1
        assert False
    except KeyError:
        assert True
    dict2 = None
    merge = merge_dict(dict1, dict2)
    assert merge is None


def test_bytes_encoder():
    data_before = {"inp": {"nest": b"bytes"}}
    data_after = {"inp": {"nest": "bytes"}}
    assert json.loads(json.dumps(jsonable_encoder(data_before))) == data_after


def test_numpy_encoding():
    data = [1, 2]
    floats = np.array(data).astype(np.float32)
    ints = floats.astype(np.int32)
    uints = floats.astype(np.uint32)
    list_of_uints = [np.int64(1), np.int64(2)]

    for np_data in [floats, ints, uints, list_of_uints]:
        assert (
            json.loads(
                json.dumps(jsonable_encoder(np_data, custom_encoder=serve_encoders))
            )
            == data
        )
    nested = {"a": np.array([1, 2])}
    assert json.loads(
        json.dumps(jsonable_encoder(nested, custom_encoder=serve_encoders))
    ) == {"a": [1, 2]}


@serve.deployment
def decorated_f(*args):
    return "reached decorated_f"


@serve.deployment
class DecoratedActor:
    def __call__(self, *args):
        return "reached decorated_actor"


def gen_func():
    @serve.deployment
    def f():
        pass

    return f


def gen_class():
    @serve.deployment
    class A:
        pass

    return A


class TestOverrideRuntimeEnvsExceptEnvVars:
    def test_merge_empty(self):
        assert {"env_vars": {}} == override_runtime_envs_except_env_vars({}, {})

    def test_merge_empty_parent(self):
        child = {"env_vars": {"test1": "test_val"}, "working_dir": "."}
        assert child == override_runtime_envs_except_env_vars({}, child)

    def test_merge_empty_child(self):
        parent = {"env_vars": {"test1": "test_val"}, "working_dir": "."}
        assert parent == override_runtime_envs_except_env_vars(parent, {})

    @pytest.mark.parametrize("invalid_env", [None, 0, "runtime_env", set()])
    def test_invalid_type(self, invalid_env):
        with pytest.raises(TypeError):
            override_runtime_envs_except_env_vars(invalid_env, {})
        with pytest.raises(TypeError):
            override_runtime_envs_except_env_vars({}, invalid_env)
        with pytest.raises(TypeError):
            override_runtime_envs_except_env_vars(invalid_env, invalid_env)

    def test_basic_merge(self):
        parent = {
            "py_modules": ["http://test.com/test0.zip", "s3://path/test1.zip"],
            "working_dir": "gs://path/test2.zip",
            "env_vars": {"test": "val", "trial": "val2"},
            "pip": ["pandas", "numpy"],
            "excludes": ["my_file.txt"],
        }
        original_parent = parent.copy()

        child = {
            "py_modules": [],
            "working_dir": "s3://path/test1.zip",
            "env_vars": {"test": "val", "trial": "val2"},
            "pip": ["numpy"],
        }
        original_child = child.copy()

        merged = override_runtime_envs_except_env_vars(parent, child)

        assert original_parent == parent
        assert original_child == child
        assert merged == {
            "py_modules": [],
            "working_dir": "s3://path/test1.zip",
            "env_vars": {"test": "val", "trial": "val2"},
            "pip": ["numpy"],
            "excludes": ["my_file.txt"],
        }

    def test_merge_deep_copy(self):
        """Check that the env values are actually deep-copied."""

        parent_env_vars = {"parent": "pval"}
        child_env_vars = {"child": "cval"}

        parent = {"env_vars": parent_env_vars}
        child = {"env_vars": child_env_vars}
        original_parent = parent.copy()
        original_child = child.copy()

        merged = override_runtime_envs_except_env_vars(parent, child)
        assert merged["env_vars"] == {"parent": "pval", "child": "cval"}
        assert original_parent == parent
        assert original_child == child

    def test_merge_empty_env_vars(self):
        env_vars = {"test": "val", "trial": "val2"}

        non_empty = {"env_vars": {"test": "val", "trial": "val2"}}
        empty = {}

        assert (
            env_vars
            == override_runtime_envs_except_env_vars(non_empty, empty)["env_vars"]
        )
        assert (
            env_vars
            == override_runtime_envs_except_env_vars(empty, non_empty)["env_vars"]
        )
        assert {} == override_runtime_envs_except_env_vars(empty, empty)["env_vars"]

    def test_merge_env_vars(self):
        parent = {
            "py_modules": ["http://test.com/test0.zip", "s3://path/test1.zip"],
            "working_dir": "gs://path/test2.zip",
            "env_vars": {"parent": "pval", "override": "old"},
            "pip": ["pandas", "numpy"],
            "excludes": ["my_file.txt"],
        }

        child = {
            "py_modules": [],
            "working_dir": "s3://path/test1.zip",
            "env_vars": {"child": "cval", "override": "new"},
            "pip": ["numpy"],
        }

        merged = override_runtime_envs_except_env_vars(parent, child)
        assert merged == {
            "py_modules": [],
            "working_dir": "s3://path/test1.zip",
            "env_vars": {"parent": "pval", "child": "cval", "override": "new"},
            "pip": ["numpy"],
            "excludes": ["my_file.txt"],
        }

    def test_inheritance_regression(self):
        """Check if the general Ray runtime_env inheritance behavior matches.

        override_runtime_envs_except_env_vars should match the general Ray
        runtime_env inheritance behavior. This test checks if that behavior
        has changed, which would indicate a regression in
        override_runtime_envs_except_env_vars. If the runtime_env inheritance
        behavior changes, override_runtime_envs_except_env_vars should also
        change to match.
        """

        with ray.init(
            runtime_env={
                "py_modules": [TEST_DAG_PINNED_URI],
                "env_vars": {"var1": "hello"},
            }
        ):

            @ray.remote
            def check_module():
                # Check that Ray job's py_module loaded correctly
                from conditional_dag import serve_dag  # noqa: F401

                return os.getenv("var1")

            assert ray.get(check_module.remote()) == "hello"

            @ray.remote(
                runtime_env={
                    "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
                    "env_vars": {"var2": "world"},
                }
            )
            def test_task():
                with pytest.raises(ImportError):
                    # Check that Ray job's py_module was overwritten
                    from conditional_dag import serve_dag  # noqa: F401

                from test_env.shallow_import import ShallowClass

                if ShallowClass()() == "Hello shallow world!":
                    return os.getenv("var1") + " " + os.getenv("var2")

            assert ray.get(test_task.remote()) == "hello world"


class TestSnakeToCamelCase:
    def test_empty(self):
        assert "" == snake_to_camel_case("")

    def test_single_word(self):
        assert snake_to_camel_case("oneword") == "oneword"

    def test_multiple_words(self):
        assert (
            snake_to_camel_case("there_are_multiple_words_in_this_phrase")
            == "thereAreMultipleWordsInThisPhrase"
        )

    def test_single_char_words(self):
        assert snake_to_camel_case("this_is_a_test") == "thisIsATest"

    def test_leading_capitalization(self):
        """If the leading character is already capitalized, leave it capitalized."""

        assert snake_to_camel_case("Leading_cap") == "LeadingCap"

    def test_leading_alphanumeric(self):
        assert (
            snake_to_camel_case("check_@lphanum3ric_©har_behavior")
            == "check@lphanum3ric©harBehavior"
        )

    def test_embedded_capitalization(self):
        assert snake_to_camel_case("check_eMbEDDed_caPs") == "checkEMbEDDedCaPs"

    def test_mixed_caps_alphanumeric(self):
        assert (
            snake_to_camel_case("check_3Mb3DD*d_©a!s_behAvior_Here_wIth_MIxed_cAPs")
            == "check3Mb3DD*d©a!sBehAviorHereWIthMIxedCAPs"
        )

    def test_leading_underscore(self):
        """Should strip leading underscores."""

        assert snake_to_camel_case("_leading_underscore") == "leadingUnderscore"

    def test_trailing_underscore(self):
        """Should strip trailing underscores."""

        assert snake_to_camel_case("trailing_underscore_") == "trailingUnderscore"

    def test_leading_and_trailing_underscores(self):
        """Should strip leading and trailing underscores"""

        assert snake_to_camel_case(f"{'_' * 5}hello__world{'_' * 10}") == "helloWorld"

    def test_double_underscore(self):
        """Should treat repeated underscores as single underscore."""

        assert snake_to_camel_case("double__underscore") == "doubleUnderscore"
        assert snake_to_camel_case(f"many{'_' * 30}underscore") == "manyUnderscore"


def test_get_head_node_id():
    """Test get_head_node_id() returning the correct head node id.

    When there are woker node, dead head node, and other alive head nodes,
    get_head_node_id() should return the node id of the first alive head node.
    When there are no alive head nodes, get_head_node_id() should raise assertion error.
    """
    nodes = [
        {"NodeID": "worker_node1", "Alive": True, "Resources": {"CPU": 1}},
        {
            "NodeID": "dead_head_node1",
            "Alive": False,
            "Resources": {"CPU": 1, HEAD_NODE_RESOURCE_NAME: 1.0},
        },
        {
            "NodeID": "alive_head_node1",
            "Alive": True,
            "Resources": {"CPU": 1, HEAD_NODE_RESOURCE_NAME: 1.0},
        },
        {
            "NodeID": "alive_head_node2",
            "Alive": True,
            "Resources": {"CPU": 1, HEAD_NODE_RESOURCE_NAME: 1.0},
        },
    ]
    with patch("ray.nodes", return_value=nodes):
        assert get_head_node_id() == "alive_head_node1"

    with patch("ray.nodes", return_value=[]):
        with pytest.raises(AssertionError):
            get_head_node_id()


def test_calculate_remaining_timeout():
    # Always return `None` or negative value.
    assert (
        calculate_remaining_timeout(
            timeout_s=None,
            start_time_s=100,
            curr_time_s=101,
        )
        is None
    )

    assert (
        calculate_remaining_timeout(
            timeout_s=-1,
            start_time_s=100,
            curr_time_s=101,
        )
        == -1
    )

    # Return delta from start.
    assert (
        calculate_remaining_timeout(
            timeout_s=10,
            start_time_s=100,
            curr_time_s=101,
        )
        == 9
    )

    assert (
        calculate_remaining_timeout(
            timeout_s=100,
            start_time_s=100,
            curr_time_s=101.1,
        )
        == 98.9
    )

    # Never return a negative timeout once it has elapsed.
    assert (
        calculate_remaining_timeout(
            timeout_s=10,
            start_time_s=100,
            curr_time_s=111,
        )
        == 0
    )


def test_get_all_live_placement_group_names(ray_instance):
    """Test the logic to parse the Ray placement group table.

    The test contains three cases:
    - A named placement group that was created and is still alive ("pg2").
    - A named placement group that's still being scheduled ("pg3").
    - An unnamed placement group.

    Only "pg2" and "pg3" should be returned as live placement group names.
    """

    # Named placement group that's been removed (should not be returned).
    pg1 = ray.util.placement_group([{"CPU": 0.1}], name="pg1")
    ray.util.remove_placement_group(pg1)

    # Named, detached placement group that's been removed (should not be returned).
    pg2 = ray.util.placement_group([{"CPU": 0.1}], name="pg2", lifetime="detached")
    ray.util.remove_placement_group(pg2)

    # Named placement group that's still alive (should be returned).
    pg3 = ray.util.placement_group([{"CPU": 0.1}], name="pg3")
    assert pg3.wait()

    # Named, detached placement group that's still alive (should be returned).
    pg4 = ray.util.placement_group([{"CPU": 0.1}], name="pg4", lifetime="detached")
    assert pg4.wait()

    # Named placement group that's being scheduled (should be returned).
    pg5 = ray.util.placement_group([{"CPU": 1000}], name="pg5")
    assert not pg5.wait(timeout_seconds=0.001)

    # Named, detached placement group that's being scheduled (should be returned).
    pg6 = ray.util.placement_group([{"CPU": 1000}], name="pg6", lifetime="detached")
    assert not pg6.wait(timeout_seconds=0.001)

    # Unnamed placement group (should not be returned).
    pg7 = ray.util.placement_group([{"CPU": 0.1}])
    assert pg7.wait()

    # Unnamed, detached placement group (should not be returned).
    pg8 = ray.util.placement_group([{"CPU": 0.1}], lifetime="detached")
    assert pg8.wait()

    assert set(get_all_live_placement_group_names()) == {"pg3", "pg4", "pg5", "pg6"}


def test_is_running_in_asyncio_loop_false():
    assert is_running_in_asyncio_loop() is False


@pytest.mark.asyncio
async def test_is_running_in_asyncio_loop_true():
    assert is_running_in_asyncio_loop() is True

    async def check():
        return is_running_in_asyncio_loop()

    # Verify that it also works in a task.
    assert await asyncio.ensure_future(check()) is True


def test_get_current_actor_id(ray_instance):
    @ray.remote
    class A:
        def call_get_current_actor_id(self):
            return get_current_actor_id()

    a = A.remote()
    actor_id = ray.get(a.call_get_current_actor_id.remote())
    assert len(actor_id) > 0
    assert actor_id != "DRIVER"

    assert get_current_actor_id() == "DRIVER"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
