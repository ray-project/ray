import sys
import pytest
import random
import itertools
from typing import Dict, List

from ray import serve
from ray.serve.config import DeploymentConfig
from ray.serve.deployment import deployment_to_schema, schema_to_deployment


def get_random_dict_combos(d: Dict, n: int) -> List[Dict]:
    """Gets n random combinations of dictionary d.

    Returns:
        List of dictionary combinations of lengths from 0 to len(d). List
        contains n random combinations of d's elements.
    """

    # Shuffle dictionary without modifying original dictionary
    d = dict(random.sample(list(d.items()), len(d)))

    combos = []

    # Sample random combos of random size
    subset_sizes = list(range(len(d) + 1))
    random.shuffle(subset_sizes)

    for subset_size in subset_sizes:
        subset_combo_iterator = map(
            dict, itertools.combinations(d.items(), subset_size)
        )
        if len(combos) < n:
            subset_combos = list(
                itertools.islice(subset_combo_iterator, n - len(combos))
            )
            combos.extend(subset_combos)
        else:
            break

    return combos


class TestGetDictCombos:
    def test_empty(self):
        assert get_random_dict_combos({}, 1) == [{}]

    def test_basic(self):
        d = {"a": 1, "b": 2, "c": 3}
        combos = get_random_dict_combos(d, 8)

        # Sort combos for comparison (sort by length, break ties by value sum)
        combos.sort(key=lambda d: len(d) * 100 + sum(d.values()))

        assert combos == [
            # Dictionaries of length 0
            {},
            # Dictionaries of length 1
            *({"a": 1}, {"b": 2}, {"c": 3}),
            # Dictionaries of length 2
            *({"a": 1, "b": 2}, {"a": 1, "c": 3}, {"b": 2, "c": 3}),
            # Dictionaries of length 3
            {"a": 1, "b": 2, "c": 3},
        ]

    def test_len(self):
        d = {i: i + 1 for i in range(50)}
        assert len(get_random_dict_combos(d, 1000)) == 1000

    def test_randomness(self):
        d = {i: i + 1 for i in range(1000)}
        combo1 = get_random_dict_combos(d, 1000)[0]
        combo2 = get_random_dict_combos(d, 1000)[0]
        assert combo1 != combo2


class TestDeploymentOptions:

    # Deployment options mapped to sample input
    deployment_options = {
        "name": "test",
        "version": "abcd",
        "num_replicas": 1,
        "init_args": (),
        "init_kwargs": {},
        "route_prefix": "/",
        "ray_actor_options": {},
        "user_config": {},
        "max_concurrent_queries": 10,
        "autoscaling_config": None,
        "graceful_shutdown_wait_loop_s": 10,
        "graceful_shutdown_timeout_s": 10,
        "health_check_period_s": 10,
        "health_check_timeout_s": 10,
    }

    deployment_option_combos = get_random_dict_combos(deployment_options, 1000)

    @pytest.mark.parametrize("options", deployment_option_combos)
    def test_user_configured_option_names(self, options: Dict):
        """Check that user_configured_option_names tracks the correct options.

        Args:
            options: Maps deployment option strings (e.g. "name",
                "num_replicas", etc.) to sample inputs. Pairs come from
                TestDeploymentOptions.deployment_options.
        """

        @serve.deployment(**options)
        def f():
            pass

        assert f._config.user_configured_option_names == set(options.keys())

    @pytest.mark.parametrize("options", deployment_option_combos)
    def test_user_configured_option_names_schematized(self, options: Dict):
        """Check user_configured_option_names after schematization.

        Args:
            options: Maps deployment option strings (e.g. "name",
                "num_replicas", etc.) to sample inputs. Pairs come from
                TestDeploymentOptions.deployment_options.
        """

        # Some options won't be considered user-configured after schematization
        # since the schema doesn't track them.
        untracked_options = ["name", "version", "init_args", "init_kwargs"]

        for option in untracked_options:
            if option in options:
                del options[option]

        @serve.deployment(**options)
        def f():
            pass

        schematized_deployment = deployment_to_schema(f)
        deschematized_deployment = schema_to_deployment(schematized_deployment)

        # Don't track name in the deschematized deployment since it's optional
        # in deployment decorator but required in schema, leading to
        # inconsistent behavior.
        if "name" in deschematized_deployment._config.user_configured_option_names:
            deschematized_deployment._config.user_configured_option_names.remove("name")

        assert deschematized_deployment._config.user_configured_option_names == set(
            options.keys()
        )

    @pytest.mark.parametrize("options", deployment_option_combos)
    def test_user_configured_option_names_serialized(self, options: Dict):
        """Check user_configured_option_names after serialization.

        Args:
            options: Maps deployment option strings (e.g. "name",
                "num_replicas", etc.) to sample inputs. Pairs come from
                TestDeploymentOptions.deployment_options.
        """

        # init_kwargs requires independent serialization, so we omit it.
        if "init_kwargs" in options:
            del options["init_kwargs"]

        @serve.deployment(**options)
        def f():
            pass

        serialized_config = f._config.to_proto_bytes()
        deserialized_config = DeploymentConfig.from_proto_bytes(serialized_config)

        assert deserialized_config.user_configured_option_names == set(options.keys())

    @pytest.mark.parametrize(
        "option",
        [
            "num_replicas",
            "route_prefix",
            "autoscaling_config",
            "user_config",
        ],
    )
    def test_nullable_options(self, option: str):
        """Check that nullable options can be set to None."""

        deployment_options = {option: None}

        # One of "num_replicas" or "autoscaling_config" must be provided.
        if option == "num_replicas":
            deployment_options["autoscaling_config"] = {
                "min_replicas": 1,
                "max_replicas": 5,
                "target_num_ongoing_requests_per_replica": 5,
            }
        elif option == "autoscaling_config":
            deployment_options["num_replicas"] = 5

        # Deployment should be created without error.
        @serve.deployment(**deployment_options)
        def f():
            pass

    @pytest.mark.parametrize("options", deployment_option_combos)
    def test_options(self, options):
        """Check that updating options also updates user_configured_options_names."""

        @serve.deployment
        def f():
            pass

        f = f.options(**options)
        assert f._config.user_configured_option_names == set(options.keys())

        @serve.deployment
        def g():
            pass

        g.set_options(**options)
        assert g._config.user_configured_option_names == set(options.keys())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
