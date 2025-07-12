import inspect
import unittest

from ray.train.v2._internal.execution.context import TrainContext
from ray.train.v2._internal.execution.local_testing_context import (
    LocalTestingTrainContext,
)


class TestLocalTestingTrainContext(unittest.TestCase):
    def test_same_public_methods_as_train_context(self):
        """Test that LocalTestingTrainContext has the same public methods as TrainContext."""
        # Get all public methods from TrainContext
        train_context_methods = set()
        for name, method in inspect.getmembers(
            TrainContext, predicate=inspect.isfunction
        ):
            if not name.startswith("_"):  # Include only public methods
                train_context_methods.add(name)

        # Get all public methods from LocalTestingTrainContext
        local_testing_context_methods = set()
        for name, method in inspect.getmembers(
            LocalTestingTrainContext, predicate=inspect.isfunction
        ):
            if not name.startswith("_"):  # Include only public methods
                local_testing_context_methods.add(name)

        # Check that LocalTestingTrainContext has all public methods that TrainContext has
        missing_methods = train_context_methods - local_testing_context_methods
        self.assertEqual(
            missing_methods,
            set(),
            f"LocalTestingTrainContext is missing these public methods: {missing_methods}",
        )

    def test_same_method_signatures(self):
        """Test that LocalTestingTrainContext methods have the same signatures as TrainContext."""
        # Get all public methods from both classes
        train_context_methods = {}
        for name, method in inspect.getmembers(
            TrainContext, predicate=inspect.isfunction
        ):
            if not name.startswith("_"):  # Include only public methods
                train_context_methods[name] = method

        local_testing_context_methods = {}
        for name, method in inspect.getmembers(
            LocalTestingTrainContext, predicate=inspect.isfunction
        ):
            if not name.startswith("_"):  # Include only public methods
                local_testing_context_methods[name] = method

        # Check method signatures for common methods
        for method_name in train_context_methods:
            if method_name in local_testing_context_methods:
                train_sig = inspect.signature(train_context_methods[method_name])
                local_sig = inspect.signature(
                    local_testing_context_methods[method_name]
                )

                # Compare parameter names (excluding 'self')
                train_params = list(train_sig.parameters.keys())[1:]  # Skip 'self'
                local_params = list(local_sig.parameters.keys())[1:]  # Skip 'self'

                self.assertEqual(
                    train_params,
                    local_params,
                    f"Method {method_name} has different parameter names: "
                    f"TrainContext: {train_params}, LocalTestingTrainContext: {local_params}",
                )

    def test_local_testing_context_functionality(self):
        """Test basic functionality of LocalTestingTrainContext."""
        context = LocalTestingTrainContext()

        # Test basic attributes
        self.assertEqual(context.get_experiment_name(), "local_testing")
        self.assertEqual(context.get_world_rank(), 0)
        self.assertEqual(context.get_world_size(), 1)
        self.assertEqual(context.get_local_rank(), 0)
        self.assertEqual(context.get_local_world_size(), 1)
        self.assertEqual(context.get_node_rank(), 0)

        # Test no-op methods
        self.assertIsNone(context.get_storage())
        self.assertIsNone(context.get_result_queue())
        self.assertIsNone(context.get_synchronization_actor())
        self.assertIsNone(context.get_checkpoint())
        self.assertIsInstance(context.get_context_callbacks(), list)
        self.assertEqual(len(context.get_context_callbacks()), 0)

        # Test dataset shard
        shard = context.get_dataset_shard("test_dataset")
        self.assertIsNotNone(shard)

        # Test report functionality (should be no-op)
        try:
            context.report({"loss": 0.5, "epoch": 1})
            context.report(
                {"loss": 0.3, "epoch": 2}, checkpoint={"model_state": "test"}
            )
        except Exception as e:
            self.fail(f"Report method should not raise exceptions: {e}")


if __name__ == "__main__":
    unittest.main()
