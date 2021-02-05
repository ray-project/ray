import ray
from ray.serve.config import BackendConfig


def test_imported_backend(serve_instance):
    client = serve_instance

    config = BackendConfig(user_config="config", max_batch_size=2)
    client.create_backend(
        "imported",
        "ray.serve.utils.MockImportedBackend",
        "input_arg",
        config=config)
    client.create_endpoint("imported", backend="imported")

    # Basic sanity check.
    handle = client.get_handle("imported")
    assert ray.get(handle.remote()) == {"arg": "input_arg", "config": "config"}

    # Check that updating backend config works.
    client.update_backend_config(
        "imported", BackendConfig(user_config="new_config"))
    assert ray.get(handle.remote()) == {
        "arg": "input_arg",
        "config": "new_config"
    }

    # Check that other call methods work.
    handle = handle.options(method_name="other_method")
    assert ray.get(handle.remote("hello")) == "hello"

    # Check that functions work as well.
    client.create_backend(
        "imported_func",
        "ray.serve.utils.mock_imported_function",
        config=BackendConfig(max_batch_size=2))
    client.create_endpoint("imported_func", backend="imported_func")
    handle = client.get_handle("imported_func")
    assert ray.get(handle.remote("hello")) == "hello"
