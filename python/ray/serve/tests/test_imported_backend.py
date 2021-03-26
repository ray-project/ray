import ray
from ray import serve
from ray.serve.config import BackendConfig


def test_imported_backend(serve_instance):
    config = BackendConfig(user_config="config", max_batch_size=2)
    serve.create_backend(
        "imported",
        "ray.serve.utils.MockImportedBackend",
        "input_arg",
        config=config)
    serve.create_endpoint("imported", backend="imported")

    # Basic sanity check.
    handle = serve.get_handle("imported")
    assert ray.get(handle.remote()) == {"arg": "input_arg", "config": "config"}

    # Check that updating backend config works.
    serve.update_backend_config(
        "imported", BackendConfig(user_config="new_config"))
    assert ray.get(handle.remote()) == {
        "arg": "input_arg",
        "config": "new_config"
    }

    # Check that other call methods work.
    handle = handle.options(method_name="other_method")
    assert ray.get(handle.remote("hello")) == "hello"

    # Check that functions work as well.
    serve.create_backend(
        "imported_func",
        "ray.serve.utils.mock_imported_function",
        config=BackendConfig(max_batch_size=2))
    serve.create_endpoint("imported_func", backend="imported_func")
    handle = serve.get_handle("imported_func")
    assert ray.get(handle.remote("hello")) == "hello"
