import ray
from ray.serve.backends import ImportedBackend
from ray.serve.config import BackendConfig


def test_imported_backend(serve_instance):
    client = serve_instance

    backend_class = ImportedBackend("ray.serve.utils.MockImportedBackend")
    config = BackendConfig(user_config="config")
    client.create_backend(
        "imported", backend_class, "input_arg", config=config)
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
