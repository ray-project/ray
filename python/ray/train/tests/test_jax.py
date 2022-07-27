import jax


def test_import_jax():
    assert jax.numpy.ones(10).any()