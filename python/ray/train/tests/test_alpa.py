import jax


def test_import_jax():
    assert jax.numpy.ones(10).any()
    
def test_import_alpa():
    import alpa
    assert alpa.__version__
    