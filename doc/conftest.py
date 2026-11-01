def pytest_configure(config):
    for requirement in ("2GPU", "4GPU"):
        config.addinivalue_line(
            "markers", f"requires_{requirement}: required H100 cluster topology"
        )
