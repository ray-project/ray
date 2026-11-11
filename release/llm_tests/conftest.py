def pytest_configure(config):
    for requirement in ("2GPU", "3GPU", "4GPU", "2x_1GPU", "2x_2GPU"):
        config.addinivalue_line(
            "markers", f"requires_{requirement}: required H100 cluster topology"
        )
