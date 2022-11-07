import pytest
import ray._private.ray_constants as ray_constants


@pytest.fixture
def set_override_dashboard_url(monkeypatch, request):
    override_url = getattr(request, "param", "https://external_dashboard_url")
    with monkeypatch.context() as m:
        if override_url:
            m.setenv(
                ray_constants.RAY_OVERRIDE_DASHBOARD_URL,
                override_url,
            )
        yield
