"""Check the small TLS fixture's reader connection evidence."""

import json
import logging
import os
import shlex
import shutil
import ssl
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray.data.tests.datasource import _hive_test_utils as utils
from ray.data.tests.datasource.test_hive_integration import _EnsureCases

pytest_plugins = ["pytester"]


def test_certificate_error_traverses_thrift_inner_and_cause():
    cert_error = ssl.SSLCertVerificationError("certificate marker")
    wrapped = SimpleNamespace(inner=cert_error)
    assert utils._certificate_error(wrapped) is cert_error
    error = RuntimeError("reader")
    error.__cause__ = cert_error
    assert utils._certificate_error(error) is cert_error
    assert utils._certificate_error(RuntimeError("unrelated")) is None


def _failed_connection_record(tmp_path, connect, expected_error):
    logger = logging.getLogger("thrift.transport.TSocket")
    old_level, old_propagate = logger.level, logger.propagate

    def checked_connect():
        assert (
            json.loads(next(tmp_path.glob("*.json")).read_text())["complete"] is False
        )
        return connect()

    factory = utils._make_auth_factory(checked_connect, tmp_path)
    with pytest.raises(expected_error):
        factory()
    record_text = next(tmp_path.glob("*.json")).read_text()
    record = json.loads(record_text)
    assert record["complete"] is True
    assert record["pid"] == os.getpid()
    assert record["connected"] is False
    assert "secret-error-marker" not in record_text
    assert (logger.level, logger.propagate) == (old_level, old_propagate)
    return record


def test_factory_records_logged_certificate_error(tmp_path):
    logger = logging.getLogger("thrift.transport.TSocket")

    def connect():
        try:
            error = ssl.SSLCertVerificationError("secret-error-marker")
            error.verify_code = 19
            raise error
        except ssl.SSLCertVerificationError:
            logger.info("certificate failed", exc_info=True)
        raise RuntimeError("transport failed")

    record = _failed_connection_record(tmp_path, connect, RuntimeError)
    assert record["certificate_error"] is True
    assert record["verify_code"] == 19
    assert record["hostname_mismatch"] is False


def test_factory_records_hostname_mismatch(tmp_path, monkeypatch):
    # Model Thrift's Python 3.10 callback on hosts where it was removed.
    def match_hostname(cert, hostname):
        raise ssl.CertificateError("hostname mismatch")

    monkeypatch.setattr(ssl, "match_hostname", match_hostname, raising=False)

    class TransportFailure(RuntimeError):
        def __init__(self, inner):
            super().__init__("transport failed")
            self.inner = inner

    def connect():
        try:
            match_hostname({"subjectAltName": (("DNS", "hiveserver2"),)}, "wrong")
        except ssl.CertificateError as error:
            raise TransportFailure(error) from error

    record = _failed_connection_record(tmp_path, connect, RuntimeError)
    assert record["certificate_error"] is True
    assert record["verify_code"] is None
    assert record["hostname_mismatch"] is True


def test_factory_ignores_other_threads_certificate_error(tmp_path):
    logger = logging.getLogger("thrift.transport.TSocket")

    def connect():
        def log_other_thread():
            try:
                raise ssl.SSLCertVerificationError("secret-error-marker")
            except ssl.SSLCertVerificationError:
                logger.info("unrelated certificate failure", exc_info=True)

        thread = threading.Thread(target=log_other_thread)
        thread.start()
        thread.join()
        raise OSError("unrelated connection failure")

    record = _failed_connection_record(tmp_path, connect, OSError)
    assert record["certificate_error"] is False
    assert record["verify_code"] is None
    assert record["hostname_mismatch"] is False


def test_rejects_missing_reader_evidence(tmp_path):
    with pytest.raises(ValueError):
        utils._read_evidence(tmp_path)


def test_rejects_unfinished_reader_evidence(tmp_path):
    path = tmp_path / "event.json"
    path.write_text(json.dumps({"complete": False}))
    with pytest.raises(ValueError):
        utils._read_evidence(tmp_path)


@pytest.mark.parametrize(
    "first_case,options,expected_exit,summary",
    [
        pytest.param("pass", (), 0, "3 passed", id="all_pass"),
        pytest.param(
            "pytest.skip('test skip')",
            ("--no-summary",),
            1,
            "2 passed, 1 skipped",
            id="call_skip",
        ),
        pytest.param(
            "pass", ("--collect-only",), 1, "3 tests collected", id="collect_only"
        ),
    ],
)
def test_tls_guard(pytester, first_case, options, expected_exit, summary):
    pytester.makepyfile(
        f"import pytest\n"
        f"def test_verified_table(): {first_case}\n"
        "def test_untrusted_ca_driver(): pass\n"
        "def test_hostname_worker(): pass\n"
    )
    result = pytester.runpytest("-q", *options, plugins=[_EnsureCases()])
    assert result.ret == expected_exit
    assert summary in result.stdout.str()


@pytest.mark.parametrize(
    "args,settings,expected_exit",
    [
        pytest.param(
            ("-m", "pytest", "-q", "{target}::TestHiveTLS"), {}, 0, id="ordinary"
        ),
        pytest.param(("{target}",), {"RAY_HIVE_TLS_SUITE": "1"}, 1, id="entry"),
    ],
)
def test_tls_entry_without_ca(args, settings, expected_exit, tmp_path):
    root = tmp_path / "TLS path with space"
    root.mkdir()
    target = root / "test_hive_integration.py"
    shutil.copy2(Path(__file__).with_name(target.name), target)
    (root / "sitecustomize.py").write_text(
        "import ray, ray.data, sys\nsys.modules['certifi'] = None\n"
        "print('certifi blocked', file=sys.stderr)\n"
    )
    env = os.environ.copy()
    env.pop("RAY_HIVE_CA_PEM_B64", None)
    env.pop("RAY_HIVE_TLS_SUITE", None)
    env.update(
        RAY_HIVE_TEST_HOST="hiveserver2",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PYTEST_ADDOPTS=(
            "--noconftest -p no:cacheprovider -c /dev/null --disable-warnings "
            f"--rootdir={shlex.quote(str(root))}"
        ),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=os.pathsep.join((str(root), env.get("PYTHONPATH", ""))),
        **settings,
    )
    result = subprocess.run(
        [sys.executable, *(arg.format(target=target) for arg in args)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == expected_exit, result.stdout + result.stderr
    assert "3 skipped" in result.stdout, result.stdout + result.stderr
    assert "certifi blocked" in result.stderr, result.stdout + result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
