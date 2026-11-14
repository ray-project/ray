"""Check the small TLS fixture's reader connection evidence."""

import json
import logging
import os
import ssl
import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray.data.tests.datasource import _hive_test_utils as utils


def test_certificate_error_traverses_thrift_inner_and_cause():
    cert_error = ssl.SSLCertVerificationError("certificate marker")
    wrapped = SimpleNamespace(inner=cert_error)
    assert utils._certificate_error(wrapped) is cert_error
    error = RuntimeError("reader")
    error.__cause__ = cert_error
    assert utils._certificate_error(error) is cert_error
    assert utils._certificate_error(RuntimeError("unrelated")) is None


@pytest.mark.parametrize(
    "failure", ["logged_tls", "wrapped_tls", "hostname", "unrelated", "success"]
)
def test_factory_records_only_this_readers_connection(tmp_path, monkeypatch, failure):
    import ray

    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "node")
    )
    directory = tmp_path

    logger = logging.getLogger("thrift.transport.TSocket")
    old_level, old_propagate = logger.level, logger.propagate
    secret = "secret-error-marker"
    connection = object()

    # Python 3.13 removed ssl.match_hostname; model Thrift's Python 3.10
    # callback while still exercising this helper on newer test hosts.
    def match_hostname(cert, hostname):
        raise ssl.CertificateError("hostname mismatch")

    monkeypatch.setattr(ssl, "match_hostname", match_hostname, raising=False)

    class TransportFailure(RuntimeError):
        def __init__(self, inner):
            super().__init__("transport failed")
            self.inner = inner

    def connect():
        assert (
            json.loads(next(directory.glob("*.json")).read_text())["complete"] is False
        )
        if failure == "logged_tls":
            try:
                error = ssl.SSLCertVerificationError(secret)
                error.verify_code = 19
                raise error
            except ssl.SSLCertVerificationError:
                logger.info("certificate failed", exc_info=True)
            raise RuntimeError("transport failed")
        if failure == "wrapped_tls":
            error = ssl.SSLCertVerificationError(secret)
            error.verify_code = 62
            raise TransportFailure(error)
        if failure == "hostname":
            try:
                match_hostname({"subjectAltName": (("DNS", "hiveserver2"),)}, "wrong")
            except ssl.CertificateError as error:
                raise TransportFailure(error) from error
        if failure == "unrelated":

            def log_other_thread():
                try:
                    raise ssl.SSLCertVerificationError(secret)
                except ssl.SSLCertVerificationError:
                    logger.info("unrelated certificate failure", exc_info=True)

            thread = threading.Thread(target=log_other_thread)
            thread.start()
            thread.join()
            raise OSError("unrelated connection failure")
        return connection

    factory = utils._make_auth_factory(connect, directory)
    if failure == "success":
        assert factory() is connection
    else:
        expected_error: tuple[type[Exception], ...] = (RuntimeError, OSError)
        with pytest.raises(expected_error):
            factory()
    record_text = next(directory.glob("*.json")).read_text()
    record = json.loads(record_text)
    assert record["complete"] is True
    assert record["node"] == "node"
    assert record["connected"] is (failure == "success")
    assert record["certificate_error"] is (
        failure in {"logged_tls", "wrapped_tls", "hostname"}
    )
    assert record["verify_code"] == {"logged_tls": 19, "wrapped_tls": 62}.get(failure)
    assert record["hostname_mismatch"] is (failure == "hostname")
    assert secret not in record_text
    assert (logger.level, logger.propagate) == (old_level, old_propagate)


def test_certificate_log_from_another_thread_is_not_attributed():
    handler = utils._CertificateFailure()
    record = logging.LogRecord(
        "thrift.transport.TSocket", logging.INFO, "", 0, "", (), None
    )
    record.exc_info = (
        ssl.SSLCertVerificationError,
        ssl.SSLCertVerificationError("bad"),
        None,
    )
    record.thread = threading.get_ident() + 1
    handler.emit(record)
    assert handler.error is None


@pytest.mark.parametrize("checkout", ["/ray", "/rayci"])
def test_reader_evidence_matches_its_checkout(tmp_path, checkout):
    data_dir = Path(checkout) / "python/ray/data"
    test_file = data_dir / "tests/datasource/test_hive_auth_integration.py"
    source = data_dir / "_internal/datasource_v2/formats/hive/hive_hs2.py"
    record = dict(complete=True, node="node", source=str(source.resolve()))
    event = tmp_path / "event.json"
    event.write_text(json.dumps(record))
    assert utils._read_evidence(tmp_path, {"node"}, test_file) == [record]

    for wrong_source in (
        source.with_name("another_hive_module.py"),
        Path(checkout + "-stale")
        / "python/ray/data/_internal/datasource_v2/formats/hive/hive_hs2.py",
    ):
        record["source"] = str(wrong_source.resolve())
        event.write_text(json.dumps(record))
        with pytest.raises(ValueError):
            utils._read_evidence(tmp_path, {"node"}, test_file)


@pytest.mark.parametrize(
    "damage", ["missing", "truncated", "unfinished", "node", "source"]
)
def test_rejects_missing_or_invalid_reader_evidence(tmp_path, damage):
    data_dir = tmp_path / "checkout/python/ray/data"
    test_file = data_dir / "tests/datasource/test_hive_auth_integration.py"
    if damage != "missing":
        record = dict(
            complete=damage != "unfinished",
            node="node",
            source=str(data_dir / "_internal/datasource_v2/formats/hive/hive_hs2.py"),
        )
        if damage in {"node", "source"}:
            record[damage] = "unexpected"
        path = tmp_path / "event.json"
        path.write_text(json.dumps(record) if damage != "truncated" else "{")
    with pytest.raises(ValueError):
        utils._read_evidence(tmp_path, {"node"}, test_file)


@pytest.mark.parametrize(
    "mode,exit_code,summary",
    [
        ("ordinary", 0, "3 skipped"),
        ("entry", 1, "3 skipped"),
        ("entry_no_summary", 1, "3 skipped"),
        ("one_skip", 1, "2 passed, 1 skipped"),
        ("collect_only", 1, "3 tests collected"),
        ("all_pass", 0, "3 passed"),
    ],
)
def test_tls_opt_in_and_entry_guard(mode, exit_code, summary):
    """Exercise pytest collection and the real entry guard without Hive or Ray."""
    target = Path(__file__).with_name("test_hive_auth_integration.py").resolve()
    bootstrap = textwrap.dedent(
        """\
        import os, runpy, sys, types
        import pytest

        arrow = types.ModuleType('pyarrow')
        arrow.schema = lambda fields: fields
        arrow.int64 = arrow.string = lambda: None
        sys.modules['pyarrow'] = arrow
        sys.modules['certifi'] = None
        for name in ('ray', 'ray.data', 'ray.data.tests', 'ray.data.tests.datasource'):
            module = types.ModuleType(name)
            module.__path__ = []
            sys.modules[name] = module
            if '.' in name:
                parent, child = name.rsplit('.', 1)
                setattr(sys.modules[parent], child, module)
        for name, attrs in (
            ('_hive_test_utils', ('_make_auth_factory', '_read_evidence')),
            ('test_hive_integration', ('_connection_options', '_make_connection_factory', '_temporary_table')),
        ):
            module = types.ModuleType(name)
            for attr in attrs:
                setattr(module, attr, lambda *args, **kwargs: None)
            sys.modules['ray.data.tests.datasource.' + name] = module

        target, mode = sys.argv[1:]
        if mode in ('entry', 'entry_no_summary', 'collect_only'):
            runpy.run_path(target, run_name='__main__')
        elif mode == 'ordinary':
            sys.exit(pytest.main(['-q', target]))
        else:
            os.environ['RAY_HIVE_CA_PEM_B64'] = 'configured'
            class StubSuite:
                @pytest.fixture
                def ray_start_cluster_enabled(self):
                    return None
                def pytest_collection_modifyitems(self, items):
                    for item in items:
                        if mode == 'one_skip' and item.name == 'test_hive_tls[table]':
                            item.obj = lambda **kwargs: pytest.skip('simulated skip')
                        else:
                            item.obj = lambda **kwargs: None
            original_main = pytest.main
            def main_with_skip(args, plugins=None):
                return original_main(args, plugins=[*(plugins or []), StubSuite()])
            pytest.main = main_with_skip
            runpy.run_path(target, run_name='__main__')
        """
    )
    env = os.environ.copy()
    env.pop("RAY_HIVE_CA_PEM_B64", None)
    env.update(
        RAY_HIVE_TEST_HOST="hiveserver2",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PYTEST_ADDOPTS=f"--noconftest -p no:cacheprovider -c {os.devnull} --disable-warnings",
        PYTHONDONTWRITEBYTECODE="1",
    )
    if mode == "entry_no_summary":
        env["PYTEST_ADDOPTS"] += " --no-summary"
    if mode == "collect_only":
        env["PYTEST_ADDOPTS"] += " --collect-only"
    result = subprocess.run(
        [sys.executable, "-c", bootstrap, str(target), mode],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert summary in result.stdout, result.stdout + result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
