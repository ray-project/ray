"""Check the small TLS fixture's reader connection evidence."""

import json
import logging
import os
import shlex
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


@pytest.mark.parametrize("failure", ["logged_tls", "hostname", "unrelated"])
def test_factory_records_only_this_readers_connection(tmp_path, monkeypatch, failure):
    directory = tmp_path

    logger = logging.getLogger("thrift.transport.TSocket")
    old_level, old_propagate = logger.level, logger.propagate
    secret = "secret-error-marker"

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

    factory = utils._make_auth_factory(connect, directory)
    expected_error: tuple[type[Exception], ...] = (RuntimeError, OSError)
    with pytest.raises(expected_error):
        factory()
    record_text = next(directory.glob("*.json")).read_text()
    record = json.loads(record_text)
    assert record["complete"] is True
    assert record["pid"] == os.getpid()
    assert record["connected"] is False
    assert record["certificate_error"] is (failure != "unrelated")
    assert record["verify_code"] == (19 if failure == "logged_tls" else None)
    assert record["hostname_mismatch"] is (failure == "hostname")
    assert secret not in record_text
    assert (logger.level, logger.propagate) == (old_level, old_propagate)


@pytest.mark.parametrize("damage", ["missing", "unfinished"])
def test_rejects_missing_or_invalid_reader_evidence(tmp_path, damage):
    if damage != "missing":
        path = tmp_path / "event.json"
        path.write_text(json.dumps({"complete": False}))
    with pytest.raises(ValueError):
        utils._read_evidence(tmp_path)


@pytest.mark.parametrize(
    "mode,exit_code,summary",
    [
        ("ordinary", 0, "3 skipped"),
        ("normal_main", 0, "test_hive_tls"),
        ("entry", 1, "3 skipped"),
        ("one_skip", 1, "2 passed, 1 skipped"),
        ("collect_only", 1, "3 tests collected"),
        ("partial", 4, "TLS suite did not collect exactly the expected cases"),
        ("all_pass", 0, "3 passed"),
        ("interrupted", 2, "guard status: 2"),
    ],
)
def test_tls_opt_in_and_entry_guard(mode, exit_code, summary, tmp_path):
    """Exercise pytest collection and the real entry guard without Hive or Ray."""
    target = Path(__file__).with_name("test_hive_integration.py").resolve()
    root = tmp_path / "pytest root"
    root.mkdir()
    cases = root / "test_cases.py"
    cases.write_text(
        "import os, pytest\n"
        "@pytest.mark.parametrize('scenario', "
        "['table', 'untrusted_ca_driver', 'hostname_worker'])\n"
        "def test_hive_tls(scenario):\n"
        "    if scenario == 'table' and os.environ['MODE'] == 'one_skip':\n"
        "        pytest.skip('simulated skip')\n"
        "    if scenario == 'table' and os.environ['MODE'] == 'interrupted':\n"
        "        pytest.exit('controlled exit', returncode=2)\n"
    )
    bootstrap = textwrap.dedent(
        """\
        import os, runpy, sys, types
        import pytest

        arrow = types.ModuleType('pyarrow')
        arrow.schema = lambda fields: fields
        arrow.int64 = arrow.string = lambda: None
        sys.modules['pyarrow'] = arrow
        sys.modules['certifi'] = None
        for name in ('ray', 'ray.data'):
            module = types.ModuleType(name)
            module.__path__ = []
            sys.modules[name] = module
            if '.' in name:
                parent, child = name.rsplit('.', 1)
                setattr(sys.modules[parent], child, module)
        module = types.ModuleType('_hive_test_utils')
        module._make_auth_factory = module._read_evidence = lambda *args: None
        sys.modules['ray.data.tests.datasource._hive_test_utils'] = module
        module = types.ModuleType('hive_hs2')
        module._connect = lambda *args: None
        sys.modules['ray.data._internal.datasource_v2.formats.hive.hive_hs2'] = module

        target, mode, cases = sys.argv[1:]
        if mode == 'ordinary':
            sys.exit(pytest.main(['-q', target + '::test_hive_tls']))
        if mode == 'normal_main':
            os.environ.pop('RAY_HIVE_TEST_HOST', None)
        else:
            os.environ['RAY_HIVE_TLS_SUITE'] = '1'
            if mode in ('one_skip', 'all_pass', 'interrupted'):
                os.environ['RAY_HIVE_CA_PEM_B64'] = 'configured'
                original_main = pytest.main
                def run_cases(args, plugins=None):
                    guard = plugins[0]
                    finish = guard.pytest_sessionfinish
                    def observe_guard(session, exitstatus):
                        print('guard status:', int(exitstatus), flush=True)
                        finish(session, exitstatus)
                    guard.pytest_sessionfinish = observe_guard
                    return original_main(['-v', cases], plugins=plugins)
                pytest.main = run_cases
        runpy.run_path(target, run_name='__main__')
        """
    )
    env = os.environ.copy()
    env.pop("RAY_HIVE_CA_PEM_B64", None)
    env.pop("RAY_HIVE_TLS_SUITE", None)
    env.update(
        RAY_HIVE_TEST_HOST="hiveserver2",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PYTEST_ADDOPTS=(
            f"--noconftest -p no:cacheprovider -c {os.devnull} "
            f"--rootdir={shlex.quote(str(root))} --disable-warnings"
        ),
        PYTHONDONTWRITEBYTECODE="1",
        MODE=mode,
    )
    if mode == "one_skip":
        env["PYTEST_ADDOPTS"] += " --no-summary"
    if mode == "collect_only":
        env["PYTEST_ADDOPTS"] += " --collect-only"
    if mode == "partial":
        env["PYTEST_ADDOPTS"] += " -k table"
    result = subprocess.run(
        [sys.executable, "-c", bootstrap, str(target), mode, str(cases)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert summary in result.stdout + result.stderr, result.stdout + result.stderr
    if mode == "normal_main":
        for case in ("table", "untrusted_ca_driver", "hostname_worker"):
            assert f"test_hive_tls[{case}] SKIPPED" in result.stdout
    if mode == "one_skip":
        assert "guard status: 0" in result.stdout


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
