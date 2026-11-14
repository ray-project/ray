"""Observe the actual reader's TLS connection without recording RPCs."""

import json
import logging
import ssl
import threading
import uuid
from pathlib import Path


def _certificate_error(error):
    """Find a certificate error without trusting generic transport messages."""
    seen = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return current
        pending.extend(
            getattr(current, name, None)
            for name in ("__cause__", "__context__", "inner")
        )
    return None


def _hostname_mismatch(error):
    # Thrift 0.16 calls ssl.match_hostname after the verified TLS handshake;
    # its CertificateError has no OpenSSL verify_code.
    match_hostname = getattr(ssl, "match_hostname", None)
    trace = getattr(error, "__traceback__", None)
    while trace and match_hostname is not None:
        if trace.tb_frame.f_code is match_hostname.__code__:
            return True
        trace = trace.tb_next
    return False


class _CertificateFailure(logging.Handler):
    """Capture certificate errors from this synchronous Thrift connection only."""

    def __init__(self):
        super().__init__(logging.INFO)
        self.thread = threading.get_ident()
        self.error: ssl.SSLCertVerificationError | None = None

    def emit(self, record):
        if record.thread == self.thread and record.exc_info:
            self.error = _certificate_error(record.exc_info[1]) or self.error


def _make_auth_factory(factory, directory):
    """Record node and certificate failure for each actual reader connection."""

    def observed():
        import ray
        from ray.data._internal.datasource_v2.formats.hive import hive_hs2

        path = Path(directory) / f"{uuid.uuid4().hex}.json"
        evidence = dict(
            node=ray.get_runtime_context().get_node_id(),
            source=str(Path(hive_hs2.__file__).resolve()),
            complete=False,
            connected=False,
            certificate_error=False,
            hostname_mismatch=False,
            verify_code=None,
        )
        path.write_text(json.dumps(evidence) + "\n")
        logger = logging.getLogger("thrift.transport.TSocket")
        handler = _CertificateFailure()
        level, propagate = logger.level, logger.propagate
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        try:
            connection = factory()
        except Exception as exc:
            error = _certificate_error(exc) or handler.error
            evidence["certificate_error"] = error is not None
            evidence["verify_code"] = getattr(error, "verify_code", None)
            evidence["hostname_mismatch"] = _hostname_mismatch(error)
            raise
        else:
            evidence["connected"] = True
            return connection
        finally:
            logger.removeHandler(handler)
            logger.setLevel(level)
            logger.propagate = propagate
            evidence["complete"] = True
            path.write_text(json.dumps(evidence) + "\n")

    return observed


def _read_evidence(directory, nodes, test_file):
    records = [json.loads(path.read_text()) for path in Path(directory).glob("*.json")]
    if not records or {record["node"] for record in records} != set(nodes):
        raise ValueError("Missing or unexpected reader connection evidence")
    expected_source = (
        Path(test_file).resolve().parents[2]
        / "_internal/datasource_v2/formats/hive/hive_hs2.py"
    ).resolve()
    for record in records:
        if record["complete"] is not True or record["source"] != str(expected_source):
            raise ValueError("Incomplete or unexpected reader connection evidence")
    return records
