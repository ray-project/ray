"""Azure-backed ``DatabricksUnityCatalog`` Delta reads over a real data plane.

Azure coverage in ``test_catalog.py`` stops at ``resolve()``: the Databricks SDK
is mocked, the assertions check that ``AZURE_STORAGE_SAS_TOKEN`` lands in
``os.environ``, and no byte is ever read. That leaves the part that actually
matters untested -- whether a vended SAS gets a *reader* to the data, on the
driver and in the read tasks.

These tests drive the whole path instead: the real ``DatabricksUnityCatalog``
resolves a table, its real ``_creds_to_env`` / ``_apply_env`` deliver the
credential, and ``ray.data.read_delta`` fetches real bytes from real Azure blob
storage. The aggregations (``count`` / ``min`` / ``max``) are the point --
authenticating is necessary but not sufficient, the rows have to come back
*correct*.

The table is deliberately not trivial
-------------------------------------
It is written as ``_COMMIT_COUNT`` separate appends with a small
``target_file_size``, so it holds several ``_delta_log/*.json`` versions and
several Parquet data files rather than one of each. That is what makes the read
exercise anything: the driver has to replay the whole log to learn which files
are current, and the read tasks each have to authenticate separately for their
own files. A single-file single-version table would pass with one credential
reaching one process, which is exactly the bug being guarded against.
``test_table_shape_is_non_trivial`` asserts the shape rather than trusting it,
so the suite cannot silently decay into the trivial case.

Backends (see the ``azure_backend`` factory fixture)
---------------------------------------------------
``azurite``  (default)
    Storage is a local `Azurite <https://github.com/Azure/Azurite>`_ emulator,
    the way the rest of the Ray Data suite uses ``moto`` for S3. Only the
    Databricks *control plane* is faked -- ``_workspace_client`` is patched to
    return a ``GenerateTemporaryTableCredentialResponse`` carrying a genuine,
    signature-checked Azurite SAS and a genuine ``abfss://`` URL. Azurite
    validates that SAS, so the credential is load-bearing: strip it and the
    reads fail. Requires ``azurite`` on ``PATH`` (``npm install -g azurite``)
    and ``azure-storage-blob`` importable; skips cleanly otherwise. Azurite is
    a Node package, so it stays a local prerequisite, not a Python requirement.

``real``  (when the environment is configured)
    A real Azure Databricks workspace over real Azure storage. Nothing is
    mocked. Enabled by setting::

        RAY_TEST_AZURE_DATABRICKS_HOST=https://adb-XXXX.NN.azuredatabricks.net
        RAY_TEST_AZURE_DATABRICKS_TOKEN=dapi...
        RAY_TEST_AZURE_UC_DELTA_TABLE=main.ray_test.ray_azure_delta

    A *managed* table is enough -- credential vending works on
    Databricks-managed storage, which on Azure is ADLS Gen2, so the vended URL
    is still ``abfss://`` and the credential still an Azure user-delegation SAS.
    That avoids having to provision a storage account, an access connector, its
    role assignments, a storage credential and an external location.

    The table must hold ``_EXPECTED_COUNT`` rows -- ``id`` 0..``_EXPECTED_MAX``
    and ``v`` = ``"r{id}"`` -- written as several commits, so that the log has
    more than one version::

        CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>;
        CREATE TABLE <catalog>.<schema>.<table> (id BIGINT, v STRING) USING DELTA
          TBLPROPERTIES ('delta.enableDeletionVectors' = false);
        -- repeated over consecutive id ranges covering 0..._EXPECTED_MAX
        INSERT INTO <catalog>.<schema>.<table>
          SELECT id, concat('r', id) AS v FROM range(<lo>, <hi>);

    ``delta.enableDeletionVectors = false`` is not optional. Deletion vectors
    are on by default for Databricks managed tables, and deltalake cannot read a
    table that merely *declares* that reader feature through pyarrow datasets --
    which is the path ``read_delta`` takes -- so the read fails before it
    authenticates.

    The metastore also needs external data access enabled (an *account*-level
    setting: ``external_access_enabled``, not reachable from the workspace API),
    and the principal needs ``EXTERNAL USE SCHEMA`` on the schema on top of
    ``SELECT``. Without both, ``generate_temporary_table_credentials`` refuses
    rather than vending a SAS.

Expected failures
-----------------
Two of these fail today. They are written against the behaviour a correct
implementation must have, not against current behaviour, so that fixing the
code is what turns them green:

``test_delta_count_min_max_via_catalog``
``test_vended_credentials_reach_read_tasks``
    ``_apply_env`` seeds the cluster ``runtime_env`` only when Ray is *not* yet
    initialized. Any already-running cluster -- every case where a Dataset
    already exists -- leaves read tasks with no credential. AWS survives this
    because an ``S3FileSystem`` carrying the credentials is pickled into the
    tasks; Azure gets no filesystem, so the environment is its only channel and
    the workers end up with nothing.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

import ray
from ray.data.tests.conftest import *  # noqa: F401,F403
from ray.tests.conftest import *  # noqa: F401,F403

pa = pytest.importorskip("pyarrow")
pytest.importorskip("deltalake")
pytest.importorskip("databricks.sdk")

from databricks.sdk.service.catalog import (  # noqa: E402
    AzureUserDelegationSas,
    DataSourceFormat,
    GenerateTemporaryTableCredentialResponse,
    TableInfo,
)

from ray.data.catalog import (  # noqa: E402
    CatalogAccessMode,
    DatabricksUnityCatalog,
    ReaderFormat,
)

# Shape of the table both backends read. Split across several appends, each
# capped to a small file size, so the table has multiple log versions and
# multiple Parquet files -- see the module docstring for why that matters.
_ROWS_PER_COMMIT = 4_000
_COMMIT_COUNT = 3
_TARGET_FILE_SIZE = 64 * 1024

_EXPECTED_COUNT = _ROWS_PER_COMMIT * _COMMIT_COUNT
_EXPECTED_MIN = 0
_EXPECTED_MAX = _EXPECTED_COUNT - 1


def _reference_rows(start: int, count: int):
    return {
        "id": list(range(start, start + count)),
        "v": [f"r{i}" for i in range(start, start + count)],
    }


# Azurite's well-known development account. Not a secret -- it is published in
# Azure's own docs and is what every Azurite instance starts with.
_AZURITE_ACCOUNT = "devstoreaccount1"
_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)

_REAL_ENV = {
    "host": "RAY_TEST_AZURE_DATABRICKS_HOST",
    "token": "RAY_TEST_AZURE_DATABRICKS_TOKEN",
    "delta_table": "RAY_TEST_AZURE_UC_DELTA_TABLE",
}

# Every env var the catalog or the emulator plumbing might set, cleared between
# tests so one test's vended SAS can never satisfy the next one's read.
_AZURE_ENV_PREFIXES = ("AZURE_", "AZURITE_")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _AzuriteBackend:
    """Real Azure blob storage semantics, locally, with a faked control plane.

    Seeding uses the account key (an admin path a UC client would never have);
    the tests then read with nothing but the vended SAS, so what they exercise
    is exactly the credential the catalog hands over.
    """

    is_emulator = True

    def __init__(self, process, port, tmp_location):
        self._process = process
        self._tmp_location = tmp_location
        self.authority = f"127.0.0.1:{port}"
        # deltalake's object_store needs the account in the endpoint; pyarrow
        # takes the bare authority and adds the account itself.
        self.account_endpoint = f"http://{self.authority}/{_AZURITE_ACCOUNT}"
        self.container = f"c{uuid.uuid4().hex[:10]}"
        self._patcher = None
        self._sas = None

    # ---- lifecycle -----------------------------------------------------
    @classmethod
    def start(cls):
        exe = shutil.which("azurite")
        if exe is None:
            pytest.skip("azurite not installed (npm install -g azurite)")
        pytest.importorskip(
            "azure.storage.blob",
            reason="azure-storage-blob is needed to mint an Azurite SAS",
        )

        port = _free_port()
        location = tempfile.mkdtemp(prefix="azurite-")
        process = subprocess.Popen(
            [
                exe,
                "--silent",
                "--location",
                location,
                "--blobHost",
                "127.0.0.1",
                "--blobPort",
                str(port),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                pytest.fail(f"azurite exited early with {process.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.25)
        else:
            process.terminate()
            pytest.fail("azurite did not start within 30s")

        return cls(process, port, location)

    def stop(self):
        if self._patcher is not None:
            self._patcher.stop()
            self._patcher = None
        self._process.terminate()
        try:
            self._process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self._process.kill()
        shutil.rmtree(self._tmp_location, ignore_errors=True)

    # ---- seeding (account key; not the path under test) ----------------
    def _admin_filesystem(self):
        import pyarrow.fs as pafs

        return pafs.AzureFileSystem(
            account_name=_AZURITE_ACCOUNT,
            account_key=_AZURITE_KEY,
            blob_storage_authority=self.authority,
            dfs_storage_authority=self.authority,
            blob_storage_scheme="http",
            dfs_storage_scheme="http",
        )

    def seed(self):
        from deltalake import write_deltalake

        fs = self._admin_filesystem()
        fs.create_dir(self.container)

        # One `write_deltalake` call per commit, so the table ends up with
        # `_COMMIT_COUNT` log versions rather than one.
        for commit in range(_COMMIT_COUNT):
            write_deltalake(
                f"azure://{self.container}/delta",
                pa.table(_reference_rows(commit * _ROWS_PER_COMMIT, _ROWS_PER_COMMIT)),
                mode="overwrite" if commit == 0 else "append",
                target_file_size=_TARGET_FILE_SIZE,
                storage_options={
                    "account_name": _AZURITE_ACCOUNT,
                    "account_key": _AZURITE_KEY,
                    "azure_storage_endpoint": self.account_endpoint,
                    "allow_http": "true",
                },
            )

        from azure.storage.blob import ContainerSasPermissions, generate_container_sas

        self._sas = generate_container_sas(
            account_name=_AZURITE_ACCOUNT,
            container_name=self.container,
            account_key=_AZURITE_KEY,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    def object_names(self):
        """Blob names under the table prefix, for the table-shape assertion."""
        import pyarrow.fs as pafs

        fs = self._admin_filesystem()
        selector = pafs.FileSelector(f"{self.container}/delta", recursive=True)
        return [
            info.path
            for info in fs.get_file_info(selector)
            if info.type == pafs.FileType.File
        ]

    # ---- the faked Databricks control plane ----------------------------
    @property
    def table_url(self) -> str:
        # The shape real UC vends for an ADLS Gen2 location.
        return (
            f"abfss://{self.container}@{_AZURITE_ACCOUNT}"
            f".dfs.core.windows.net/delta"
        )

    def catalog(self) -> DatabricksUnityCatalog:
        """The real catalog, with only ``tables.get`` / credential vending faked.

        ``_resolve_storage``, ``_creds_to_env``, ``_apply_env`` and
        ``_infer_format`` all run as shipped -- they are what is under test.
        """
        assert self._sas is not None, "seed() must run before catalog()"

        table_info = TableInfo(
            table_id="tid-delta",
            data_source_format=DataSourceFormat("DELTA"),
            storage_location=self.table_url,
        )
        response = GenerateTemporaryTableCredentialResponse(
            url=self.table_url,
            azure_user_delegation_sas=AzureUserDelegationSas(sas_token=self._sas),
        )
        client = mock.MagicMock()
        client.tables.get.return_value = table_info
        client.temporary_table_credentials.generate_temporary_table_credentials.return_value = (  # noqa: E501
            response
        )

        if self._patcher is not None:
            self._patcher.stop()
        self._patcher = mock.patch.object(
            DatabricksUnityCatalog, "_workspace_client", return_value=client
        )
        self._patcher.start()

        return DatabricksUnityCatalog(
            url="https://fake-workspace.azuredatabricks.net", token="dapi-fake"
        )

    def table_identifier(self) -> str:
        return "main.ray_test.delta"

    def reader_storage_options(self):
        """Emulator plumbing -- endpoint and scheme, deliberately no credential.

        A real reader would need none of this; Azurite is not on Azure's DNS and
        speaks http. The SAS is *not* included: it has to arrive through the
        catalog, which is the whole point.
        """
        return {
            "azure_storage_endpoint": self.account_endpoint,
            "allow_http": "true",
        }


class _RealAzureBackend:
    """A real Azure Databricks workspace over real Azure storage."""

    is_emulator = False

    def __init__(self, config):
        self._config = config

    @classmethod
    def start(cls):
        return cls({k: os.environ[v] for k, v in _REAL_ENV.items()})

    def stop(self):
        pass

    def seed(self):
        # Provisioned out of band: creating a UC table needs a SQL warehouse,
        # well outside what a test should stand up. The module docstring carries
        # the DDL.
        pass

    def object_names(self):
        # Listing managed-table storage directly would need a second credential
        # path that has nothing to do with what is under test.
        return None

    def catalog(self) -> DatabricksUnityCatalog:
        return DatabricksUnityCatalog(
            url=self._config["host"], token=self._config["token"]
        )

    def table_identifier(self) -> str:
        return self._config["delta_table"]

    def reader_storage_options(self):
        return {}


def _real_azure_configured() -> bool:
    return all(os.environ.get(name) for name in _REAL_ENV.values())


@pytest.fixture(autouse=True)
def clean_azure_env():
    """Clear Azure env vars for the test, and undo anything the test sets.

    The teardown half is load-bearing, and ``monkeypatch`` cannot provide it:
    ``monkeypatch.delenv`` only restores variables that existed at setup, and
    ``_apply_env`` *creates* ``AZURE_STORAGE_SAS_TOKEN`` mid-test by writing
    straight to ``os.environ``. Leaving it behind is not a cosmetic leak -- the
    module-scoped Ray cluster inherits the process environment when it starts,
    so a leaked SAS reaches every read task for free and
    ``test_vended_credentials_reach_read_tasks`` passes for entirely the wrong
    reason. Autouse so no test can opt out of it by accident.
    """
    saved = {
        name: value
        for name, value in os.environ.items()
        if name.startswith(_AZURE_ENV_PREFIXES)
    }
    for name in saved:
        del os.environ[name]
    try:
        yield
    finally:
        for name in [n for n in os.environ if n.startswith(_AZURE_ENV_PREFIXES)]:
            del os.environ[name]
        os.environ.update(saved)


@pytest.fixture
def suppress_ray_init(monkeypatch):
    """Stop ``resolve()`` from starting a Ray cluster of its own.

    ``_apply_env`` calls ``ray.init(runtime_env=...)`` whenever Ray is not yet
    up, so a resolve-only test would leave a cluster behind and the next test to
    ask for a shared cluster would die on "called ray.init twice". Same trick as
    ``isolated_env`` in ``test_catalog.py``. A no-op once a shared cluster is
    genuinely running, which makes test order irrelevant.
    """
    monkeypatch.setattr("ray.is_initialized", lambda: True)


@pytest.fixture(scope="module")
def azure_backend_module():
    """Start one backend per module and seed it once.

    Seeding writes `_EXPECTED_COUNT` rows over several commits, which is slow
    enough that doing it per test would dominate the suite.
    """
    backend_cls = _RealAzureBackend if _real_azure_configured() else _AzuriteBackend
    backend = backend_cls.start()
    try:
        backend.seed()
        yield backend
    finally:
        backend.stop()


@pytest.fixture
def azure_backend(azure_backend_module):
    """The module-scoped backend, seeded once.

    Real Azure when it is configured, Azurite otherwise; both satisfy the same
    interface -- ``catalog()``, ``table_identifier()``,
    ``reader_storage_options()``, ``object_names()``, ``is_emulator`` -- so every
    test below reads identically against either.
    """
    return azure_backend_module


# ---------------------------------------------------------------------------
# Harness sanity -- these must pass, or the failures below mean nothing
# ---------------------------------------------------------------------------


def test_resolve_vends_sas_for_azure_delta(azure_backend, suppress_ray_init):
    """The backend really does vend an Azure SAS against an ``abfss://`` URL.

    Guards the harness itself: if this breaks, the credential-delivery tests
    below are failing for an uninteresting reason.
    """
    catalog = azure_backend.catalog()

    resolved = catalog.resolve(
        azure_backend.table_identifier(),
        reader=ReaderFormat.DELTA,
        mode=CatalogAccessMode.READ,
    )

    # A non-Azure URL means the workspace is not Azure-backed, and `resolve()`
    # took the AWS or GCP branch instead. Everything below would then be
    # exercising a different cloud's code path, so say so rather than letting
    # the later failures look like Azure bugs.
    assert resolved.path.startswith("abfss://"), (
        f"expected an Azure (abfss://) table, got {resolved.path!r}. A "
        "Databricks account on AWS or GCP vends those clouds' credentials and "
        "never reaches the Azure code path under test -- point "
        f"{_REAL_ENV['host']} at an Azure Databricks workspace."
    )
    assert resolved.data_format is ReaderFormat.DELTA
    sas = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    assert sas, (
        "resolve() vended no SAS into the environment. On a real workspace this "
        "usually means credential vending is off: the metastore needs external "
        "data access enabled, and the principal needs EXTERNAL USE SCHEMA on "
        "the schema on top of SELECT."
    )
    assert not sas.startswith("?"), "leading '?' must be stripped"


def test_table_shape_is_non_trivial(azure_backend):
    """The table really has several log versions and several data files.

    Without this the suite could quietly degrade to a one-file one-version
    table, where a single credential reaching a single process would be enough
    to pass -- and the propagation bug these tests exist for would go unnoticed.
    """
    names = azure_backend.object_names()
    if names is None:
        pytest.skip("backend does not expose its object listing")

    log_files = [n for n in names if "_delta_log/" in n and n.endswith(".json")]
    data_files = [n for n in names if n.endswith(".parquet")]

    assert len(log_files) >= _COMMIT_COUNT, (
        f"expected >= {_COMMIT_COUNT} _delta_log commits, got "
        f"{len(log_files)}: {sorted(log_files)}"
    )
    assert len(data_files) > 1, (
        f"expected the table to span multiple Parquet files, got "
        f"{len(data_files)}: {sorted(data_files)}"
    )


# ---------------------------------------------------------------------------
# The system test: do the rows come back, and come back right?
# ---------------------------------------------------------------------------


def test_delta_count_min_max_via_catalog(ray_start_10_cpus_shared, azure_backend):
    """A catalog-resolved Azure Delta table reads back correctly.

    Authenticating is only half of it -- the aggregations are what catch a read
    that succeeds but returns the wrong rows (a partial log replay, a stale
    version, one block silently dropped). The table spans several Parquet files,
    so the scan fans out across read tasks and the credential has to survive the
    trip into the workers, not just work on the driver.
    """
    catalog = azure_backend.catalog()

    ds = ray.data.read_delta(
        azure_backend.table_identifier(),
        catalog=catalog,
        storage_options=azure_backend.reader_storage_options() or None,
    )

    assert ds.count() == _EXPECTED_COUNT
    assert ds.min("id") == _EXPECTED_MIN
    assert ds.max("id") == _EXPECTED_MAX
    # Every id exactly once: catches duplicated or dropped commits, which an
    # aggregate alone can miss.
    ids = sorted(row["id"] for row in ds.take_all())
    assert ids == list(range(_EXPECTED_COUNT))


def test_vended_credentials_reach_read_tasks(ray_start_10_cpus_shared, azure_backend):
    """The vended credential must reach read tasks, not just the driver.

    Deliberately channel-agnostic: it asserts the credential is *reachable* from
    a worker, not how. Ray gives `resolve()` three ways to manage that, and only
    two of them work on a running cluster:

    - inside the returned `filesystem`, or inside `storage_options` -- both are
      pickled along with the read plan, so they arrive intact;
    - via the environment, which does NOT arrive: a worker is a separate process
      and `_apply_env` can only amend the cluster `runtime_env` while Ray is
      still uninitialized. Every case with an existing Dataset is past that.

    AWS passes by the first route (its vended `S3FileSystem` carries the keys).
    The shared-cluster fixture puts us on an already-running cluster on purpose
    -- that is the case that breaks.
    """
    assert ray.is_initialized(), "fixture should have started Ray already"

    @ray.remote
    def sas_in_worker():
        return os.environ.get("AZURE_STORAGE_SAS_TOKEN")

    # Self-check on the harness: the cluster must not already be carrying a SAS
    # when we start. It inherits the process environment at startup, so an
    # earlier test leaking one would hand every read task a credential for free
    # and make this test pass without the code doing anything. `clean_azure_env`
    # exists to prevent that; this is the assertion that it worked.
    assert ray.get(sas_in_worker.remote()) is None, (
        "a Ray worker already sees AZURE_STORAGE_SAS_TOKEN before resolve() "
        "was called, so the cluster started with a leaked credential in its "
        "environment. This test cannot prove anything in that state -- fix the "
        "leak (see clean_azure_env) rather than trusting a pass."
    )

    catalog = azure_backend.catalog()
    resolved = catalog.resolve(
        azure_backend.table_identifier(),
        reader=ReaderFormat.DELTA,
        mode=CatalogAccessMode.READ,
    )
    driver_sas = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    assert driver_sas, "precondition: resolve() should have vended a SAS"

    travels_in_options = any(
        driver_sas in str(value) for value in (resolved.storage_options or {}).values()
    )
    # A filesystem holding the credential pickles too, but it exposes no way to
    # read the SAS back out, so its mere presence is the strongest check
    # available here.
    travels_in_filesystem = resolved.filesystem is not None
    reaches_via_env = ray.get(sas_in_worker.remote()) == driver_sas

    assert travels_in_options or travels_in_filesystem or reaches_via_env, (
        "read tasks cannot reach the catalog-vended SAS by any channel. "
        "resolve() returned no filesystem and no storage_options carrying it, "
        "and _apply_env only seeds runtime_env while Ray is uninitialized -- so "
        "on a running cluster the credential stops at the driver and every read "
        "task authenticates with nothing."
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
