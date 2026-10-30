"""Prepare disposable credentials; this is test infrastructure, not reader code."""

import ctypes
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

REALM = "RAY.TEST"
HOST = "hive.auth.test"
PROFILE = os.environ.get("HIVE_AUTH_PROFILE", "")


def run(args, stage, *, env=None):
    # Do not include argv or captured stderr in errors: either may contain secrets.
    try:
        result = subprocess.run(
            args, env=env, capture_output=True, text=True, timeout=30
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Fixture stage timed out: {stage}") from None
    if result.returncode:
        raise RuntimeError(f"Fixture stage failed: {stage}")
    return result.stdout


def write(path, content, *, private=True):
    path = Path(path)
    path.write_text(content)
    path.chmod(0o600 if private else 0o644)


def xml(path, properties):
    root = Element("configuration")
    for key, value in properties.items():
        prop = SubElement(root, "property")
        SubElement(prop, "name").text = key
        SubElement(prop, "value").text = str(value)
    write(path, tostring(root, encoding="unicode"))


def certificates():
    for name in ("trusted", "unrelated"):
        prefix = f"/kdc/{name}"
        run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                f"/CN={name}-test-ca",
                "-keyout",
                prefix + ".key",
                "-out",
                prefix + ".pem",
            ],
            "create ephemeral CA",
        )
    run(
        [
            "openssl",
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            f"/CN={HOST}",
            "-keyout",
            "/server-security/server.key",
            "-out",
            "/kdc/server.csr",
        ],
        "create server key",
    )
    write(
        "/kdc/server.ext", f"subjectAltName=DNS:{HOST}\nextendedKeyUsage=serverAuth\n"
    )
    run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            "/kdc/server.csr",
            "-CA",
            "/kdc/trusted.pem",
            "-CAkey",
            "/kdc/trusted.key",
            "-CAcreateserial",
            "-days",
            "2",
            "-extfile",
            "/kdc/server.ext",
            "-out",
            "/kdc/server.pem",
        ],
        "sign server certificate",
    )
    password = secrets.token_urlsafe(32)
    write("/server-security/store-password", password)
    run(
        [
            "openssl",
            "pkcs12",
            "-export",
            "-inkey",
            "/server-security/server.key",
            "-in",
            "/kdc/server.pem",
            "-certfile",
            "/kdc/trusted.pem",
            "-out",
            "/server-security/server.p12",
            "-passout",
            "file:/server-security/store-password",
        ],
        "create PKCS12 keystore",
    )
    shutil.copyfile("/kdc/trusted.pem", "/ca/ca.pem")
    shutil.copyfile("/kdc/unrelated.pem", "/ca/wrong-ca.pem")
    return password


def initialize_kdc():
    if PROFILE not in {"G", "T", "GT", "N"}:
        raise ValueError("Choose G, T, GT, or N")
    os.umask(0o077)
    for path in (
        "/krb",
        "/kdc",
        "/server-security",
        "/driver-security",
        "/worker-security",
        "/ca",
        "/events",
    ):
        Path(path).mkdir(exist_ok=True)
    write(
        "/krb/krb5.conf",
        f"""[libdefaults]
 default_realm = {REALM}
 dns_lookup_kdc = false
 dns_lookup_realm = false
 dns_canonicalize_hostname = false
 rdns = false
 clockskew = 1
 forwardable = false
[realms]
 {REALM} = {{
  kdc = kdc
 }}
[domain_realm]
 .auth.test = {REALM}
 auth.test = {REALM}
""",
        private=False,
    )
    os.environ["KRB5_KDC_PROFILE"] = "/kdc/kdc.conf"
    write(
        "/kdc/kdc.conf",
        f"""[kdcdefaults]
 kdc_ports = 88
 kdc_tcp_ports = 88
[realms]
 {REALM} = {{
  database_name = /kdc/principal
  key_stash_file = /kdc/stash
  max_life = 1h
  max_renewable_life = 0
 }}
""",
    )
    run(["kdb5_util", "create", "-s", "-P", secrets.token_urlsafe(32)], "create realm")
    for principal, directory in (
        (f"hive/{HOST}", "/server-security"),
        ("driver", "/driver-security"),
        ("worker", "/worker-security"),
    ):
        run(
            ["kadmin.local", "-q", f"addprinc -randkey {principal}"], "create principal"
        )
        run(
            ["kadmin.local", "-q", f"ktadd -k {directory}/identity.keytab {principal}"],
            "create role keytab",
        )
    password = certificates()
    # LOAD DATA LOCAL supplies the secure profiles without provisioning a
    # secure MR/YARN write environment unrelated to HS2 authentication reads.
    write(
        "/server-security/test-data.tsv",
        "".join(
            str(i) + "\t" + (r"\N" if i % 7 == 0 else f"row-{i}") + "\n"
            for i in range(2049)
        ),
    )
    secure = PROFILE in {"G", "GT"}
    core = {
        "fs.defaultFS": "file:///",
        "mapreduce.framework.name": "local",
        "hadoop.security.authentication": "kerberos" if secure else "simple",
    }
    if secure:
        # TokenCache checks a renewer even when local files need no tokens.
        core["yarn.resourcemanager.principal"] = f"hive/{HOST}@{REALM}"
    hive = {
        "javax.jdo.option.ConnectionURL": "jdbc:derby:;databaseName=/tmp/metastore_db;create=true",
        "hive.metastore.warehouse.dir": "file:///tmp/warehouse",
        "hive.exec.scratchdir": "/tmp/hive",
        "hive.execution.engine": "mr",
        "mapreduce.framework.name": "local",
        "hive.server2.transport.mode": "binary",
        "hive.server2.thrift.bind.host": "0.0.0.0",
        "hive.server2.thrift.port": "10000",
        "hive.server2.authentication": (
            "KERBEROS" if secure else ("NOSASL" if PROFILE == "N" else "NONE")
        ),
        "hive.server2.enable.doAs": "false",
        "hive.server2.thrift.sasl.qop": "auth",
        "hive.server2.authentication.kerberos.principal": f"hive/{HOST}@{REALM}",
        "hive.server2.authentication.kerberos.keytab": "/security/identity.keytab",
        "hive.server2.use.SSL": str(PROFILE in {"T", "GT"}).lower(),
        "hive.server2.keystore.path": "/security/server.p12",
        "hive.server2.keystore.password": password,
        "hive.server2.keystore.type": "PKCS12",
        "hive.metastore.event.db.notification.api.auth": "false",
    }
    xml("/server-security/core-site.xml", core)
    xml("/server-security/hive-site.xml", hive)
    for directory in ("/server-security", "/driver-security", "/worker-security"):
        os.chown(directory, 1000, 1000)
        os.chmod(directory, 0o700)
        for path in Path(directory).iterdir():
            os.chown(path, 1000, 1000)
            path.chmod(0o600)
    for path in Path("/ca").iterdir():
        path.chmod(0o644)
    os.chmod("/ca", 0o755)
    os.chmod("/krb", 0o755)
    os.chown("/events", 1000, 1000)
    os.chmod("/events", 0o700)
    write("/krb/ready", PROFILE, private=False)
    os.execvp("krb5kdc", ["krb5kdc", "-n"])


def effective_clockskew():
    library = ctypes.CDLL("libkrb5.so.3")
    pointer = ctypes.c_void_p
    library.krb5_init_context.argtypes = [ctypes.POINTER(pointer)]
    library.krb5_get_profile.argtypes = [pointer, ctypes.POINTER(pointer)]
    library.profile_get_integer.argtypes = [
        pointer,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_int),
    ]
    library.profile_release.argtypes = [pointer]
    library.profile_release.restype = None
    library.krb5_free_context.argtypes = [pointer]
    library.krb5_free_context.restype = None
    context, profile = pointer(), pointer()
    if library.krb5_init_context(ctypes.byref(context)):
        raise RuntimeError("Cannot initialize native Kerberos configuration")
    try:
        if library.krb5_get_profile(context, ctypes.byref(profile)):
            raise RuntimeError("Cannot inspect native Kerberos configuration")
        try:
            seconds = ctypes.c_int()
            code = library.profile_get_integer(
                profile, b"libdefaults", b"clockskew", None, 300, ctypes.byref(seconds)
            )
            if code or seconds.value != 1:
                raise RuntimeError("Unexpected native Kerberos clockskew")
            return seconds.value
        finally:
            library.profile_release(profile)
    finally:
        library.krb5_free_context(context)


def credentials(role):
    if PROFILE not in {"G", "GT"}:
        return
    clockskew = effective_clockskew()
    deadline = time.monotonic() + 30
    while True:
        try:
            run(
                [
                    "kinit",
                    "-k",
                    "-t",
                    "/auth/identity.keytab",
                    "-l",
                    "1h",
                    f"{role}@{REALM}",
                ],
                "obtain role credentials",
            )
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    env = dict(os.environ, KRB5CCNAME="FILE:/auth/expired.ccache")
    run(
        ["kinit", "-k", "-t", "/auth/identity.keytab", "-l", "8s", f"{role}@{REALM}"],
        "obtain short-lived credentials",
        env=env,
    )
    run(["klist", "-s"], "verify initially valid short-lived cache", env=env)
    listing = run(["klist"], "read ticket end times", env=env)
    matches = re.findall(r"\d\d/\d\d/\d\d \d\d:\d\d:\d\d", listing)
    ends = [
        datetime.strptime(v, "%m/%d/%y %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        for v in matches[1::2]
    ]
    if not ends:
        raise RuntimeError("Cannot read actual ticket expiration")
    write(
        "/auth/expiry.json",
        json.dumps(
            {
                "initially_valid": True,
                "end": max(ends),
                "issued": time.time(),
                "clockskew": clockskew,
                "margin": 2,
            }
        ),
    )
    Path("/auth/invalid.ccache").write_bytes(b"invalid-test-cache")


def client(role):
    credentials(role)
    run(
        [
            sys.executable,
            "/ray/python/ray/setup-dev.py",
            "-y",
            "--allow",
            "data",
            "tests",
        ],
        "link checked-out data and tests",
    )
    # Named volumes start as root-owned; only the non-sensitive event volume is shared.
    if not os.access("/events", os.W_OK):
        raise RuntimeError("Event volume must be writable by the test runtime")
    args = [
        sys.executable,
        "-m",
        "ray.scripts.scripts",
        "start",
        "--block",
        "--disable-usage-stats",
        "--object-store-memory=134217728",
    ]
    if role == "driver":
        args += ["--head", "--port=6379", "--num-cpus=0", "--include-dashboard=false"]
    else:
        args += [
            "--address=head:6379",
            "--num-cpus=2",
            '--resources={"hive_auth_worker":1}',
        ]
    os.execvp(args[0], args)


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "kdc":
        initialize_kdc()
    elif action == "ready":
        if not Path("/krb/ready").is_file():
            raise SystemExit(1)
        with socket.create_connection(("127.0.0.1", 88), timeout=2):
            pass
    elif action in {"head", "worker"}:
        client("driver" if action == "head" else "worker")
    else:
        raise SystemExit("Unknown fixture action")
