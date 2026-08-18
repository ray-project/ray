"""Import every pinned package version in a lock file into CodeArtifact.

CodeArtifact imports a package version from pypi.org the first time something
asks for one of its *assets*. Until that import finishes it will list the file
in the simple index and answer the download with a 404 -- and 404 is not in
urllib3's status_forcelist, so pip does not retry it and the build fails. The
window is short (measured at 1.3-1.6s) but it is real, so the repository has to
be warm before CI depends on it.

Requesting the /simple/<pkg>/ listing does not import anything. One asset GET
imports every asset of that version, so fetching the single smallest asset per
pinned version is the cheapest complete warm-up.

Usage:
    python ci/codeartifact_prewarm.py release/requirements_py310.txt [more.lock ...]

Reads the index from $PIP_INDEX_URL and credentials from the netrc at $NETRC,
both of which ci/codeartifact_env.sh exports.
"""

import argparse
import concurrent.futures
import json
import netrc
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from typing import Dict, List, Optional, Set, Tuple

_PIN = re.compile(r"^([A-Za-z0-9._-]+)==([^ \\\n]+)")
_SUFFIXES = (".whl", ".tar.gz", ".zip", ".tar.bz2", ".egg")
_JSON_ACCEPT = "application/vnd.pypi.simple.v1+json"


def _normalize(name: str) -> str:
    """PEP 503 normalisation: '-', '_' and '.' are interchangeable."""
    return re.sub(r"[-_.]+", "-", name.lower())


def _belongs_to(filename: str, name: str, version: str) -> bool:
    """Is this distribution file the given name==version?

    The version field cannot be found by splitting on '-': an sdist of a
    hyphenated project is python-dateutil-2.9.0.tar.gz, so the name contributes
    hyphens too. Strip the extension and match the normalised name-version
    prefix instead.
    """
    for suffix in _SUFFIXES:
        if filename.endswith(suffix):
            filename = filename[: -len(suffix)]
            break
    stem = _normalize(filename)
    prefix = _normalize(f"{name}-{version}")
    return stem == prefix or stem.startswith(prefix + "-")


def parse_locks(paths: List[str]) -> Dict[str, str]:
    """Collect {package: version} from pip/uv lock files."""
    pins: Dict[str, str] = {}
    for path in paths:
        with open(path) as handle:
            for line in handle:
                match = _PIN.match(line)
                if match:
                    pins[_normalize(match.group(1))] = match.group(2)
    return pins


class Index:
    def __init__(self, index_url: str, netrc_path: Optional[str]) -> None:
        self.index_url = index_url if index_url.endswith("/") else index_url + "/"
        host = urllib.parse.urlparse(self.index_url).hostname or ""
        self.headers = {}
        auth = netrc.netrc(netrc_path).authenticators(host) if netrc_path else None
        if auth:
            login, _, password = auth
            raw = f"{login}:{password}".encode()
            self.headers["Authorization"] = "Basic " + b64encode(raw).decode()

    def _get(self, url: str, accept: Optional[str] = None) -> Tuple[int, bytes]:
        headers = dict(self.headers)
        if accept:
            headers["Accept"] = accept
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, b""

    def warm(
        self, name: str, version: str, deadline: float
    ) -> Tuple[str, str, Optional[str], int]:
        """Import name==version, retrying past the cold-import 404."""
        project = self.index_url + name + "/"
        status, body = self._get(project, _JSON_ACCEPT)
        if status != 200:
            return name, version, f"index HTTP {status}", 0

        files = [
            entry
            for entry in json.loads(body).get("files", [])
            if _belongs_to(entry["filename"], name, version)
        ]
        if not files:
            return name, version, "not on the index", 0

        smallest = min(files, key=lambda entry: entry.get("size") or sys.maxsize)
        asset = urllib.parse.urljoin(project, smallest["url"])

        retries = 0
        started = time.monotonic()
        while True:
            status, _ = self._get(asset)
            if status == 200:
                return name, version, None, retries
            if status != 404 or time.monotonic() - started > deadline:
                return name, version, f"asset HTTP {status}", retries
            retries += 1
            time.sleep(1.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("locks", nargs="+", help="lock files to warm")
    parser.add_argument("--index-url", default=os.environ.get("PIP_INDEX_URL", ""))
    parser.add_argument("--netrc", default=os.environ.get("NETRC"))
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="seconds to keep retrying one asset's cold-import 404",
    )
    args = parser.parse_args()

    if not args.index_url or "codeartifact" not in args.index_url:
        print(
            "codeartifact-prewarm: PIP_INDEX_URL is not a CodeArtifact index, "
            "nothing to warm",
            file=sys.stderr,
        )
        return 0

    pins = parse_locks(args.locks)
    index = Index(args.index_url, args.netrc)
    print(f"warming {len(pins)} package versions from {len(args.locks)} lock file(s)")

    started = time.monotonic()
    failures: List[Tuple[str, str, str]] = []
    raced: Set[str] = set()
    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        futures = [
            pool.submit(index.warm, name, version, args.timeout)
            for name, version in sorted(pins.items())
        ]
        for future in concurrent.futures.as_completed(futures):
            name, version, error, retries = future.result()
            if error:
                failures.append((name, version, error))
            elif retries:
                raced.add(f"{name}=={version}")

    elapsed = time.monotonic() - started
    print(
        f"warmed {len(pins) - len(failures)}/{len(pins)} in {elapsed:.0f}s; "
        f"{len(raced)} hit the cold-import 404 and cleared on retry"
    )
    for name, version, error in failures:
        print(f"  FAILED {name}=={version}: {error}", file=sys.stderr)

    # Warming is best effort: a package that cannot be imported here is one that
    # a build would have had to fetch from PyPI anyway.
    return 0


if __name__ == "__main__":
    sys.exit(main())
