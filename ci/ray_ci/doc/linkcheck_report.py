#!/usr/bin/env python3
"""Post-process Sphinx linkcheck output into a confirmed-broken link report.

The ``doc: linkcheck`` step runs ``make -C doc linkcheck_all``, which writes a
machine-readable ``output.json`` (one JSON record per line). That step is
``soft_fail``, so a broken link never fails the build. This script filters the
reported-broken external links down to the ones that are genuinely dead, prints
them, and writes them to a ``linkcheck-report.json`` build artifact.

The step holds no Slack credential. A scheduled job maintained by the docs
team reads the artifact through the Buildkite API and posts a weekday digest
to the docs Slack channel. That job falls back to parsing the printed
``broken<TAB>code<TAB>uri<TAB>file`` lines and the ``confirmed=N
inconclusive=M`` summary when the artifact is missing, so keep their format
stable.

The filter mirrors the Anyscale docs external-link scan. A single concurrent
crawl draws transient rejections from hosts that rate-limit or bot-filter by
source IP, so each reported-broken link is re-checked once, serially, after a
cooldown:

* 2xx, or 403 (bot filtering, not a dead link): recovered, dropped.
* 429 (rate limited): inconclusive, reported but not counted as broken.
* anything else, including a 3xx from a failed redirect chain: confirmed
  broken.

The script never fails the build; the report is the signal.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

# 403 is Cloudflare-style bot filtering, not a dead link.
ACCEPT_CODES = {403}
# Cooldowns are env-tunable so the re-check pass can be exercised quickly in
# tests without waiting out the production cooldown.
COOLDOWN = int(os.environ.get("LINKCHECK_RECHECK_COOLDOWN", "120"))
BACKOFF = int(os.environ.get("LINKCHECK_RECHECK_BACKOFF", "30"))
# The Buildkite agent uploads everything in the host's /tmp/artifacts, which the
# job container mounts here.
ARTIFACT_DIR = "/artifact-mount"
REPORT_NAME = "linkcheck-report.json"


def recheck(url: str) -> int:
    """Return the HTTP status for ``url``, following redirects.

    Args:
        url: The link to re-check.

    Returns:
        The final HTTP status code, or 0 if the request could not complete
        (DNS failure, timeout, connection error).
    """
    req = urllib.request.Request(url, headers={"User-Agent": "ray-linkcheck/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code
    except Exception:
        return 0


def load_broken(path: str) -> list:
    """Return the reported-broken external links from a linkcheck output file.

    Sphinx 8.2 reports an unreachable host that hangs as ``timeout`` rather than
    ``broken`` (``linkcheck_report_timeouts_as_broken`` defaults to False), so
    both statuses are collected and left to the re-check to confirm.

    Args:
        path: Path to the Sphinx linkcheck ``output.json``.

    Returns:
        The records whose status is ``broken`` or ``timeout`` and whose URI is
        external.
    """
    broken = []
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                uri = record.get("uri", "")
                if record.get("status") in ("broken", "timeout") and uri.startswith(
                    "http"
                ):
                    broken.append(record)
    except FileNotFoundError:
        print(f"::warning:: {path} not found; skipping report.")
    return broken


def confirm(broken: list) -> tuple:
    """Re-check each broken link and split it into confirmed and inconclusive.

    Args:
        broken: Records from :func:`load_broken`.

    Returns:
        A ``(confirmed, inconclusive)`` pair of record lists.
    """
    confirmed, inconclusive = [], []
    cache = {}
    for record in broken:
        uri = record["uri"]
        if uri in cache:
            code = cache[uri]
        else:
            code = recheck(uri)
            if code == 429:
                time.sleep(BACKOFF)
                code = recheck(uri)
            cache[uri] = code
            time.sleep(1)
        # urlopen follows redirects to a 2xx, so a 3xx that reaches here is a
        # failed chain (redirect loop or too many hops) and stays broken.
        if 200 <= code < 300 or code in ACCEPT_CODES:
            continue
        if code == 429:
            inconclusive.append(record)
        else:
            record["recheck_code"] = code
            confirmed.append(record)
    return confirmed, inconclusive


def write_report(confirmed: list, inconclusive: list) -> None:
    """Write the confirmed and inconclusive links to the build artifact.

    Does nothing outside CI, where the artifact directory doesn't exist.

    Args:
        confirmed: Links that failed the re-check.
        inconclusive: Links that stayed rate-limited (429) on re-check.
    """
    if not os.path.isdir(ARTIFACT_DIR):
        return
    report = {
        "confirmed": [
            {
                "code": record.get("recheck_code", "ERR"),
                "uri": record["uri"],
                "filename": record.get("filename"),
                "lineno": record.get("lineno"),
            }
            for record in confirmed
        ],
        "inconclusive_count": len(inconclusive),
    }
    path = os.path.join(ARTIFACT_DIR, REPORT_NAME)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    print(f"Wrote {path}.")


def main(path: str) -> int:
    """Report confirmed-broken external links from a linkcheck output file.

    Args:
        path: Path to the Sphinx linkcheck ``output.json``.

    Returns:
        Always 0. The report is the signal; this never fails the build.
    """
    broken = load_broken(path)
    if not broken:
        print("linkcheck: no broken external links reported.")
        write_report([], [])
        return 0

    print(f"Re-checking {len(broken)} reported-broken link(s) after {COOLDOWN}s.")
    time.sleep(COOLDOWN)
    confirmed, inconclusive = confirm(broken)
    print(f"confirmed={len(confirmed)} inconclusive={len(inconclusive)}")

    for record in confirmed:
        code = record.get("recheck_code", "ERR")
        print(f"broken\t{code}\t{record['uri']}\t{record.get('filename')}")

    write_report(confirmed, inconclusive)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: linkcheck_report.py <output.json>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
