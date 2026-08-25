#!/usr/bin/env python3
"""Post-process Sphinx linkcheck output and alert on confirmed-broken links.

The nightly ``doc: linkcheck`` step runs ``make -C doc linkcheck_all``, which
writes a machine-readable ``output.json`` (one JSON record per line). That step
is ``soft_fail``, so a broken link never fails the build and, today, reaches no
one. This script turns that output into an actionable signal: it filters the
reported-broken external links down to the ones that are genuinely dead, then
posts them to Slack.

The filter mirrors the Anyscale docs external-link scan. A single concurrent
crawl draws transient rejections from hosts that rate-limit or bot-filter by
source IP, so each reported-broken link is re-checked once, serially, after a
cooldown:

* 2xx/3xx, or 403 (bot filtering, not a dead link): recovered, dropped.
* 429 (rate limited): inconclusive, reported but not counted as broken.
* anything else: confirmed broken.

The script never fails the build; the Slack alert is the signal.
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
WEBHOOK_ENV = "DOCS_LINKCHECK_SLACK_WEBHOOK"
MAX_SLACK_ROWS = 15


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

    Args:
        path: Path to the Sphinx linkcheck ``output.json``.

    Returns:
        The records whose status is ``broken`` and whose URI is external.
    """
    broken = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            uri = record.get("uri", "")
            if record.get("status") == "broken" and uri.startswith("http"):
                broken.append(record)
    return broken


def confirm(broken: list) -> tuple:
    """Re-check each broken link and split it into confirmed and inconclusive.

    Args:
        broken: Records from :func:`load_broken`.

    Returns:
        A ``(confirmed, inconclusive)`` pair of record lists.
    """
    confirmed, inconclusive = [], []
    for record in broken:
        code = recheck(record["uri"])
        if code == 429:
            time.sleep(BACKOFF)
            code = recheck(record["uri"])
        if 200 <= code < 400 or code in ACCEPT_CODES:
            continue
        if code == 429:
            inconclusive.append(record)
        else:
            record["recheck_code"] = code
            confirmed.append(record)
        time.sleep(1)
    return confirmed, inconclusive


def format_message(confirmed: list, inconclusive: list) -> str:
    """Return the Slack message body for a set of confirmed-broken links.

    Args:
        confirmed: Links that failed the re-check.
        inconclusive: Links that stayed rate-limited (429) on re-check.

    Returns:
        The Slack message text.
    """
    rows = []
    for record in confirmed[:MAX_SLACK_ROWS]:
        code = record.get("recheck_code", record.get("code", "ERR"))
        source = f"{record.get('filename', '?')}:{record.get('lineno', 0)}"
        rows.append(f"• `{code}` {record['uri']}\n    ↳ {source}")
    text = (
        f":rotating_light: *Ray docs: {len(confirmed)} broken external link(s)*\n"
        + "\n".join(rows)
    )
    if len(confirmed) > MAX_SLACK_ROWS:
        text += f"\n_…and {len(confirmed) - MAX_SLACK_ROWS} more. See the build log._"
    if inconclusive:
        text += (
            f"\n\n_{len(inconclusive)} link(s) stayed rate-limited (429) on "
            "re-check and couldn't be verified; not counted as broken._"
        )
    return text


def post_to_slack(text: str) -> None:
    """Post ``text`` to the Slack webhook, if one is configured.

    Args:
        text: The message body to send.
    """
    webhook = os.environ.get(WEBHOOK_ENV)
    if not webhook:
        print(f"::warning:: {WEBHOOK_ENV} is not set; skipping Slack alert.")
        return
    payload = json.dumps({"text": text}).encode()
    request = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as resp:
        if resp.status != 200:
            print(f"::warning:: Slack webhook returned HTTP {resp.status}.")


def main(path: str) -> int:
    """Report confirmed-broken external links from a linkcheck output file.

    Args:
        path: Path to the Sphinx linkcheck ``output.json``.

    Returns:
        Always 0. The Slack alert is the signal; this never fails the build.
    """
    broken = load_broken(path)
    if not broken:
        print("linkcheck: no broken external links reported.")
        return 0

    print(f"Re-checking {len(broken)} reported-broken link(s) after {COOLDOWN}s.")
    time.sleep(COOLDOWN)
    confirmed, inconclusive = confirm(broken)
    print(f"confirmed={len(confirmed)} inconclusive={len(inconclusive)}")

    for record in confirmed:
        code = record.get("recheck_code", "ERR")
        print(f"broken\t{code}\t{record['uri']}\t{record.get('filename')}")

    if confirmed:
        post_to_slack(format_message(confirmed, inconclusive))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: linkcheck_report.py <output.json>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
