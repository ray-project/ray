import datetime

from typing import Dict, Optional


def handle_result(
    created_on: datetime.datetime,
    category: str,
    test_suite: str,
    test_name: str,
    status: str,
    results: Dict,
    artifacts: Dict,
    last_logs: str,
    team: str,
    commit_url: str,
    session_url: str,
    runtime: float,
    stable: bool,
    return_code: int,
) -> Optional[str]:

    if not status == "finished":
        return f"Test script did not finish successfully ({status})."

    return None
