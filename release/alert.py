import argparse
from collections import defaultdict, Counter
from typing import Any, List, Tuple, Mapping, Optional
import datetime
import hashlib
import json
import logging
import os
import requests
import sys

import boto3

from e2e import GLOBAL_CONFIG

from alerts.default import handle_result as default_handle_result
from alerts.rllib_tests import handle_result as rllib_tests_handle_result
from alerts.long_running_tests import handle_result as long_running_tests_handle_result
from alerts.tune_tests import handle_result as tune_tests_handle_result
from alerts.xgboost_tests import handle_result as xgboost_tests_handle_result

SUITE_TO_FN = {
    "long_running_tests": long_running_tests_handle_result,
    "rllib_tests": rllib_tests_handle_result,
    "tune_tests": tune_tests_handle_result,
    "xgboost_tests": xgboost_tests_handle_result,
}

GLOBAL_CONFIG["RELEASE_AWS_DB_STATE_TABLE"] = "alert_state"
GLOBAL_CONFIG["SLACK_WEBHOOK"] = os.environ.get("SLACK_WEBHOOK", "")
GLOBAL_CONFIG["SLACK_CHANNEL"] = os.environ.get("SLACK_CHANNEL", "#oss-test-cop")

RESULTS_LIMIT = 120

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(
    fmt="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def maybe_fetch_slack_webhook():
    if GLOBAL_CONFIG["SLACK_WEBHOOK"] in [None, ""]:
        print("Missing SLACK_WEBHOOK, retrieving from AWS secrets store")
        GLOBAL_CONFIG["SLACK_WEBHOOK"] = boto3.client(
            "secretsmanager", region_name="us-west-2"
        ).get_secret_value(
            SecretId="arn:aws:secretsmanager:us-west-2:029272617770:secret:"
            "release-automation/"
            "slack-webhook-Na0CFP"
        )[
            "SecretString"
        ]


def _obj_hash(obj: Any) -> str:
    json_str = json.dumps(obj, sort_keys=True, ensure_ascii=True)
    sha = hashlib.sha256()
    sha.update(json_str.encode())
    return sha.hexdigest()


def fetch_latest_alerts(rds_data_client):
    schema = GLOBAL_CONFIG["RELEASE_AWS_DB_STATE_TABLE"]

    sql = f"""
        SELECT DISTINCT ON (category, test_suite, test_name)
               category, test_suite, test_name, last_result_hash,
               last_notification_dt
        FROM   {schema}
        ORDER BY category, test_suite, test_name, last_notification_dt DESC
        LIMIT {RESULTS_LIMIT}
        """

    result = rds_data_client.execute_statement(
        database=GLOBAL_CONFIG["RELEASE_AWS_DB_NAME"],
        secretArn=GLOBAL_CONFIG["RELEASE_AWS_DB_SECRET_ARN"],
        resourceArn=GLOBAL_CONFIG["RELEASE_AWS_DB_RESOURCE_ARN"],
        schema=schema,
        sql=sql,
    )
    for row in result["records"]:
        category, test_suite, test_name, last_result_hash, last_notification_dt = (
            r["stringValue"] if "stringValue" in r else None for r in row
        )
        last_notification_dt = datetime.datetime.strptime(
            last_notification_dt, "%Y-%m-%d %H:%M:%S"
        )
        yield category, test_suite, test_name, last_result_hash, last_notification_dt


def fetch_latest_results(
    rds_data_client, fetch_since: Optional[datetime.datetime] = None
):
    schema = GLOBAL_CONFIG["RELEASE_AWS_DB_TABLE"]

    sql = f"""
        SELECT DISTINCT ON (category, test_suite, test_name)
               created_on, category, test_suite, test_name, status, results,
               artifacts, last_logs
        FROM   {schema} """

    parameters = []
    if fetch_since is not None:
        sql += "WHERE created_on >= :created_on "
        parameters = [
            {
                "name": "created_on",
                "typeHint": "TIMESTAMP",
                "value": {"stringValue": fetch_since.strftime("%Y-%m-%d %H:%M:%S")},
            },
        ]

    sql += "ORDER BY category, test_suite, test_name, created_on DESC "
    sql += f"LIMIT {RESULTS_LIMIT}"

    result = rds_data_client.execute_statement(
        database=GLOBAL_CONFIG["RELEASE_AWS_DB_NAME"],
        secretArn=GLOBAL_CONFIG["RELEASE_AWS_DB_SECRET_ARN"],
        resourceArn=GLOBAL_CONFIG["RELEASE_AWS_DB_RESOURCE_ARN"],
        schema=schema,
        sql=sql,
        parameters=parameters,
    )
    for row in result["records"]:
        (
            created_on,
            category,
            test_suite,
            test_name,
            status,
            results,
            artifacts,
            last_logs,
        ) = (r["stringValue"] if "stringValue" in r else None for r in row)

        # Calculate hash before converting strings to objects
        result_obj = (
            created_on,
            category,
            test_suite,
            test_name,
            status,
            results,
            artifacts,
            last_logs,
        )
        result_json = json.dumps(result_obj)
        result_hash = _obj_hash(result_json)

        # Convert some strings to python objects
        created_on = datetime.datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S")
        results = json.loads(results)
        artifacts = json.loads(artifacts)

        yield result_hash, created_on, category, test_suite, test_name, status, results, artifacts, last_logs  # noqa: E501


def mark_as_handled(
    rds_data_client,
    update: bool,
    category: str,
    test_suite: str,
    test_name: str,
    result_hash: str,
    last_notification_dt: datetime.datetime,
):
    schema = GLOBAL_CONFIG["RELEASE_AWS_DB_STATE_TABLE"]

    if not update:
        sql = f"""
            INSERT INTO {schema}
            (category, test_suite, test_name,
            last_result_hash, last_notification_dt)
            VALUES (:category, :test_suite, :test_name,
                    :last_result_hash, :last_notification_dt)
            """
    else:
        sql = f"""
            UPDATE {schema}
            SET last_result_hash=:last_result_hash,
                last_notification_dt=:last_notification_dt
            WHERE category=:category AND test_suite=:test_suite
            AND test_name=:test_name
            """

    rds_data_client.execute_statement(
        database=GLOBAL_CONFIG["RELEASE_AWS_DB_NAME"],
        parameters=[
            {"name": "category", "value": {"stringValue": category}},
            {"name": "test_suite", "value": {"stringValue": test_suite or ""}},
            {"name": "test_name", "value": {"stringValue": test_name}},
            {"name": "last_result_hash", "value": {"stringValue": result_hash}},
            {
                "name": "last_notification_dt",
                "typeHint": "TIMESTAMP",
                "value": {
                    "stringValue": last_notification_dt.strftime("%Y-%m-%d %H:%M:%S")
                },
            },
        ],
        secretArn=GLOBAL_CONFIG["RELEASE_AWS_DB_SECRET_ARN"],
        resourceArn=GLOBAL_CONFIG["RELEASE_AWS_DB_RESOURCE_ARN"],
        schema=schema,
        sql=sql,
    )


def post_alerts_to_slack(
    channel: str, alerts: List[Tuple[str, str, str, str]], non_alerts: Mapping[str, int]
):
    if len(alerts) == 0:
        logger.info("No alerts to post to slack.")
        return

    markdown_lines = [
        f"* {len(alerts)} new release test failures found!*",
        "",
    ]

    category_alerts = defaultdict(list)
    for (category, test_suite, test_name, alert) in alerts:
        category_alerts[category].append(
            f"   *{test_suite}/{test_name}* failed: {alert}"
        )

    for category, alert_list in category_alerts.items():
        markdown_lines.append(f"Branch: *{category}*")
        markdown_lines.extend(alert_list)
        markdown_lines.append("")

    total_non_alerts = sum(n for n in non_alerts.values())
    non_alert_detail = [f"{n} on {c}" for c, n in non_alerts.items()]

    markdown_lines += [
        f"Additionally, {total_non_alerts} tests passed successfully "
        f"({', '.join(non_alert_detail)})."
    ]

    slack_url = GLOBAL_CONFIG["SLACK_WEBHOOK"]

    resp = requests.post(
        slack_url,
        json={
            "text": "\n".join(markdown_lines),
            "channel": channel,
            "username": "Fail Bot",
            "icon_emoji": ":red_circle:",
        },
    )
    print(resp.status_code)
    print(resp.text)


def post_statistics_to_slack(
    channel: str, alerts: List[Tuple[str, str, str, str]], non_alerts: Mapping[str, int]
):
    total_alerts = len(alerts)

    category_alerts = defaultdict(list)
    for (category, test_suite, test_name, alert) in alerts:
        category_alerts[category].append(f"`{test_suite}/{test_name}`")

    alert_detail = [f"{len(a)} on {c}" for c, a in category_alerts.items()]

    total_non_alerts = sum(n for n in non_alerts.values())
    non_alert_detail = [f"{n} on {c}" for c, n in non_alerts.items()]

    markdown_lines = [
        "*Periodic release test report*",
        "",
        f"In the past 24 hours, "
        f"*{total_non_alerts}* release tests finished successfully, and "
        f"*{total_alerts}* release tests failed.",
    ]

    markdown_lines.append("")

    if total_alerts:
        markdown_lines.append(f"*Failing:* {', '.join(alert_detail)}")
        for c, a in category_alerts.items():
            markdown_lines.append(f"  *{c}*: {', '.join(sorted(a))}")
    else:
        markdown_lines.append("*Failing:* None")

    markdown_lines.append("")

    if total_non_alerts:
        markdown_lines.append(f"*Passing:* {', '.join(non_alert_detail)}")
    else:
        markdown_lines.append("*Passing:* None")

    slack_url = GLOBAL_CONFIG["SLACK_WEBHOOK"]

    resp = requests.post(
        slack_url,
        json={
            "text": "\n".join(markdown_lines),
            "channel": channel,
            "username": "Fail Bot",
            "icon_emoji": ":red_circle:",
        },
    )
    print(resp.status_code)
    print(resp.text)


def handle_results_and_get_alerts(
    rds_data_client,
    fetch_since: Optional[datetime.datetime] = None,
    always_try_alert: bool = False,
    no_status_update: bool = False,
):
    # First build a map of last notifications
    last_notifications_map = {}
    for (
        category,
        test_suite,
        test_name,
        last_result_hash,
        last_notification_dt,
    ) in fetch_latest_alerts(rds_data_client):
        last_notifications_map[(category, test_suite, test_name)] = (
            last_result_hash,
            last_notification_dt,
        )

    alerts = []
    non_alerts = Counter()

    # Then fetch latest results
    for (
        result_hash,
        created_on,
        category,
        test_suite,
        test_name,
        status,
        results,
        artifacts,
        last_logs,
    ) in fetch_latest_results(rds_data_client, fetch_since=fetch_since):
        key = (category, test_suite, test_name)

        try_alert = always_try_alert
        if key in last_notifications_map:
            # If we have an alert for this key, fetch info
            last_result_hash, last_notification_dt = last_notifications_map[key]

            if last_result_hash != result_hash:
                # If we got a new result, handle new result
                try_alert = True
            # Todo: maybe alert again after some time?
        else:
            try_alert = True

        if try_alert:
            handle_fn = SUITE_TO_FN.get(test_suite, None)
            if not handle_fn:
                logger.warning(f"No handle for suite {test_suite}")
                alert = default_handle_result(
                    created_on,
                    category,
                    test_suite,
                    test_name,
                    status,
                    results,
                    artifacts,
                    last_logs,
                )
            else:
                alert = handle_fn(
                    created_on,
                    category,
                    test_suite,
                    test_name,
                    status,
                    results,
                    artifacts,
                    last_logs,
                )

            if alert:
                logger.warning(
                    f"Alert raised for test {test_suite}/{test_name} "
                    f"({category}): {alert}"
                )

                alerts.append((category, test_suite, test_name, alert))
            else:
                logger.debug(
                    f"No alert raised for test {test_suite}/{test_name} "
                    f"({category})"
                )
                non_alerts[category] += 1

            if not no_status_update:
                mark_as_handled(
                    rds_data_client,
                    key in last_notifications_map,
                    category,
                    test_suite,
                    test_name,
                    result_hash,
                    datetime.datetime.now(),
                )

    return alerts, non_alerts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stats",
        action="store_true",
        default=False,
        help="Finish quickly for training.",
    )
    args = parser.parse_args()

    maybe_fetch_slack_webhook()

    rds_data_client = boto3.client("rds-data", region_name="us-west-2")

    if args.stats:
        # Only update last 24 hour stats
        fetch_since = datetime.datetime.now() - datetime.timedelta(days=1)
        alerts, non_alerts = handle_results_and_get_alerts(
            rds_data_client,
            fetch_since=fetch_since,
            always_try_alert=True,
            no_status_update=True,
        )
        post_statistics_to_slack(GLOBAL_CONFIG["SLACK_CHANNEL"], alerts, non_alerts)

    else:
        alerts, non_alerts = handle_results_and_get_alerts(rds_data_client)
        post_alerts_to_slack(GLOBAL_CONFIG["SLACK_CHANNEL"], alerts, non_alerts)
