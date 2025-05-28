from typing import List
from google.cloud import logging_v2
import requests

def list_logs(project_id: str) -> List[str]:
    """Lists all logs in a project.

    Args:
        project_id: the ID of the project

    Returns:
        A list of log names.
    """
    client = logging_v2.services.logging_service_v2.LoggingServiceV2Client()
    # Filter logs for k8s containers with specific ray_submission_id
    filter_query = 'resource.type="k8s_container" AND jsonPayload.ray_submission_id="tune-scalability-bookkeeping-overhead-kuberay-pvxtb"'

    print("Listing logs...")
    page_token = ""
    next_page_token = ""
    request_cnt = 0
    line_cnt = 0
    # url = "https://logging.googleapis.com/v2/entries:list?key=AIzaSyCDQ5xHcRQ9a5YnicUTpM292SE116SrFO8"
    # request = {
    #     "resource_names": [f"projects/{project_id}"],
    #     "filter": filter_query,
    #     "pageSize": 10,
    # }
    # headers = {
    #     "Content-Type": "application/json; charset=utf-8"
    # }
    # # "X-goog-api-key": "AIzaSyAZ_doK1NfEbrXRz5jkh7tF9PYBNkgvKZY",
    # response = requests.post(url, json=request, headers=headers)
    # print(response.json())
    result = client.list_log_entries(
        request={
                "resource_names": [f"projects/{project_id}"],
                "filter": filter_query,
                "page_size": 100000,
                "page_token": page_token,
            },
            timeout=300,
        )
    request_cnt += 1
    next_page_token = result.next_page_token
    while next_page_token:
        line_cnt += len(result.entries)
        result = client.list_log_entries(
        request={
                "resource_names": [f"projects/{project_id}"],
                "filter": filter_query,
                "page_size": 100000,
                "page_token": next_page_token,
        },
        timeout=300,
        )
        request_cnt += 1
        next_page_token = result.next_page_token
        print("next page token: ", next_page_token)
        print("#entries: ", len(result.entries))
        with open("logs.txt", "w") as f:
            for entry in result.entries:
                log_message = entry.json_payload["log"]
                f.write(log_message + "\n")
                timestamp = entry.receive_timestamp
                f.write(str(timestamp) + "\n")
        print("CNT: ", request_cnt)
        print("LINE CNT: ", line_cnt)

list_logs("dhyey-dev")