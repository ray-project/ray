import json

import ray
from ray import workflow


@workflow.step
def extract() -> dict:
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    order_data_dict = json.loads(data_string)
    return order_data_dict


@workflow.step
def transform(order_data_dict: dict) -> dict:
    total_order_value = 0
    for value in order_data_dict.values():
        total_order_value += value
    return {"total_order_value": ray.put(total_order_value)}


@workflow.step
def load(data_dict: dict) -> str:
    total_order_value = ray.get(data_dict["total_order_value"])
    return f"Total order value is: {total_order_value:.2f}"


if __name__ == "__main__":
    workflow.init()
    order_data = extract.step()
    order_summary = transform.step(order_data)
    etl = load.step(order_summary)
    print(etl.run())
