import os
import pandas as pd
import ray

producer_task_name = "task::ReadRange->MapBatches(produce)"
consumer_task_name = "task::MapBatches(consume)"

COLORS = {
    producer_task_name: "rail_response",
    consumer_task_name: "cq_build_passed",
}

def save_timeline(addr: str):
    ray.timeline(addr)
    df = pd.read_json(addr)
    filtered_df = df[df["cat"].isin(COLORS.keys())]
    filtered_df.loc[:, "cname"] = filtered_df["cat"].apply(lambda x: COLORS[x])
    filtered_df.to_json(addr, orient="records")
    print(f"Processed and saved modified data to {addr}")
