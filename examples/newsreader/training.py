from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import bz2
import json

from vowpalwabbit import pyvw


def load_hn_submissions(path):
    records = []
    with open(path, "r") as f:
        for line in f.readlines():
            body = json.loads(line)["body"]
            if "title" in body and "score" in body:
                records.append((body["title"], body["score"]))
    return records


def create_datapoints(records, cutoff):
    datapoints = []
    for title, score in records:
        if score >= cutoff:
            label = "1"
        else:
            label = "-1"
        features = [token for token in title.split(" ") if ":" not in token]
        datapoints.append(label + " | " + " ".join(features))
    return datapoints


def learn_model(datapoints):
    vw = pyvw.vw()
    for record in records:
        example = vw.example(record)
        vw.learn(example)
    return vw
