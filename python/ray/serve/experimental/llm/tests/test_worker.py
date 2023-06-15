import pytest
from copy import deepcopy
from ray.serve.experimental.llm.models.model import _reset_batch_id


def test_pass_through(default_worker, default_pb_batch):
    _reset_batch_id()
    generations, batch_id = default_worker.process_new_batch(default_pb_batch)
    assert len(generations) == len(default_pb_batch)
    assert generations[0].request_id == default_pb_batch[0].id
    assert generations[0].token_text == "."
    assert batch_id == 1

    request0 = deepcopy(default_pb_batch[0])
    request0.id = 2
    request1 = deepcopy(default_pb_batch[0])
    request1.id = 3
    request1.input_text = "Hello"
    batch1 = [request0, request1]
    generations, batch_id = default_worker.process_new_batch(batch1)
    assert len(generations) == len(batch1)
    assert generations[0].request_id == batch1[0].id
    assert generations[0].token_text == "."
    assert generations[1].request_id == batch1[1].id
    assert generations[1].token_text == ","
    assert batch_id == 2

    generations, batch_id = default_worker.generate_next_token([1, 2])
    assert len(generations) == len(batch1) + len(default_pb_batch)

    assert generations[0].request_id == default_pb_batch[0].id
    assert generations[0].token_text == "java"
    assert generations[1].request_id == batch1[0].id
    assert generations[1].token_text == "java"
    assert generations[2].request_id == batch1[1].id
    assert generations[2].token_text == " I"
    assert batch_id == 3

    assert 4 == default_worker.filter_requests(3, [0, 3])
    exception_thrown = False
    try:
        generations, batch_id = default_worker.generate_next_token([3])
    except ValueError as e:
        exception_thrown = True

    assert exception_thrown

    generations, batch_id = default_worker.generate_next_token([4])
    assert len(generations) == 2
    assert generations[0].request_id == default_pb_batch[0].id
    assert generations[0].token_text == ":"
    assert generations[1].request_id == request1.id
    assert generations[1].token_text == "'m"
    assert batch_id == 4
