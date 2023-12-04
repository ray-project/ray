# flake8: noqa
# fmt: off

from typing import List

# __textbot_setup_start__
import asyncio
import logging
from queue import Empty

from fastapi import FastAPI
from starlette.responses import StreamingResponse
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer

from ray import serve

logger = logging.getLogger("ray.serve")
# __textbot_setup_end__


# __textbot_constructor_start__
fastapi_app = FastAPI()


@serve.deployment
@serve.ingress(fastapi_app)
class Textbot:
    def __init__(self, model_id: str):
        self.loop = asyncio.get_running_loop()

        self.model_id = model_id
        self.model = AutoModelForCausalLM.from_pretrained(self.model_id)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)

    # __textbot_constructor_end__

    # __textbot_logic_start__
    @fastapi_app.post("/")
    def handle_request(self, prompt: str) -> StreamingResponse:
        logger.info(f'Got prompt: "{prompt}"')
        streamer = TextIteratorStreamer(
            self.tokenizer, timeout=0, skip_prompt=True, skip_special_tokens=True
        )
        self.loop.run_in_executor(None, self.generate_text, prompt, streamer)
        return StreamingResponse(
            self.consume_streamer(streamer), media_type="text/plain"
        )

    def generate_text(self, prompt: str, streamer: TextIteratorStreamer):
        input_ids = self.tokenizer([prompt], return_tensors="pt").input_ids
        self.model.generate(input_ids, streamer=streamer, max_length=10000)

    async def consume_streamer(self, streamer: TextIteratorStreamer):
        while True:
            try:
                for token in streamer:
                    logger.info(f'Yielding token: "{token}"')
                    yield token
                break
            except Empty:
                # The streamer raises an Empty exception if the next token
                # hasn't been generated yet. `await` here to yield control
                # back to the event loop so other coroutines can run.
                await asyncio.sleep(0.001)

    # __textbot_logic_end__


# __textbot_bind_start__
app = Textbot.bind("microsoft/DialoGPT-small")
# __textbot_bind_end__


serve.run(app)

chunks = []
# __stream_client_start__
import requests

prompt = "Tell me a story about dogs."

response = requests.post(f"http://localhost:8000/?prompt={prompt}", stream=True)
response.raise_for_status()
for chunk in response.iter_content(chunk_size=None, decode_unicode=True):
    print(chunk, end="")

    # Dogs are the best.
    # __stream_client_end__
    chunks.append(chunk)

# Check that streaming is happening.
assert chunks == ["Dogs ", "are ", "the ", "best."]


# __chatbot_setup_start__
import asyncio
import logging
from queue import Empty

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer

from ray import serve

logger = logging.getLogger("ray.serve")
# __chatbot_setup_end__


# __chatbot_constructor_start__
fastapi_app = FastAPI()


@serve.deployment
@serve.ingress(fastapi_app)
class Chatbot:
    def __init__(self, model_id: str):
        self.loop = asyncio.get_running_loop()

        self.model_id = model_id
        self.model = AutoModelForCausalLM.from_pretrained(self.model_id)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)

    # __chatbot_constructor_end__

    # __chatbot_logic_start__
    @fastapi_app.websocket("/")
    async def handle_request(self, ws: WebSocket) -> None:
        await ws.accept()

        conversation = ""
        try:
            while True:
                prompt = await ws.receive_text()
                logger.info(f'Got prompt: "{prompt}"')
                conversation += prompt
                streamer = TextIteratorStreamer(
                    self.tokenizer,
                    timeout=0,
                    skip_prompt=True,
                    skip_special_tokens=True,
                )
                self.loop.run_in_executor(
                    None, self.generate_text, conversation, streamer
                )
                response = ""
                async for text in self.consume_streamer(streamer):
                    await ws.send_text(text)
                    response += text
                await ws.send_text("<<Response Finished>>")
                conversation += response
        except WebSocketDisconnect:
            print("Client disconnected.")

    def generate_text(self, prompt: str, streamer: TextIteratorStreamer):
        input_ids = self.tokenizer([prompt], return_tensors="pt").input_ids
        self.model.generate(input_ids, streamer=streamer, max_length=10000)

    async def consume_streamer(self, streamer: TextIteratorStreamer):
        while True:
            try:
                for token in streamer:
                    logger.info(f'Yielding token: "{token}"')
                    yield token
                break
            except Empty:
                await asyncio.sleep(0.001)


# __chatbot_logic_end__


# __chatbot_bind_start__
app = Chatbot.bind("microsoft/DialoGPT-small")
# __chatbot_bind_end__

serve.run(app)

chunks = []
# Monkeypatch `print` for testing
original_print, print = print, (lambda chunk, end=None: chunks.append(chunk))

# __ws_client_start__
from websockets.sync.client import connect

with connect("ws://localhost:8000") as websocket:
    websocket.send("Space the final")
    while True:
        received = websocket.recv()
        if received == "<<Response Finished>>":
            break
        print(received, end="")
    print("\n")

    websocket.send(" These are the voyages")
    while True:
        received = websocket.recv()
        if received == "<<Response Finished>>":
            break
        print(received, end="")
    print("\n")
# __ws_client_end__

assert chunks == [
    " ",
    "",
    "",
    "frontier.",
    "\n",
    " ",
    "of ",
    "the ",
    "starship ",
    "",
    "",
    "Enterprise.",
    "\n",
]

print = original_print

# __batchbot_setup_start__
import asyncio
import logging
from queue import Empty, Queue

from fastapi import FastAPI
from transformers import AutoModelForCausalLM, AutoTokenizer

from ray import serve

logger = logging.getLogger("ray.serve")
# __batchbot_setup_end__

# __raw_streamer_start__
class RawStreamer:
    def __init__(self, timeout: float = None):
        self.q = Queue()
        self.stop_signal = None
        self.timeout = timeout

    def put(self, values):
        self.q.put(values)

    def end(self):
        self.q.put(self.stop_signal)

    def __iter__(self):
        return self

    def __next__(self):
        result = self.q.get(timeout=self.timeout)
        if result == self.stop_signal:
            raise StopIteration()
        else:
            return result


# __raw_streamer_end__

# __batchbot_constructor_start__
fastapi_app = FastAPI()


@serve.deployment
@serve.ingress(fastapi_app)
class Batchbot:
    def __init__(self, model_id: str):
        self.loop = asyncio.get_running_loop()

        self.model_id = model_id
        self.model = AutoModelForCausalLM.from_pretrained(self.model_id)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)
        self.tokenizer.pad_token = self.tokenizer.eos_token

    # __batchbot_constructor_end__

    # __batchbot_logic_start__
    @fastapi_app.post("/")
    async def handle_request(self, prompt: str) -> StreamingResponse:
        logger.info(f'Got prompt: "{prompt}"')
        return StreamingResponse(self.run_model(prompt), media_type="text/plain")

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=15)
    async def run_model(self, prompts: List[str]):
        streamer = RawStreamer()
        self.loop.run_in_executor(None, self.generate_text, prompts, streamer)
        on_prompt_tokens = True
        async for decoded_token_batch in self.consume_streamer(streamer):
            # The first batch of tokens contains the prompts, so we skip it.
            if not on_prompt_tokens:
                logger.info(f"Yielding decoded_token_batch: {decoded_token_batch}")
                yield decoded_token_batch
            else:
                logger.info(f"Skipped prompts: {decoded_token_batch}")
                on_prompt_tokens = False

    def generate_text(self, prompts: str, streamer: RawStreamer):
        input_ids = self.tokenizer(prompts, return_tensors="pt", padding=True).input_ids
        self.model.generate(input_ids, streamer=streamer, max_length=10000)

    async def consume_streamer(self, streamer: RawStreamer):
        while True:
            try:
                for token_batch in streamer:
                    decoded_tokens = []
                    for token in token_batch:
                        decoded_tokens.append(
                            self.tokenizer.decode(token, skip_special_tokens=True)
                        )
                    logger.info(f"Yielding decoded tokens: {decoded_tokens}")
                    yield decoded_tokens
                break
            except Empty:
                await asyncio.sleep(0.001)


# __batchbot_logic_end__


# __batchbot_bind_start__
app = Batchbot.bind("microsoft/DialoGPT-small")
# __batchbot_bind_end__

serve.run(app)

# Test batching code
from functools import partial
from concurrent.futures.thread import ThreadPoolExecutor


def get_buffered_response(prompt) -> List[str]:
    response = requests.post(f"http://localhost:8000/?prompt={prompt}", stream=True)
    chunks = []
    for chunk in response.iter_content(chunk_size=None, decode_unicode=True):
        chunks.append(chunk)
    return chunks


with ThreadPoolExecutor() as pool:
    futs = [
        pool.submit(partial(get_buffered_response, prompt))
        for prompt in ["Introduce yourself to me!", "Tell me a story about dogs."]
    ]
    responses = [fut.result() for fut in futs]
    assert responses == [
        ["I", "'m", " not", " sure", " if", " I", "'m", " ready", " for", " that", "."],
        ["D", "ogs", " are", " the", " best", "."],
    ]
