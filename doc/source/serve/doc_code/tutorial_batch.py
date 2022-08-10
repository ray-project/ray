# fmt: off
# __doc_import_begin__
from typing import List

from starlette.requests import Request
from transformers import pipeline, Pipeline

from ray import serve
# __doc_import_end__
# fmt: on


# __doc_define_servable_begin__
@serve.deployment
class BatchTextGenerator:
    def __init__(self, model: Pipeline):
        self.model = model

    @serve.batch(max_batch_size=4)
    async def handle_batch(self, inputs: List[str]) -> List[str]:
        print("Our input array has length:", len(inputs))

        results = self.model(inputs)
        return [result[0]["generated_text"] for result in results]

    async def __call__(self, request: Request) -> List[str]:
        return await self.handle_batch(request.query_params["text"])
        # __doc_define_servable_end__


# __doc_deploy_begin__
model = pipeline("text-generation", "gpt2")
generator = BatchTextGenerator.bind(model)
# __doc_deploy_end__
