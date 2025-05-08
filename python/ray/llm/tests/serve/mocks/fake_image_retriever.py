import numpy as np
from PIL import Image

from ray.llm._internal.serve.deployments.llm.image_retriever import ImageRetriever


class FakeImageRetriever(ImageRetriever):
    def __init__(self):
        pass

    async def get(self, url: str) -> Image.Image:
        height, width = 256, 256
        random_image = np.random.randint(0, 256, (height, width, 3), dtype=np.uint8)

        return Image.fromarray(random_image)
