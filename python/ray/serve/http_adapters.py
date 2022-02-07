from typing import List, Union
import numpy as np
from ray.serve.model_wrappers import DataBatchType
from pydantic import BaseModel


class Ndarray(BaseModel):
    array: Union[List[float], List[List[float]], List[List[List[float]]]]


def serve_api_resolver(payload: Ndarray) -> DataBatchType:
    return np.array(payload.array)


from fastapi import File
from PIL import Image
from io import BytesIO
import numpy as np


async def image_to_databatch(img: bytes = File(...)) -> np.ndarray:
    image = Image.open(BytesIO(img))
    return np.array(image)
