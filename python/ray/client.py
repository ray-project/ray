import requests
import pyarrow
import pickle
import numpy as np

ctx = pyarrow.default_serialization_context()
val = np.random.rand(5, 5)
obj = ctx.serialize(val)

data = obj.to_buffer().to_pybytes()


res = requests.post(url="http://localhost:5000",
                    files={
                        "data": data,
                        "meta": pickle.dumps(123)
                    })
