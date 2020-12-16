import mlflow.pyfunc
import pandas as pd
from mlflow.deployments import get_deploy_client


# Define the model class
class AddN(mlflow.pyfunc.PythonModel):
    def __init__(self, n):
        self.n = n

    def predict(self, context, model_input):
        return model_input.apply(lambda column: column + self.n)


# Construct and save the model
model_path = "/Users/archit/ray/add_n_model"
add5_model = AddN(n=5)
mlflow.pyfunc.save_model(path=model_path, python_model=add5_model)

# Evaluate the model
client = get_deploy_client("ray-serve")

client.create_deployment("add5", "/Users/archit/ray/add_n_model")
print(client.list_deployments())
print(client.get_deployment("add5"))

model_input = pd.DataFrame([range(10)])
model_output = client.predict("add5", model_input)
assert model_output.equals(pd.DataFrame([range(5, 15)]))

client.delete_deployment("add5")
