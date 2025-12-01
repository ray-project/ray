# __train_model_start__
from sklearn.datasets import make_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import mlflow
import mlflow.sklearn


def train_and_register_model():
    with mlflow.start_run() as run:
        X, y = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        params = {"max_depth": 2, "random_state": 42}
        model = RandomForestRegressor(**params)
        model.fit(X_train, y_train)

        # Log parameters and metrics using the MLflow APIs
        mlflow.log_params(params)

        y_pred = model.predict(X_test)
        mlflow.log_metrics({"mse": mean_squared_error(y_test, y_pred)})

        # Log the sklearn model and register as version 1
        mlflow.sklearn.log_model(
            sk_model=model,
            name="sklearn-model",
            input_example=X_train,
            registered_model_name="sk-learn-random-forest-reg-model",
        )
# __train_model_end__


# __deployment_start__
from ray import serve
import mlflow.sklearn


@serve.deployment
class MLflowModelDeployment:
    def __init__(self, model_uri: str):
        self.model = mlflow.sklearn.load_model(model_uri)

    async def __call__(self, request):
        data = await request.json()
        prediction = self.model.predict(data["features"])
        return {"prediction": prediction.tolist()}


app = MLflowModelDeployment.bind(model_uri="models:/sk-learn-random-forest-reg-model/latest")
# __deployment_end__


if __name__ == "__main__":
    import requests
    from ray import serve

    train_and_register_model()
    serve.run(app)

    # predict
    response = requests.post("http://localhost:8000/", json={"features": [[0.1, 0.2, 0.3, 0.4]]})
    print(response.json())


# __ab_testing_train_models_start__
import random
import time


def train_and_register_two_models():
    """Train two models with different parameters and register them with MLflow."""
    X, y = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train model version 1 with fewer estimators
    params_v1 = {"max_depth": 2, "random_state": 42, "n_estimators": 50}
    model_v1 = RandomForestRegressor(**params_v1)
    model_v1.fit(X_train, y_train)

    mlflow.sklearn.log_model(
        sk_model=model_v1,
        artifact_path="sklearn-model",
        input_example=X_train[:5],
        registered_model_name="ab-test-rf-model",
    )
    
    # Tag as version 1
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions("name='ab-test-rf-model'")
    v1_version = versions[0].version
    client.set_model_version_tag(
        name="ab-test-rf-model",
        version=v1_version,
        key="model_version",
        value="v1"
    )
    model_v1_uri = f"models:/ab-test-rf-model/{v1_version}"

    # Train model version 2 with more estimators
    params_v2 = {"max_depth": 5, "random_state": 42, "n_estimators": 100}
    model_v2 = RandomForestRegressor(**params_v2)
    model_v2.fit(X_train, y_train)

    mlflow.sklearn.log_model(
        sk_model=model_v2,
        artifact_path="sklearn-model",
        input_example=X_train[:5],
        registered_model_name="ab-test-rf-model",
    )
    
    # Tag as version 2
    versions = client.search_model_versions("name='ab-test-rf-model'")
    v2_version = versions[0].version
    client.set_model_version_tag(
        name="ab-test-rf-model",
        version=v2_version,
        key="model_version",
        value="v2"
    )
    model_v2_uri = f"models:/ab-test-rf-model/{v2_version}"

    return model_v1_uri, model_v2_uri
# __ab_testing_train_models_end__


# __ab_testing_router_start__
@serve.deployment
class ModelVersionDeployment:
    def __init__(self, model_uri: str, version: str):
        self.model = mlflow.sklearn.load_model(model_uri)
        self.version = version

    async def __call__(self, data):
        start_time = time.time()
        prediction = self.model.predict(data)
        inference_time = time.time() - start_time
        
        return {
            "prediction": prediction.tolist(),
            "version": self.version,
            "inference_time_ms": inference_time * 1000
        }


@serve.deployment
class ABTestRouter:
    def __init__(self, model_v1_handle, model_v2_handle):
        self.model_v1 = model_v1_handle
        self.model_v2 = model_v2_handle
        self.request_count = {"v1": 0, "v2": 0}
        # Set up MLflow experiment for tracking all A/B test metrics
        mlflow.set_experiment("ray-serve-ab-experiment")

    async def __call__(self, request):
        data = await request.json()
        features = data["features"]
        
        # Randomly route to v1 or v2
        if random.random() < 0.5:
            result = await self.model_v1.remote(features)
        else:
            result = await self.model_v2.remote(features)
        
        # Track inference metrics in MLflow
        version = result["version"]
        self.request_count[version] += 1
        
        with mlflow.start_run(run_name="ab-test-run", nested=True):
            # Log which version handled the request
            mlflow.log_param("model_version", version)
            mlflow.log_param("num_features", len(features[0]))
            
            # Log inference metrics
            mlflow.log_metrics({
                "inference_time_ms": result["inference_time_ms"],
                "prediction_value": float(result["prediction"][0]),
            })
        
        return result
# __ab_testing_router_end__

# __ab_testing_run_start__
if __name__ == "__main__":
    import requests
    
    # Train and register two models with version tags
    model_v1_uri, model_v2_uri = train_and_register_two_models()
    
    # Create and deploy the AB test application
    model_v1 = ModelVersionDeployment.options(
        name="model-v1",
    ).bind(model_uri=model_v1_uri, version="v1")
    model_v2 = ModelVersionDeployment.options(
        name="model-v2",
    ).bind(model_uri=model_v2_uri, version="v2")
    app = ABTestRouter.bind(model_v1_handle=model_v1, model_v2_handle=model_v2)
    serve.run(app)
    
    # Send multiple requests to see routing and track inference metrics
    print("\nSending requests to AB test router:")
    print("MLflow tracks inference metrics for each request:\n")
    
    for i in range(100):
        response = requests.post(
            "http://localhost:8000/",
            json={"features": [[0.1, 0.2, 0.3, 0.4]]}
        )
        result = response.json()
        print(
            f"Request {i+1}: Routed to {result['version']}, "
            f"Prediction: {result['prediction']}, "
            f"Latency: {result['inference_time_ms']:.2f}ms"
        )
# __ab_testing_run_end__
