# __train_model_start__
from sklearn.datasets import make_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from mlflow.entities import LoggedModelStatus
from mlflow.models import infer_signature
import numpy as np


def train_and_register_model():
    # Initialize model in PENDING state
    logged_model = mlflow.initialize_logged_model(
        name="sk-learn-random-forest-reg-model",
        model_type="sklearn",
        tags={"model_type": "random_forest"},
    )

    try:
        with mlflow.start_run() as run:
            X, y = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )

            params = {"max_depth": 2, "random_state": 42}
            
            # Best Practice: Use sklearn Pipeline to persist preprocessing
            # This ensures training and serving transformations stay aligned
            pipeline = Pipeline([
                ("scaler", StandardScaler()),
                ("regressor", RandomForestRegressor(**params))
            ])
            pipeline.fit(X_train, y_train)

            # Log parameters and metrics
            mlflow.log_params(params)

            y_pred = pipeline.predict(X_test)
            mlflow.log_metrics({"mse": mean_squared_error(y_test, y_pred)})

            # Best Practice: Infer model signature for input validation
            # Prevents silent failures from mismatched feature order or missing columns
            signature = infer_signature(X_train, y_pred)

            # Best Practice: Pin dependency versions explicitly
            # Ensures identical behavior across training, evaluation, and serving
            pip_requirements = [
                f"scikit-learn=={__import__('sklearn').__version__}",
                f"numpy=={np.__version__}",
            ]

            # Log the sklearn pipeline with signature and dependencies
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                name="sklearn-model",
                input_example=X_train[:1],
                signature=signature,
                pip_requirements=pip_requirements,
                registered_model_name="sk-learn-random-forest-reg-model",
                model_id=logged_model.model_id,
            )

        # Finalize model as READY
        mlflow.finalize_logged_model(logged_model.model_id, LoggedModelStatus.READY)

        mlflow.set_logged_model_tags(
            logged_model.model_id,
            tags={"production": "true"},
        )

    except Exception as e:
        # Mark model as FAILED if issues occur
        mlflow.finalize_logged_model(logged_model.model_id, LoggedModelStatus.FAILED)
        raise

    # Retrieve and work with the logged model
    final_model = mlflow.get_logged_model(logged_model.model_id)
    print(f"Model {final_model.name} is {final_model.status}")
# __train_model_end__


# __deployment_start__
from ray import serve
import mlflow.pyfunc
import numpy as np


@serve.deployment
class MLflowModelDeployment:
    def __init__(self):
        # Search for models with production tag
        models = mlflow.search_logged_models(
            filter_string="tags.production='true' AND name='sk-learn-random-forest-reg-model'",
            order_by=[{"field_name": "creation_time", "ascending": False}],
        )
        if models.empty:
            raise ValueError("No model with production tag found")
        
        # Get the most recent production model
        model_row = models.iloc[0]
        artifact_location = model_row["artifact_location"]
        
        # Best Practice: Load model once during initialization (warm-start)
        # This eliminates first-request latency spikes
        self.model = mlflow.pyfunc.load_model(artifact_location)
        
        # Pre-warm the model with a dummy prediction
        dummy_input = np.zeros((1, 4))
        _ = self.model.predict(dummy_input)

    async def __call__(self, request):
        data = await request.json()
        features = np.array(data["features"])
        
        # MLflow validates input against the logged signature automatically
        prediction = self.model.predict(features)
        return {"prediction": prediction.tolist()}


app = MLflowModelDeployment.bind()
# __deployment_end__


if __name__ == "__main__":
    import requests
    from ray import serve

    train_and_register_model()
    serve.run(app)

    # Test prediction
    response = requests.post("http://localhost:8000/", json={"features": [[0.1, 0.2, 0.3, 0.4]]})
    print(response.json())
