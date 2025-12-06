# __train_model_start__
from sklearn.datasets import make_regression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import mlflow
import mlflow.sklearn
from mlflow.entities import LoggedModelStatus


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
            model = RandomForestRegressor(**params)
            model.fit(X_train, y_train)

            # Log parameters and metrics using the MLflow APIs
            mlflow.log_params(params)

            y_pred = model.predict(X_test)
            mlflow.log_metrics({"mse": mean_squared_error(y_test, y_pred)})

            # Log the sklearn model and link to the logged model
            mlflow.sklearn.log_model(
                sk_model=model,
                name="sklearn-model",
                input_example=X_train,
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
import mlflow.sklearn


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
        
        # Get the most recent production model and load it
        model_row = models.iloc[0]
        artifact_location = model_row["artifact_location"]
        self.model = mlflow.sklearn.load_model(artifact_location)

    async def __call__(self, request):
        data = await request.json()
        prediction = self.model.predict(data["features"])
        return {"prediction": prediction.tolist()}


app = MLflowModelDeployment.bind()
# __deployment_end__


if __name__ == "__main__":
    import requests
    from ray import serve

    train_and_register_model()
    serve.run(app)

    # predict
    response = requests.post("http://localhost:8000/", json={"features": [[0.1, 0.2, 0.3, 0.4]]})
    print(response.json())
