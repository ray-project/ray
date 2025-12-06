# Model Registry integration

Ray Serve is Python-native, which means it integrates seamlessly with the broader MLOps ecosystem. You can easily connect Ray Serve deployments to Model Registry, enabling production-ready ML workflows without complex configuration or glue code. This guide shows you how to integrate Ray Serve with Model Registry to build end-to-end ML serving pipelines.

## Why Python-native integration matters

Unlike framework-specific serving solutions that require custom adapters or complex configuration, Ray Serve runs arbitrary Python code. This means you can:

- Load models directly from any model registry using standard Python clients
- Combine model loading and inference in a single deployment
- Iterate quickly without wrestling with YAML configurations or custom serialization formats

(mlflow-serving-intig)=
## Integrate with MLflow

[MLflow](https://mlflow.org/) is a popular open-source platform for managing the ML lifecycle. Ray Serve makes it easy to load models from MLflow Model Registry and serve them in production.

### Train and register a model

The following example shows how to train a scikit-learn model and register it with MLflow:

```{literalinclude} doc_code/mlflow_model_registry_integration.py
:language: python
:start-after: __train_model_start__
:end-before: __train_model_end__
```

This function trains a RandomForestRegressor, logs the model parameters and metrics, and registers the model in MLflow Model Registry with the name `sk-learn-random-forest-reg-model`.

### Load and serve the model

Once you've registered a model in MLflow, you can load and serve it with Ray Serve. The following example shows how to create a deployment that loads a model from MLflow Model Registry:

```{literalinclude} doc_code/mlflow_model_registry_integration.py
:language: python
:start-after: __deployment_start__
:end-before: __deployment_end__
```
