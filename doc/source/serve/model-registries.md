# Model Registry Integration

Ray Serve is Python-native, which means it integrates seamlessly with the broader MLOps ecosystem. You can easily connect Ray Serve deployments to Model Registry, enabling production-ready ML workflows without complex configuration or glue code. This guide shows you how to integrate Ray Serve with Model Registry to build end-to-end ML serving pipelines.

## Why Python-native integration matters

Unlike framework-specific serving solutions that require custom adapters or complex configuration, Ray Serve runs arbitrary Python code. This means you can:

- Load models directly from any model registry using standard Python clients
- Combine model loading and inference in a single deployment
- Iterate quickly without wrestling with YAML configurations or custom serialization formats

(mlflow-serving-intig)=
## Integrate with MLflow

[MLflow](https://mlflow.org/) is a popular open-source platform for managing the ML lifecycle. Ray Serve makes it easy to load models from MLflow Model Registry and serve them in production.

### Best practices for serving MLflow models

1. Use model signatures and input schema validation: Always log a model signature using `mlflow.models.infer_signature` so MLflow can validate inputs. This prevents silent failures when upstream code changes and enables automatic schema enforcement during serving.

2. Package dependencies explicitly: Use `pip_requirements` when logging models and pin versions of core libraries. This ensures your model behaves identically across training, evaluation, and serving environments.

3. Persist preprocessing pipelines: If you use scikit-learn, log complete `Pipeline` objects that include preprocessing steps. This ensures training and serving transformations stay aligned.

4. For LLMs and diffusion models, use Hugging Face Hub or Weights & Biases: MLflow's built-in REST server isn't optimized for high-concurrency GPU workloads. For large language models, diffusion models, and other heavy transformer-based architectures, use [Hugging Face Hub](https://huggingface.co/docs/hub/) or [Weights & Biases](https://wandb.ai/) as your model registry. These platforms provide better tooling for large model artifacts, and Ray Serve handles GPU batching, autoscaling, and scheduling efficiently.

### Train and register a model

The following example shows how to train a scikit-learn model with best practices and register it with MLflow:

```{literalinclude} doc_code/mlflow_model_registry_integration.py
:language: python
:start-after: __train_model_start__
:end-before: __train_model_end__
```

This function trains a RandomForestRegressor wrapped in a Pipeline with preprocessing, logs the model with a signature and pinned dependencies, and registers it in MLflow Model Registry with the name `sk-learn-random-forest-reg-model`.

### Load and serve the model

Once you've registered a model in MLflow, you can load and serve it with Ray Serve. The following example shows how to create a deployment that loads a model from MLflow Model Registry with warm-start initialization:

```{literalinclude} doc_code/mlflow_model_registry_integration.py
:language: python
:start-after: __deployment_start__
:end-before: __deployment_end__
```
