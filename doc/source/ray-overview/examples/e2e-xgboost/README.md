# Classification with XGBoost


```{toctree}
:hidden:

notebooks/01-Distributed_Training
notebooks/02-Validation
notebooks/03-Serving

```


<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/xgboost-training-and-serving?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_content=run_on_anyscale&utm_campaign=xgboost-training-and-serving"><img src="https://raw.githubusercontent.com/ray-project/ray/c34b74c22a9390aa89baf80815ede59397786d2e/doc/source/_static/img/run-on-anyscale.svg" alt=\"Run on Anyscale\">
<br></br>
<a href="https://github.com/anyscale/e2e-xgboost" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>


<div align="center">
  <img src="https://raw.githubusercontent.com/anyscale/e2e-xgboost/refs/heads/main/images/overview.png" width=800>
</div>

These tutorials implement an end-to-end XGBoost application including:


- **Distributed data preprocessing and model training**: Ingest and preprocess data at scale using [Ray Data](https://docs.ray.io/en/latest/data/data.html). Then, train a distributed [XGBoost model](https://xgboost.readthedocs.io/en/stable/python/index.html) using [Ray Train](https://docs.ray.io/en/latest/train/train.html). See [Distributed training of an XGBoost model](./notebooks/01-Distributed_Training.ipynb).
- **Model validation using offline inference**: Evaluate the model using Ray Data offline batch inference. See [Model validation using offline batch inference](./notebooks/02-Validation.ipynb).
- **Online model serving**: Deploy the model as a scalable online service using [Ray Serve](https://docs.ray.io/en/latest/serve/index.html). See [Scalable online XGBoost inference with Ray Serve](./notebooks/03-Serving.ipynb).
- **Production deployment**: Create production batch [**Jobs**](https://docs.anyscale.com/platform/jobs/) for offline workloads including data prep, training, batch prediction, and potentially online [**Services**](https://docs.anyscale.com/platform/services/).



# Dependencies

To install the dependencies, run the following:

```bash
pip install -r requirements.txt
```

