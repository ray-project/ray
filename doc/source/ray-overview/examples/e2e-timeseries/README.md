# Time-series forecasting with DLinear


```{toctree}
:hidden:

e2e_timeseries/01-Distributed-Training
e2e_timeseries/02-Validation
e2e_timeseries/03-Serving

```


<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/e2e-timeseries-forecasting?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_content=run_on_anyscale&utm_campaign=e2e-timeseries-forecasting"><img src="https://raw.githubusercontent.com/ray-project/ray/c34b74c22a9390aa89baf80815ede59397786d2e/doc/source/_static/img/run-on-anyscale.svg" alt="Run on Anyscale">
<br></br>
<a href="https://github.com/anyscale/e2e-timeseries" role="button"><img src="https://img.shields.io/static/v1?label=&message=View%20On%20GitHub&color=586069&logo=github&labelColor=2f363d"></a>
</div>


These tutorials implement an end-to-end time-series application including:

- **Distributed data preprocessing and model training**: Ingest and preprocess data at scale using [Ray Data](https://docs.ray.io/en/latest/data/data.html). Then, train a distributed [DLinear model](https://github.com/cure-lab/LTSF-Linear) using [Ray Train](https://docs.ray.io/en/latest/train/train.html).

- **Model validation using offline inference**: Evaluate the model using Ray Data offline batch inference.

- **Online model serving**: Deploy the model as a scalable online service using [Ray Serve](https://docs.ray.io/en/latest/serve/index.html).

- **Production deployment**: Create production batch Jobs for offline workloads including data prep, training, batch prediction, and potentially online Services.

## Setup

Run the following:

```bash
pip install -r requirements.txt && pip install -e .
```

## Acknowledgements

This repository is based on the official `DLinear` implementations:
- [`DLinear`](https://github.com/vivva/DLinear)
- [`LTSF-Linear`](https://github.com/cure-lab/LTSF-Linear)

And the original publication:
- ["Are Transformers Effective for Time Series Forecasting?"](https://arxiv.org/abs/2205.13504)

