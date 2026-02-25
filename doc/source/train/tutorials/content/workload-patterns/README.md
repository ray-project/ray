<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify README.ipynb instead, then regenerate this file with:
jupyter nbconvert "README.ipynb" --to markdown --output "README.md"
-->

# ML workload patterns with Ray Train

Pre-install all requirements
```bash
!pip install -r ../requirements.txt
```

These tutorials demonstrate common ML workload patterns with Ray Train. They're independent and can be taken in any order.

- **[Vision workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04a_vision_pattern.html)** - Computer vision with Food-101 dataset
- **[Tabular workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04b_tabular_workload_pattern.html)** - XGBoost with CoverType dataset
- **[Time series workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04c_time_series_workload_pattern.html)** - Transformer model for New York City taxi demand
- **[Generative computer vision workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04d1_generative_cv_pattern.html)** - Mini diffusion pipeline
- **[Policy learning workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04d2_policy_learning_pattern.html)** - Diffusion-policy on Pendulum-v1
- **[Recommendation system workloads](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04e_rec_sys_workload_pattern.html)** - Matrix factorization with MovieLens
