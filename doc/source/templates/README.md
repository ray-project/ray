# Ray Starter Templates

These templates are a set of minimal examples that are quick and easy to run and customize.

Although the templates may include some machine learning framework-specific code,
the individual code blocks are meant to be swapped in with your own application logic.
The templates just serve as skeletons that showcase popular applications of Ray.

## Running on a Ray Cluster

<!-- TODO(justinvyu): Add in OSS cluster support. -->
Coming soon...

## Contributing Guide

To add a template:

1. Add your template as a directory somewhere in the Ray repo.

    For example:

    ```text
    ray/
        doc/source/templates/
            <name-of-your-template>/
                README.md
                <name-of-your-template>.ipynb
    ```

    If your template requires any special dependencies that are not included in a
    base `ray-ml` Docker image, be sure to list and install the necessary dependencies
    within the notebook.

    ```{note}
    The template should be self-contained and not require any external files.
    This requirement is to simplify the testing procedure.
    ```

2. Add another copy of the template that includes test-specific code.

    NOTE: The need for a second test copy is temporary. Only one notebook will be needed
    from 2.5 onward.

    **Label all test-specific code with the `remove-cell` Jupyter notebook tag.**

    **Put this test copy in `doc/source/templates/tests/test-copy.ipynb`.**

3. Add a smoke-test version of the template to run in pre-merge CI in `doc/source/templates/BUILD`.

    Set the `SMOKE_TEST` environment variable, which should be used in your template to
    set certain smoke test parameters (like limiting dataset size).

    **Make sure that you tag the test with `"gpu"` if required, and any other tags
    needed for special dependencies.

    ```python
    py_test_run_all_notebooks(
        size = "large",
        include = ["tests/batch_inference.ipynb"],
        exclude = [],
        data = ["//doc/source/templates:workspace_templates"],
        tags = ["exclusive", "team:ml", "ray_air", "gpu", "doc_tested"],
        env = {"SMOKE_TEST": "1"},
    )
    ```

4. Add a release test for the template in `release/release_tests.yaml` (for both AWS and GCE).

    **Use the `release_test_cluster_env.yaml` and `*_release_test.yaml` files for cluster env / compute configs.**
    These contain placeholders for regions and cloud ids that our CI infra will fill in.

    ```yaml
    - name: workspace_template_small_02_many_model_training
      group: Workspace templates
      working_dir: workspace_templates/tests
      python: "3.9"
      frequency: nightly-3x
      team: ml
      cluster:
        cluster_env: ../configs/release_test_cluster_env.yaml
        cluster_compute: ../configs/compute/cpu/aws_release_test.yaml

      variations:
          - __suffix__: aws
          - __suffix__: gce
            env: gce
            frequency: manual
            cluster:
              cluster_env: ../configs/release_test_cluster_env.yaml
              cluster_compute: ../configs/compute/gpu/gce_release_test.yaml

    run:
      timeout: 300
      script: jupyter nbconvert --to script --output _test many_model_training.ipynb && ipython _test.py
    ```

5. (Anyscale Only) To make this template show up as an Anyscale Workspace Template:

    Make a PR on the product repo that links this template as an entry in the
    [`workspace-templates.yaml` file](https://github.com/anyscale/product/blob/master/backend/workspace-templates.yaml).

    **Use the `anyscale_cluster_env.yaml`, `gce.yaml`, and `aws.yaml` files, NOT the release test counterparts.**

    **Make sure the entry name (e.g., `batch-inference-ray-data`) is unique.**

    ```yaml
    batch-inference-ray-data:
      title: Batch Inference
      description: Scaling Batch Inference with Ray Data
      path: doc/source/templates/01_batch_inference
      cluster_env: doc/source/templates/configs/anyscale_cluster_env.yaml
      compute_config:
        GCP: doc/source/templates/configs/compute/gpu/gce.yaml
        AWS: doc/source/templates/configs/compute/gpu/aws.yaml
    ```

6. Success! Your template is ready for review.

<!-- 3. Add an entry to `doc/source/templates/templates.yaml` that links to your template.

    ```yaml
    - name: Many Model Training using Ray Tune
      # Paths should be relative to the Ray repo root directory
      path: doc/source/templates/02_many_model_training
      cluster_env: doc/source/templates/configs/anyscale_cluster_env.yaml
      small:
        compute_config:
          gcp: doc/source/templates/configs/compute/cpu/gcp_small.yaml
          aws: doc/source/templates/configs/compute/cpu/aws_small.yaml
      large:
        compute_config:
          # Relative to `path`
          gcp: doc/source/templates/configs/compute/cpu/gcp_large.yaml
          aws: doc/source/templates/configs/compute/cpu/aws_large.yaml
    ```

    When you specify the template's compute config, see `doc/source/templates/configs` for defaults.

4. Run a validation script on `templates.yaml` to make sure that the paths you specified are all valid.

    ```bash
    $ python doc/source/templates/validate.py
    Success!
    ``` -->
