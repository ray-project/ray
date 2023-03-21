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
    All files needed to run the template should be contained within this directory.
    For example:

    ```text
    ray/
        doc/source/templates/
            <name-of-your-template>/
                requirements.txt
                <name-of-your-template>.ipynb
    ```

    If your template requires any special dependencies that are not included in a
    base `ray-ml` Docker image, be sure to specify a `requirements.txt` file within
    the directory.

2. Add an entry to `doc/source/templates/templates.yaml` that links to your template.

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

    Make sure that you include a small/large version for the template.
    See the following table for a description of template size:

    | Attributes | Small-scale Version | Large-scale Version |
    | -- | -- | -- |
    | Number of Nodes | 1 | > 1 |
    | Dataset size | Subset (of partitions/labels/rows) | Full example dataset |
    | Model size | Pruned/mini version of the model | Full model |
    | Runtime | 30-60s | Up to ~5-10 minutes |

    When you specify the template's compute config, see `doc/source/templates/configs` for defaults.

3. Add a nightly release test for the template in `release/release_tests.yaml`.

    ```yaml
    - name: workspace_template_small_02_many_model_training
      group: Workspace templates
      working_dir: workspace_templates/02_many_model_training
      python: "3.9"
      frequency: nightly
      team: ml
      cluster:
        cluster_env: ../configs/release_test_cluster_env.yaml
        cluster_compute: ../configs/compute/cpu/aws_small.yaml

    run:
      timeout: 300
      script: pip install -U -r requirements.txt
        && jupyter nbconvert --TagRemovePreprocessor.remove_input_tags='large'
        --to script --output _test many_model_training.ipynb && ipython _test.py
    ```

    Note: `--TagRemovePreprocessor.remove_input_tags='large'` will make sure that only the small-scale
    version of the template gets tested nightly.

4. Run a validation script on `templates.yaml` to make sure that the paths you specified are all valid.

    ```bash
    $ python doc/source/templates/validate.py
    Success!
    ```

5. Success! Your template is ready for review.
