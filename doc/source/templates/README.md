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

1. Add your template as a directory somewhere in `doc/source/templates`.

    For example:

    ```text
    ray/
        doc/source/templates/
            <name-of-your-template>/
                README.md
                <name-of-your-template>.ipynb
                requirements.txt  (Optional)
            templates.yaml
    ```

    Your template does not need to be a Jupyter notebook. It can also be presented as a
    Python script with `README` instructions of how to run.

2. Add a release test for the template in `release/release_tests.yaml` (for both AWS and GCE).

    See the section on workspace templates for an example. Note that the cluster env and
    compute config are a little different for release tests. Use the files in the
    `doc/source/templates/testing/release` folder.

    The release test compute configs contain placeholders for regions and cloud ids that our CI infra will fill in.
    The cluster env builds a nightly docker image with all the required dependencies.

3. Add an entry to `doc/source/templates/templates.yaml` that links to your template.

    See the top of the `templates.yaml` file for something to copy-paste and fill in your own values.

    When you specify the template's compute config, see `doc/source/templates/configs` for shared configs. You can also create custom compute configs (of the same format as these shared ones).

    For handling dependencies:

    - If your template requires any special dependencies that are not included in a
      base image that you chose, be sure to list and provide instructions to install
      the necessary dependencies within the notebook.
      See `02_many_model_training` for an example.

    - If your template requires a custom docker image, be sure to mention this in the
      `README` and link the docker image URL somewhere. See `03_serving_stable_diffusion` for an example.

4. Run a validation script on `templates.yaml` to make sure that the paths you specified are all valid and all yamls are properly formatted.

    **Note:** This will also run in CI, but you can check quickly by running the validation script.

    ```bash
    $ python doc/source/templates/testing/validate.py
    Success!
    ```

5. Success! Your template is ready for review.

<!-- 2. Add another copy of the template that includes test-specific code and a smoke-test version if applicable.

    **Note:** The need for a second test copy is temporary. Only one notebook will be needed
    from 2.5 onward, since the test-specific code will be filtered out.

    **Label all test-specific code with the `remove-cell` Jupyter notebook tag.**

    **Put this test copy in `doc/source/templates/tests/<name-of-your-template>.ipynb`.**

3. List the smoke-test version of the template in `doc/BUILD` under the templates section. This will configure the smoke-test version to run in pre-merge CI.

    Set the `SMOKE_TEST` environment variable, which should be used in your template to
    **to make the template work for a single CI instance.**
    This environment variable can also be used to conditionally set certain smoke test parameters (like limiting dataset size).

    **Make sure that you tag the test with `"gpu"` if required, and any other tags
    needed for special dependencies.**

    ```python
    py_test_run_all_notebooks(
        size = "large",
        include = ["source/templates/tests/batch_inference.ipynb"],
        exclude = [],
        data = ["//doc:workspace_templates"],
        tags = ["exclusive", "team:ml", "ray_air", "gpu"],
        env = {"SMOKE_TEST": "1"},
    )
    ``` -->