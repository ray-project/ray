"""Upload the model files to cloud storage (s3 or gcs).

Example usage:
```bash
python -m ray.llm.utils.upload_model \
    --model-source facebook/opt-350m \
    --bucket-uri gs://my-bucket/path/to/facebook-opt-350m
```
"""

import typer

from ray.llm._internal.common.utils.upload_utils import (
    upload_model_cli as _upload_model_cli,
)


def main():
    typer.run(_upload_model_cli)


if __name__ == "__main__":
    main()
