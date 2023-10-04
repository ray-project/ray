# Cache model files to a local directory

import os

from huggingface_hub import snapshot_download

from flags import cache_model_flags


def cache(args):
    os.makedirs(args.model_dir, exist_ok=True)

    snapshot_download(
        repo_id=args.model_name, revision=args.revision, cache_dir=args.model_dir
    )


if __name__ == "__main__":
    args = cache_model_flags().parse_args()
    cache(args)
