from huggingface_hub import snapshot_download
import os
import sys

local_dir = sys.argv[1]

os.makedirs(local_dir, exist_ok=True)

snapshot_download(
    "diffusers/dog-example",
    local_dir=local_dir,
    repo_type="dataset",
    ignore_patterns=".gitattributes",
)
