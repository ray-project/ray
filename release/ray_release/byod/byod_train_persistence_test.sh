#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the train_multinode_persistence test.

set -exo pipefail

pip3 install -U torch fsspec s3fs gcsfs pyarrow>=17.0.0 pytest
