#!/bin/bash
jupyter nbconvert --to script e2e_audio/curation.ipynb  # jupyter will convert even non-python code logic
ipython e2e_audio/curation.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm e2e_audio/curation.py  # remove the generated script
