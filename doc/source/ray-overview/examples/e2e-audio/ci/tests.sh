#!/bin/bash
jupyter nbconvert --to script e2e_audio/curation.ipynb  # Jupyter will convert bash code if they start with !
ipython e2e_audio/curation.py  # Use ipython to ensure non-python cells are executed properly
rm e2e_audio/curation.py  # Clean up
