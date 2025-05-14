#!/bin/bash
python ci/nb2py.py README.ipynb README.py  # jupyter will convert even non-python code logic
python README.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm README.py  # remove the generated script
