#!/bin/bash
jupyter nbconvert --to script README.ipynb  # jupyter will convert even non-python code logic
ipython README.py  # be sure to use ipython to ensure even non-python cells are executed properly
rm README.py  # remove the generated script
