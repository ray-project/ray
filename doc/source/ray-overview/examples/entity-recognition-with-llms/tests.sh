#!/bin/bash
jupyter nbconvert --to script README.ipynb
ipython README.py
rm README.py
