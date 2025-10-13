#!/bin/bash
python ci/nb2py.py README.ipynb README.py  # convert notebook to py script
python README.py  # run the converted python script
rm README.py  # remove the generated script