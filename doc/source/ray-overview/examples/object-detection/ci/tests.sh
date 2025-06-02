#!/bin/bash
pip install jupytext
python ci/test_myst_doc.py --path 1.object_detection_train.ipynb
python ci/test_myst_doc.py --path 2.object_detection_batch_inference_eval.ipynb
python ci/test_myst_doc.py --path 3.video_processing_batch_inference.ipynb
python ci/test_myst_doc.py --path 4.object_detection_serve.ipynb