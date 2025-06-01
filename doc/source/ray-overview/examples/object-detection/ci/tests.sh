#!/bin/bash
pip install jupytext
python ci/test_myst_doc.py 1.object_detection_train.ipynb
python ci/test_myst_doc.py 2.object_detection_batch_inference_eval.ipynb
python ci/test_myst_doc.py 3.video_processing_batch_inference.ipynb
python ci/test_myst_doc.py 4.object_detection_serve.ipynb