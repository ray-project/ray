# Face Mask Detection Pipeline

## Overview
In this tutorial, we’ll build an end-to-end face mask detection pipeline that leverages distributed fine-tuning, large-scale batch inference, video analytics, and scalable serving:

[1.object_detection_train.ipynb](1.object_detection_train.ipynb):
 Fine-tune a pre-trained Faster R-CNN model on a Kaggle face mask dataset (Pascal VOC format) using Ray Train. You’ll parse XML annotations with Ray Data, retrieve images from S3, run a distributed training loop, checkpoint the model, and visualize inference results.
<img
  src="https://face-masks-data.s3.us-east-2.amazonaws.com/tutorial-diagrams/train_object_detection.png"
  alt="Object Detection Training Pipeline"
  style="width:75%;"
/>

[2.object_detection_batch_inference_eval.ipynb](2.object_detection_batch_inference_eval.ipynb):
 Load your fine-tuned model from S3 into Anyscale cluster storage, perform GPU-accelerated batch inference on a test set with Ray Data, and calculate object detection metrics (mAP, IoU, recall) using TorchMetrics for comprehensive model evaluation.
<img
  src="https://face-masks-data.s3.us-east-2.amazonaws.com/tutorial-diagrams/batch_inference_metrics_calculation.png"
  alt="Metrics Calculation Pipeline"
  style="width:75%;"
/>

[3.video_processing_batch_inference.ipynb](3.video_processing_batch_inference.ipynb)
 Demonstrate a real-world video analytics workflow: read a video from S3, split it into frames, apply your detection model in parallel via Ray Data batch inference, draw bounding boxes/labels on each frame, and regenerate an annotated video for downstream consumption.
<img
  src="https://face-masks-data.s3.us-east-2.amazonaws.com/tutorial-diagrams/video_processing.png"
  alt="Video Processing Pipeline"
  style="width:75%;"
/>

[4.object_detection_serve.ipynb](4.object_detection_serve.ipynb)
 Deploy the trained Faster R-CNN mask detector as a production-ready microservice using Ray Serve and FastAPI. You’ll set up ingress, configure autoscaling and fractional GPU allocation, test the HTTP endpoint, and manage the service lifecycle both locally and via Anyscale Services.
