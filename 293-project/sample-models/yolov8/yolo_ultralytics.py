from ultralytics import YOLO

model = YOLO("yolov8n.pt")  # Load a pre-trained YOLOv8 model
success = model.export(format="onnx")  # Export to ONNX format
