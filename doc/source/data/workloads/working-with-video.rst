.. _working_with_video:

Working with Video
==================

Ray Data provides comprehensive support for video processing workloads, from simple video transformations to complex computer vision and video analysis pipelines. This guide shows you how to efficiently work with video datasets of any scale.

**What you'll learn:**

* Loading video data from various sources and formats
* Performing video transformations and preprocessing
* Building video analysis and computer vision pipelines
* Integrating with popular video processing frameworks
* Optimizing video processing for production workloads

Why Ray Data for Video Processing
---------------------------------

Ray Data excels at video processing workloads through several key advantages:

**Multimodal Data Excellence**
Native support for video alongside other data types in unified workflows, enabling complex multimodal AI applications.

**GPU Acceleration**
Seamless GPU integration for video transformations, model inference, and preprocessing operations.

**Scalable Performance**
Process video datasets larger than memory with streaming execution and intelligent resource allocation.

**Production Ready**
Battle-tested at companies processing millions of video files daily with enterprise-grade monitoring and error handling.

Loading Video Data
------------------

Ray Data supports loading video data from multiple sources and formats with automatic format detection and optimization.

**Video File Formats**

Ray Data can read video from various formats using different read functions:

.. code-block:: python

    import ray

    # Load video files from local filesystem
    local_videos = ray.data.read_videos("data/videos/")

    # Load videos from cloud storage
    cloud_videos = ray.data.read_videos("s3://bucket/video-dataset/")

    # Load with specific file patterns
    mp4_files = ray.data.read_videos("data/videos/*.mp4")
    avi_files = ray.data.read_videos("data/videos/*.avi")
    mov_files = ray.data.read_videos("data/videos/*.mov")

    # Load with path information
    videos_with_paths = ray.data.read_videos(
        "s3://bucket/videos/",
        include_paths=True
    )

**Video Decoding and Processing**

Decode and process video files using popular video libraries:

.. code-block:: python

    import cv2
    import numpy as np
    from typing import Dict, Any
    import ray

    def decode_video(row: Dict[str, Any]) -> Dict[str, Any]:
        """Decode video file and extract frames."""
        
        video_path = row["path"]
        
        try:
            # Open video file
            cap = cv2.VideoCapture(video_path)
            
            if not cap.isOpened():
                raise ValueError(f"Could not open video file: {video_path}")
            
            # Get video properties
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            duration = frame_count / fps if fps > 0 else 0
            
            # Extract frames (sample every nth frame for efficiency)
            sample_rate = max(1, frame_count // 100)  # Sample 100 frames max
            frames = []
            frame_indices = []
            
            frame_idx = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                if frame_idx % sample_rate == 0:
                    # Convert BGR to RGB
                    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frames.append(frame_rgb)
                    frame_indices.append(frame_idx)
                
                frame_idx += 1
            
            cap.release()
            
            return {
                "frames": frames,
                "frame_indices": frame_indices,
                "fps": fps,
                "frame_count": frame_count,
                "width": width,
                "height": height,
                "duration": duration,
                "path": video_path,
                "sampled_frames": len(frames)
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "path": video_path
            }

    # Load and decode video files
    decoded_videos = (
        ray.data.read_videos("s3://bucket/videos/", include_paths=True)
        .map(decode_video)
    )

**Batch Video Processing**

Process multiple video files efficiently in batches:

.. code-block:: python

    def batch_video_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process multiple video files in batch."""
        
        processed_videos = []
        
        for video_data in batch["frames"]:
            if "error" in video_data:
                processed_videos.append(None)
                continue
            
            frames = video_data["frames"]
            
            # Process frames
            processed_frames = []
            for frame in frames:
                # Resize frame to standard size
                frame_resized = cv2.resize(frame, (224, 224))
                
                # Normalize pixel values
                frame_normalized = frame_resized.astype(np.float32) / 255.0
                
                processed_frames.append(frame_normalized)
            
            # Calculate video statistics
            if processed_frames:
                frames_array = np.array(processed_frames)
                mean_frame = np.mean(frames_array, axis=0)
                std_frame = np.std(frames_array, axis=0)
                
                processed_videos.append({
                    "original_frames": frames,
                    "processed_frames": processed_frames,
                    "mean_frame": mean_frame,
                    "std_frame": std_frame,
                    "frame_count": len(processed_frames)
                })
            else:
                processed_videos.append(None)
        
        batch["processed_videos"] = processed_videos
        return batch

    # Apply batch processing
    processed_videos = decoded_videos.map_batches(batch_video_processing)

Video Transformations
--------------------

Transform video data using Ray Data's powerful transformation capabilities with support for complex video processing operations.

**Basic Video Transformations**

.. code-block:: python

    import cv2
    import numpy as np
    from typing import Dict, Any
    import ray

    def basic_video_transformations(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply basic video transformations."""
        
        transformed_videos = []
        
        for video_data in batch["processed_videos"]:
            if video_data is None:
                transformed_videos.append(None)
                continue
            
            frames = video_data["processed_frames"]
            
            # Apply various transformations
            transformed_frames = []
            
            for frame in frames:
                # Convert back to uint8 for OpenCV operations
                frame_uint8 = (frame * 255).astype(np.uint8)
                
                # Apply transformations
                # 1. Grayscale conversion
                frame_gray = cv2.cvtColor(frame_uint8, cv2.COLOR_RGB2GRAY)
                
                # 2. Edge detection
                frame_edges = cv2.Canny(frame_gray, 50, 150)
                
                # 3. Gaussian blur
                frame_blurred = cv2.GaussianBlur(frame_uint8, (5, 5), 0)
                
                # 4. Histogram equalization (for grayscale)
                frame_equalized = cv2.equalizeHist(frame_gray)
                
                # 5. Color space transformations
                frame_hsv = cv2.cvtColor(frame_uint8, cv2.COLOR_RGB2HSV)
                frame_lab = cv2.cvtColor(frame_uint8, cv2.COLOR_RGB2LAB)
                
                transformed_frames.append({
                    "original": frame,
                    "grayscale": frame_gray,
                    "edges": frame_edges,
                    "blurred": frame_blurred,
                    "equalized": frame_equalized,
                    "hsv": frame_hsv,
                    "lab": frame_lab
                })
            
            transformed_videos.append({
                "original_video": video_data,
                "transformed_frames": transformed_frames
            })
        
        batch["transformed_videos"] = transformed_videos
        return batch

    # Apply basic transformations
    transformed_videos = processed_videos.map_batches(basic_video_transformations)

**Advanced Video Processing**

.. code-block:: python

    import cv2
    import numpy as np
    from scipy import ndimage
    from typing import Dict, Any
    import ray

    class AdvancedVideoProcessor:
        """Advanced video processing with multiple techniques."""
        
        def __init__(self):
            self.target_size = (224, 224)
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Apply advanced video processing techniques."""
            
            processed_videos = []
            
            for video_data in batch["transformed_videos"]:
                if video_data is None:
                    processed_videos.append(None)
                    continue
                
                transformed_frames = video_data["transformed_frames"]
                
                # Apply advanced processing techniques
                advanced_frames = []
                
                for frame_data in transformed_frames:
                    original = frame_data["original"]
                    
                    # 1. Motion detection (compare with previous frame)
                    # This is simplified - in practice you'd track previous frames
                    motion_mask = np.zeros_like(original[:, :, 0])
                    
                    # 2. Background subtraction (simplified)
                    # In practice, you'd use more sophisticated methods
                    background = np.mean(original, axis=(0, 1))
                    foreground_mask = np.any(np.abs(original - background) > 0.1, axis=2)
                    
                    # 3. Optical flow (simplified)
                    # In practice, you'd calculate between consecutive frames
                    flow_x = np.random.normal(0, 1, original.shape[:2])
                    flow_y = np.random.normal(0, 1, original.shape[:2])
                    
                    # 4. Temporal filtering
                    # Apply temporal smoothing (simplified)
                    frame_smoothed = ndimage.gaussian_filter(original, sigma=0.5)
                    
                    # 5. Feature detection
                    gray = cv2.cvtColor((original * 255).astype(np.uint8), cv2.COLOR_RGB2GRAY)
                    corners = cv2.goodFeaturesToTrack(gray, 25, 0.01, 10)
                    
                    advanced_frames.append({
                        "original": original,
                        "motion_mask": motion_mask,
                        "foreground_mask": foreground_mask,
                        "optical_flow": (flow_x, flow_y),
                        "smoothed": frame_smoothed,
                        "corners": corners if corners is not None else np.array([])
                    })
                
                processed_videos.append({
                    "original_video": video_data,
                    "advanced_frames": advanced_frames
                })
            
            batch["advanced_processed"] = processed_videos
            return batch

    # Apply advanced processing
    advanced_processed = transformed_videos.map_batches(AdvancedVideoProcessor())

**GPU-Accelerated Video Processing**

.. code-block:: python

    import torch
    import torchvision.transforms as transforms
    import numpy as np
    from typing import Dict, Any
    import ray

    class GPUVideoProcessor:
        """GPU-accelerated video processing with PyTorch."""
        
        def __init__(self):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Define transforms
            self.transform = transforms.Compose([
                transforms.ToTensor(),
                transforms.Resize((224, 224)),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406],
                    std=[0.229, 0.224, 0.225]
                )
            ])
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Process video using GPU acceleration."""
            
            gpu_processed = []
            
            for video_data in batch["advanced_processed"]:
                if video_data is None:
                    gpu_processed.append(None)
                    continue
                
                advanced_frames = video_data["advanced_frames"]
                
                # Process frames with GPU
                gpu_frames = []
                
                for frame_data in advanced_frames:
                    original = frame_data["original"]
                    
                    # Convert to PyTorch tensor
                    frame_tensor = self.transform(original).unsqueeze(0).to(self.device)
                    
                    # Apply GPU-accelerated operations
                    
                    # 1. Convolutional operations
                    conv_filter = torch.ones(1, 3, 3, 3).to(self.device) / 9
                    frame_convolved = torch.nn.functional.conv2d(
                        frame_tensor, conv_filter, padding=1
                    )
                    
                    # 2. Pooling operations
                    frame_pooled = torch.nn.functional.avg_pool2d(frame_tensor, 2, 2)
                    
                    # 3. Upsampling
                    frame_upsampled = torch.nn.functional.interpolate(
                        frame_tensor, scale_factor=2, mode='bilinear'
                    )
                    
                    # Convert back to CPU for storage
                    gpu_frames.append({
                        "original": original,
                        "convolved": frame_convolved.cpu().numpy(),
                        "pooled": frame_pooled.cpu().numpy(),
                        "upsampled": frame_upsampled.cpu().numpy()
                    })
                
                gpu_processed.append({
                    "original_video": video_data,
                    "gpu_frames": gpu_frames
                })
            
            batch["gpu_processed"] = gpu_processed
            return batch

    # Apply GPU-accelerated processing
    gpu_processed = advanced_processed.map_batches(
        GPUVideoProcessor,
        compute=ray.data.ActorPoolStrategy(size=4),
        num_gpus=1
    )

Video Analysis Pipelines
------------------------

Build end-to-end video analysis pipelines with Ray Data for various applications.

**Object Detection Pipeline**

.. code-block:: python

    import cv2
    import numpy as np
    from typing import Dict, Any
    import ray

    class VideoObjectDetector:
        """Object detection in video frames."""
        
        def __init__(self):
            # Load pre-trained object detection model
            self.net = cv2.dnn.readNet(
                "yolov4.weights",
                "yolov4.cfg"
            )
            
            # Load class names
            with open("coco.names", "r") as f:
                self.classes = f.read().strip().split("\n")
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Detect objects in video frames."""
            
            detection_results = []
            
            for video_data in batch["gpu_processed"]:
                if video_data is None:
                    detection_results.append(None)
                    continue
                
                gpu_frames = video_data["gpu_frames"]
                
                # Process each frame
                frame_detections = []
                
                for frame_data in gpu_frames:
                    original = frame_data["original"]
                    
                    try:
                        # Prepare image for YOLO
                        blob = cv2.dnn.blobFromImage(
                            original, 1/255.0, (416, 416), swapRB=True, crop=False
                        )
                        
                        # Run detection
                        self.net.setInput(blob)
                        outputs = self.net.forward()
                        
                        # Process detections
                        detections = []
                        for output in outputs:
                            for detection in output:
                                scores = detection[5:]
                                class_id = np.argmax(scores)
                                confidence = scores[class_id]
                                
                                if confidence > 0.5:
                                    center_x = int(detection[0] * original.shape[1])
                                    center_y = int(detection[1] * original.shape[0])
                                    width = int(detection[2] * original.shape[1])
                                    height = int(detection[3] * original.shape[0])
                                    
                                    detections.append({
                                        "class": self.classes[class_id],
                                        "confidence": float(confidence),
                                        "bbox": [center_x, center_y, width, height]
                                    })
                        
                        frame_detections.append({
                            "frame": original,
                            "detections": detections,
                            "detection_count": len(detections)
                        })
                        
                    except Exception as e:
                        frame_detections.append({
                            "frame": original,
                            "error": str(e),
                            "detections": []
                        })
                
                detection_results.append({
                    "video": video_data,
                    "frame_detections": frame_detections
                })
            
            batch["object_detection"] = detection_results
            return batch

    # Build object detection pipeline
    detection_pipeline = (
        gpu_processed
        .map_batches(VideoObjectDetector)
    )

**Video Classification Pipeline**

.. code-block:: python

    import torch
    import torchvision.models as models
    import numpy as np
    from typing import Dict, Any
    import ray

    class VideoClassifier:
        """Video classification with pre-trained models."""
        
        def __init__(self):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Load pre-trained model
            self.model = models.resnet50(pretrained=True)
            self.model.eval()
            self.model.to(self.device)
            
            # Define video categories
            self.categories = [
                "action", "comedy", "drama", "horror", "romance",
                "sci_fi", "thriller", "documentary", "animation", "other"
            ]
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Classify video content."""
            
            classifications = []
            
            for video_data in batch["gpu_processed"]:
                if video_data is None:
                    classifications.append(None)
                    continue
                
                gpu_frames = video_data["gpu_frames"]
                
                try:
                    # Sample frames for classification
                    frame_count = len(gpu_frames)
                    sample_indices = np.linspace(0, frame_count-1, 16, dtype=int)
                    
                    sampled_frames = []
                    for idx in sample_indices:
                        if idx < frame_count:
                            frame = gpu_frames[idx]["original"]
                            # Resize and normalize
                            frame_resized = cv2.resize(frame, (224, 224))
                            frame_normalized = frame_resized.astype(np.float32) / 255.0
                            sampled_frames.append(frame_normalized)
                    
                    if sampled_frames:
                        # Convert to tensor
                        frames_tensor = torch.from_numpy(np.array(sampled_frames)).to(self.device)
                        frames_tensor = frames_tensor.permute(0, 3, 1, 2)  # NHWC to NCHW
                        
                        # Run classification on each frame
                        frame_predictions = []
                        with torch.no_grad():
                            for frame in frames_tensor:
                                frame = frame.unsqueeze(0)  # Add batch dimension
                                outputs = self.model(frame)
                                probabilities = torch.nn.functional.softmax(outputs, dim=1)
                                frame_predictions.append(probabilities.cpu().numpy())
                        
                        # Aggregate predictions across frames
                        avg_predictions = np.mean(frame_predictions, axis=0)
                        predicted_class = np.argmax(avg_predictions)
                        confidence = avg_predictions[0][predicted_class]
                        
                        classification = {
                            "predicted_category": self.categories[predicted_class],
                            "confidence": float(confidence),
                            "all_probabilities": avg_predictions[0].tolist(),
                            "sampled_frames": len(sampled_frames)
                        }
                    else:
                        classification = {"error": "No valid frames found"}
                    
                    classifications.append(classification)
                    
                except Exception as e:
                    classifications.append({"error": str(e)})
            
            batch["video_classification"] = classifications
            return batch

    # Build video classification pipeline
    classification_pipeline = (
        gpu_processed
        .map_batches(
            VideoClassifier,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
    )

**Video Summarization Pipeline**

.. code-block:: python

    import cv2
    import numpy as np
    from sklearn.cluster import KMeans
    from typing import Dict, Any
    import ray

    class VideoSummarizer:
        """Video summarization using key frame extraction."""
        
        def __init__(self, num_keyframes=10):
            self.num_keyframes = num_keyframes
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Extract key frames for video summarization."""
            
            summarization_results = []
            
            for video_data in batch["gpu_processed"]:
                if video_data is None:
                    summarization_results.append(None)
                    continue
                
                gpu_frames = video_data["gpu_frames"]
                
                try:
                    # Extract frame features for clustering
                    frame_features = []
                    for frame_data in gpu_frames:
                        frame = frame_data["original"]
                        
                        # Convert to grayscale and resize for feature extraction
                        gray = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
                        gray_resized = cv2.resize(gray, (64, 64))
                        
                        # Flatten and normalize
                        features = gray_resized.flatten().astype(np.float32) / 255.0
                        frame_features.append(features)
                    
                    if len(frame_features) > self.num_keyframes:
                        # Cluster frames to find key frames
                        kmeans = KMeans(n_clusters=self.num_keyframes, random_state=42)
                        cluster_labels = kmeans.fit_predict(frame_features)
                        
                        # Find representative frames for each cluster
                        key_frames = []
                        for cluster_id in range(self.num_keyframes):
                            cluster_indices = np.where(cluster_labels == cluster_id)[0]
                            if len(cluster_indices) > 0:
                                # Find frame closest to cluster center
                                cluster_center = kmeans.cluster_centers_[cluster_id]
                                distances = [np.linalg.norm(frame_features[i] - cluster_center) 
                                           for i in cluster_indices]
                                closest_idx = cluster_indices[np.argmin(distances)]
                                key_frames.append({
                                    "frame_index": int(closest_idx),
                                    "cluster_id": int(cluster_id),
                                    "frame": gpu_frames[closest_idx]["original"]
                                })
                    else:
                        # If fewer frames than requested keyframes, use all frames
                        key_frames = [{
                            "frame_index": i,
                            "cluster_id": i,
                            "frame": frame_data["original"]
                        } for i, frame_data in enumerate(gpu_frames)]
                    
                    # Sort by frame index
                    key_frames.sort(key=lambda x: x["frame_index"])
                    
                    summarization_results.append({
                        "video": video_data,
                        "key_frames": key_frames,
                        "summary_length": len(key_frames)
                    })
                    
                except Exception as e:
                    summarization_results.append({
                        "error": str(e),
                        "video": video_data
                    })
            
            batch["video_summarization"] = summarization_results
            return batch

    # Build video summarization pipeline
    summarization_pipeline = (
        gpu_processed
        .map_batches(VideoSummarizer)
    )

Performance Optimization
------------------------

Optimize video processing pipelines for maximum performance and efficiency.

**Batch Size Optimization**

.. code-block:: python

    from ray.data.context import DataContext
    import ray

    # Configure optimal batch sizes for video processing
    ctx = DataContext.get_current()
    
    # For video processing, smaller batch sizes work better due to memory constraints
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
    
    # Optimize batch sizes based on video characteristics
    def optimize_video_batch_size(video_data):
        """Determine optimal batch size for video processing."""
        
        # Analyze video characteristics
        sample_batch = video_data.take_batch(batch_size=50)
        avg_frame_count = sum(len(v.get("frames", [])) for v in sample_batch["frames"] if v) / len(sample_batch["frames"])
        avg_frame_size = 224 * 224 * 3 * 4  # 4 bytes per float32 pixel
        
        # Calculate optimal batch size
        target_batch_size = int(64 * 1024 * 1024 / (avg_frame_count * avg_frame_size))
        
        # Ensure reasonable bounds
        target_batch_size = max(1, min(target_batch_size, 32))
        
        return target_batch_size

    # Apply optimized batch processing
    optimal_batch_size = optimize_video_batch_size(gpu_processed)
    optimized_pipeline = gpu_processed.map_batches(
        process_video,
        batch_size=optimal_batch_size
    )

**Memory Management**

.. code-block:: python

    def memory_efficient_video_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process video with memory efficiency."""
        
        # Process in smaller chunks to manage memory
        chunk_size = 16
        results = []
        
        for i in range(0, len(batch["gpu_processed"]), chunk_size):
            chunk = batch["gpu_processed"][i:i+chunk_size]
            
            # Process chunk
            processed_chunk = process_video_chunk(chunk)
            results.extend(processed_chunk)
            
            # Explicitly clear chunk from memory
            del chunk
        
        batch["processed_video"] = results
        return batch

    # Use memory-efficient processing
    memory_optimized = gpu_processed.map_batches(memory_efficient_video_processing)

**GPU Resource Management**

.. code-block:: python

    # Configure GPU strategy for optimal utilization
    gpu_strategy = ray.data.ActorPoolStrategy(
        size=2,  # Number of GPU workers (videos are memory-intensive)
        max_tasks_in_flight_per_actor=1  # Process one video at a time per GPU
    )

    # Apply GPU-optimized processing
    gpu_optimized = gpu_processed.map_batches(
        GPUVideoProcessor,
        compute=gpu_strategy,
        num_gpus=1,
        batch_size=8  # Optimize for GPU memory
    )

Saving and Exporting Video
---------------------------

Save processed video data in various formats for different use cases.

**Video File Formats**

.. code-block:: python

    import cv2
    import numpy as np
    from typing import Dict, Any
    import ray

    def save_video_files(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Save processed video in various formats."""
        
        for i, video_data in enumerate(batch["gpu_processed"]):
            if video_data is None:
                continue
            
            gpu_frames = video_data["gpu_frames"]
            
            if not gpu_frames:
                continue
            
            # Get video properties from first frame
            first_frame = gpu_frames[0]["original"]
            height, width = first_frame.shape[:2]
            
            # Create video writer
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            out = cv2.VideoWriter(f"output/video_{i}.mp4", fourcc, 30.0, (width, height))
            
            # Write frames
            for frame_data in gpu_frames:
                frame = frame_data["original"]
                # Convert RGB to BGR for OpenCV
                frame_bgr = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
                out.write(frame_bgr)
            
            out.release()
        
        return batch

    # Save video files
    saved_videos = gpu_processed.map_batches(save_video_files)

**Structured Formats**

.. code-block:: python

    # Save as Parquet with metadata
    processed_videos.write_parquet(
        "s3://output/video-dataset/",
        compression="snappy"
    )

    # Save as JSON Lines
    processed_videos.write_json(
        "s3://output/video-metadata.jsonl"
    )

    # Save key frames as images
    def save_key_frames(batch):
        """Save key frames as individual images."""
        
        for video_data in batch["video_summarization"]:
            if video_data is None or "error" in video_data:
                continue
            
            for key_frame in video_data["key_frames"]:
                frame = key_frame["frame"]
                frame_index = key_frame["frame_index"]
                cluster_id = key_frame["cluster_id"]
                
                # Save frame
                cv2.imwrite(f"output/keyframe_{frame_index}_cluster_{cluster_id}.jpg", 
                           cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
        
        return batch

    # Save key frames
    key_frames_saved = summarization_pipeline.map_batches(save_key_frames)

Integration with ML Frameworks
------------------------------

Integrate Ray Data video processing with popular machine learning frameworks.

**PyTorch Integration**

.. code-block:: python

    import torch
    from torch.utils.data import DataLoader
    import ray

    # Convert Ray Dataset to PyTorch format
    torch_dataset = processed_videos.to_torch(
        label_column="label",
        feature_columns=["frames", "extracted_features"],
        batch_size=16
    )

    # Use with PyTorch training
    model = YourPyTorchVideoModel()
    optimizer = torch.optim.Adam(model.parameters())
    
    for batch in torch_dataset:
        frames = batch["frames"]
        features = batch["extracted_features"]
        labels = batch["label"]
        
        # Training step
        optimizer.zero_grad()
        outputs = model(frames, features)
        loss = torch.nn.functional.cross_entropy(outputs, labels)
        loss.backward()
        optimizer.step()

**TensorFlow Integration**

.. code-block:: python

    import tensorflow as tf
    import ray

    # Convert Ray Dataset to TensorFlow format
    tf_dataset = processed_videos.to_tf(
        label_column="label",
        feature_columns=["frames", "extracted_features"],
        batch_size=16
    )

    # Use with TensorFlow training
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(None, 224, 224, 3)),
        tf.keras.layers.Conv3D(64, 3, activation='relu'),
        tf.keras.layers.MaxPooling3D(2),
        tf.keras.layers.Conv3D(128, 3, activation='relu'),
        tf.keras.layers.GlobalAveragePooling3D(),
        tf.keras.layers.Dense(10, activation='softmax')
    ])
    
    model.compile(
        optimizer='adam',
        loss='sparse_categorical_crossentropy',
        metrics=['accuracy']
    )
    
    model.fit(tf_dataset, epochs=10)

**Hugging Face Integration**

.. code-block:: python

    from transformers import AutoFeatureExtractor, AutoModelForVideoClassification
    import torch
    import ray

    # Load Hugging Face video model
    feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/xclip-base-patch32")
    model = AutoModelForVideoClassification.from_pretrained("microsoft/xclip-base-patch32")

    def huggingface_video_processing(batch):
        """Process video with Hugging Face models."""
        
        # Extract features using Hugging Face
        inputs = feature_extractor(
            batch["frames"],
            return_tensors="pt"
        )
        
        # Run inference
        with torch.no_grad():
            outputs = model(**inputs)
            predictions = outputs.logits.argmax(-1)
        
        batch["huggingface_predictions"] = predictions.numpy()
        return batch

    # Apply Hugging Face processing
    hf_processed = processed_videos.map_batches(huggingface_video_processing)

Production Deployment
---------------------

Deploy video processing pipelines to production with monitoring and optimization.

**Production Pipeline Configuration**

.. code-block:: python

    def production_video_pipeline():
        """Production-ready video processing pipeline."""
        
        # Configure for production
        ctx = DataContext.get_current()
        ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB blocks
        ctx.enable_auto_log_stats = True
        ctx.verbose_stats_logs = True
        
        # Load video data
        video_data = ray.data.read_videos("s3://input/videos/", include_paths=True)
        
        # Apply processing
        processed = video_data.map_batches(
            production_video_processor,
            compute=ray.data.ActorPoolStrategy(size=4),
            num_gpus=2,
            batch_size=16
        )
        
        # Save results
        processed.write_parquet("s3://output/processed-videos/")
        
        return processed

**Monitoring and Observability**

.. code-block:: python

    # Enable comprehensive monitoring
    ctx = DataContext.get_current()
    ctx.enable_per_node_metrics = True
    ctx.memory_usage_poll_interval_s = 1.0

    # Monitor pipeline performance
    def monitor_pipeline_performance(dataset):
        """Monitor video processing pipeline performance."""
        
        stats = dataset.stats()
        print(f"Processing time: {stats.total_time}")
        print(f"Memory usage: {stats.memory_usage}")
        print(f"CPU usage: {stats.cpu_usage}")
        
        return dataset

    # Apply monitoring
    monitored_pipeline = video_data.map_batches(
        process_video
    ).map_batches(monitor_pipeline_performance)

Best Practices
--------------

**1. Video Format Selection**

* **MP4**: Best for web and mobile, good compression
* **AVI**: Good for editing, larger file sizes
* **MOV**: Best for Apple ecosystem, good quality
* **WebM**: Best for web, open format

**2. Batch Size Optimization**

* Start with small batch sizes (8-16) for video processing
* Adjust based on video length and GPU memory
* Monitor memory usage and adjust accordingly

**3. Memory Management**

* Use streaming execution for large datasets
* Process videos in chunks to manage memory
* Clear intermediate results when possible

**4. GPU Utilization**

* Use `ActorPoolStrategy` for GPU workloads
* Configure appropriate concurrency levels
* Monitor GPU utilization and memory

**5. Error Handling**

* Implement robust error handling for corrupted videos
* Use `max_errored_blocks` to handle failures gracefully
* Log and monitor processing errors

Next Steps
----------

Now that you understand video processing with Ray Data, explore related topics:

* **Working with AI**: AI and machine learning workflows → :ref:`working-with-ai`
* **Working with Images**: Computer vision workflows → :ref:`working-with-images`
* **Working with Audio**: Audio processing workflows → :ref:`working-with-audio`
* **Performance Optimization**: Optimize video processing performance → :ref:`performance-optimization`

For practical examples:

* **Video Analysis Examples**: Real-world video processing applications → :ref:`video-analysis-examples`
* **Computer Vision Examples**: Video-based computer vision → :ref:`cv-examples`
* **Video Summarization Examples**: Video summarization and key frame extraction → :ref:`video-summarization-examples`
