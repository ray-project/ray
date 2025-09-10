.. _working_with_audio:

Working with Audio
==================

Ray Data provides comprehensive support for audio processing workloads, from simple audio transformations to complex speech recognition and audio analysis pipelines. This guide shows you how to efficiently work with audio datasets of any scale.

**What you'll learn:**

* Loading audio data from various sources and formats
* Performing audio transformations and preprocessing
* Building audio analysis and speech recognition pipelines
* Integrating with popular audio processing frameworks
* Optimizing audio processing for production workloads

Why Ray Data for Audio Processing
---------------------------------

Ray Data excels at audio processing workloads through several key advantages:

**Multimodal Data Excellence**
Native support for audio alongside other data types in unified workflows, enabling complex multimodal AI applications.

**GPU Acceleration**
Seamless GPU integration for audio transformations, model inference, and preprocessing operations.

**Scalable Performance**
Process audio datasets larger than memory with streaming execution and intelligent resource allocation.

**Production Ready**
Battle-tested at companies processing millions of audio files daily with enterprise-grade monitoring and error handling.

Loading Audio Data
------------------

Ray Data supports loading audio data from multiple sources and formats with automatic format detection and optimization.

**Audio File Formats**

Ray Data can read audio from various formats using different read functions:

.. code-block:: python

    import ray

    # Load audio files from local filesystem
    local_audio = ray.data.read_audio("data/audio/")

    # Load audio from cloud storage
    cloud_audio = ray.data.read_audio("s3://bucket/audio-dataset/")

    # Load with specific file patterns
    wav_files = ray.data.read_audio("data/audio/*.wav")
    mp3_files = ray.data.read_audio("data/audio/*.mp3")

    # Load with path information
    audio_with_paths = ray.data.read_audio(
        "s3://bucket/audio/",
        include_paths=True
    )

**Audio Decoding and Processing**

Decode and process audio files using popular audio libraries:

.. code-block:: python

    import librosa
    import soundfile as sf
    import numpy as np
    from typing import Dict, Any
    import ray

    def decode_audio(row: Dict[str, Any]) -> Dict[str, Any]:
        """Decode audio file and extract features."""
        
        # Read audio file
        audio_path = row["path"]
        
        try:
            # Load audio with librosa
            audio, sample_rate = librosa.load(audio_path, sr=None)
            
            # Extract basic features
            duration = librosa.get_duration(y=audio, sr=sample_rate)
            
            # Extract MFCC features
            mfccs = librosa.feature.mfcc(y=audio, sr=sample_rate, n_mfcc=13)
            
            # Extract spectral features
            spectral_centroids = librosa.feature.spectral_centroid(y=audio, sr=sample_rate)[0]
            spectral_rolloff = librosa.feature.spectral_rolloff(y=audio, sr=sample_rate)[0]
            
            # Extract rhythm features
            tempo, _ = librosa.beat.beat_track(y=audio, sr=sample_rate)
            
            return {
                "audio": audio,
                "sample_rate": sample_rate,
                "duration": duration,
                "mfccs": mfccs,
                "spectral_centroids": spectral_centroids,
                "spectral_rolloff": spectral_rolloff,
                "tempo": tempo,
                "path": audio_path
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "path": audio_path
            }

    # Load and decode audio files
    decoded_audio = (
        ray.data.read_audio("s3://bucket/audio/", include_paths=True)
        .map(decode_audio)
    )

**Batch Audio Processing**

Process multiple audio files efficiently in batches:

.. code-block:: python

    def batch_audio_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process multiple audio files in batch."""
        
        processed_audios = []
        features_list = []
        
        for audio_data in batch["audio"]:
            if "error" in audio_data:
                processed_audios.append(None)
                features_list.append(None)
                continue
            
            audio = audio_data["audio"]
            sample_rate = audio_data["sample_rate"]
            
            # Apply audio preprocessing
            # Normalize audio
            audio_normalized = librosa.util.normalize(audio)
            
            # Apply pre-emphasis filter
            audio_preemph = librosa.effects.preemphasis(audio_normalized)
            
            # Extract additional features
            features = {
                "zero_crossing_rate": librosa.feature.zero_crossing_rate(audio_preemph)[0],
                "chroma_stft": librosa.feature.chroma_stft(y=audio_preemph, sr=sample_rate),
                "mel_spectrogram": librosa.feature.melspectrogram(y=audio_preemph, sr=sample_rate),
                "spectral_contrast": librosa.feature.spectral_contrast(y=audio_preemph, sr=sample_rate)
            }
            
            processed_audios.append(audio_preemph)
            features_list.append(features)
        
        batch["processed_audio"] = processed_audios
        batch["extracted_features"] = features_list
        
        return batch

    # Apply batch processing
    processed_audio = decoded_audio.map_batches(batch_audio_processing)

Audio Transformations
--------------------

Transform audio data using Ray Data's powerful transformation capabilities with support for complex audio processing operations.

**Basic Audio Transformations**

.. code-block:: python

    import librosa
    import numpy as np
    from typing import Dict, Any
    import ray

    def basic_audio_transformations(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply basic audio transformations."""
        
        transformed_audios = []
        
        for audio in batch["audio"]:
            if audio is None:
                transformed_audios.append(None)
                continue
            
            # Apply various transformations
            # Time stretching
            audio_fast = librosa.effects.time_stretch(audio, rate=1.5)
            audio_slow = librosa.effects.time_stretch(audio, rate=0.75)
            
            # Pitch shifting
            audio_high = librosa.effects.pitch_shift(audio, sr=22050, n_steps=4)
            audio_low = librosa.effects.pitch_shift(audio, sr=22050, n_steps=-4)
            
            # Add noise
            noise = np.random.normal(0, 0.01, len(audio))
            audio_noisy = audio + noise
            
            transformed_audios.append({
                "original": audio,
                "fast": audio_fast,
                "slow": audio_slow,
                "high_pitch": audio_high,
                "low_pitch": audio_low,
                "noisy": audio_noisy
            })
        
        batch["transformed_audio"] = transformed_audios
        return batch

    # Apply basic transformations
    transformed_audio = processed_audio.map_batches(basic_audio_transformations)

**Advanced Audio Processing**

.. code-block:: python

    import librosa
    import numpy as np
    from scipy import signal
    from typing import Dict, Any
    import ray

    class AdvancedAudioProcessor:
        """Advanced audio processing with multiple techniques."""
        
        def __init__(self):
            self.sample_rate = 22050
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Apply advanced audio processing techniques."""
            
            processed_audios = []
            
            for audio_data in batch["audio"]:
                if audio_data is None:
                    processed_audios.append(None)
                    continue
                
                audio = audio_data["audio"]
                
                # Apply various advanced processing techniques
                
                # 1. Spectral subtraction for noise reduction
                stft = librosa.stft(audio)
                magnitude = np.abs(stft)
                phase = np.angle(stft)
                
                # Estimate noise from first few frames
                noise_spectrum = np.mean(magnitude[:, :10], axis=1, keepdims=True)
                
                # Apply spectral subtraction
                cleaned_magnitude = np.maximum(magnitude - 2 * noise_spectrum, 0.1 * magnitude)
                cleaned_stft = cleaned_magnitude * np.exp(1j * phase)
                audio_cleaned = librosa.istft(cleaned_stft)
                
                # 2. Harmonic-percussive separation
                audio_harmonic, audio_percussive = librosa.effects.hpss(audio)
                
                # 3. Beat synchronization
                tempo, beats = librosa.beat.beat_track(y=audio, sr=self.sample_rate)
                audio_sync = librosa.effects.remix(audio, intervals=beats)
                
                # 4. Spectral filtering
                # Design a bandpass filter
                nyquist = self.sample_rate / 2
                low = 100 / nyquist
                high = 8000 / nyquist
                b, a = signal.butter(4, [low, high], btype='band')
                audio_filtered = signal.filtfilt(b, a, audio)
                
                processed_audios.append({
                    "original": audio,
                    "cleaned": audio_cleaned,
                    "harmonic": audio_harmonic,
                    "percussive": audio_percussive,
                    "synchronized": audio_sync,
                    "filtered": audio_filtered
                })
            
            batch["advanced_processed"] = processed_audios
            return batch

    # Apply advanced processing
    advanced_processed = transformed_audio.map_batches(AdvancedAudioProcessor())

**GPU-Accelerated Audio Processing**

.. code-block:: python

    import torch
    import torchaudio
    import numpy as np
    from typing import Dict, Any
    import ray

    class GPUAudioProcessor:
        """GPU-accelerated audio processing with PyTorch."""
        
        def __init__(self):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Load pre-trained models if available
            # self.model = torch.hub.load('pytorch/audio', 'wav2vec2_base')
            # self.model.to(self.device)
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Process audio using GPU acceleration."""
            
            gpu_processed = []
            
            for audio_data in batch["audio"]:
                if audio_data is None:
                    gpu_processed.append(None)
                    continue
                
                audio = audio_data["audio"]
                
                # Convert to PyTorch tensor
                audio_tensor = torch.from_numpy(audio).float().to(self.device)
                
                # Apply GPU-accelerated transformations
                
                # 1. Mel spectrogram
                mel_spec = torchaudio.transforms.MelSpectrogram(
                    sample_rate=22050,
                    n_fft=2048,
                    hop_length=512,
                    n_mels=128
                ).to(self.device)
                
                mel_spectrogram = mel_spec(audio_tensor.unsqueeze(0))
                
                # 2. MFCC
                mfcc_transform = torchaudio.transforms.MFCC(
                    sample_rate=22050,
                    n_mfcc=13,
                    melkwargs={'n_fft': 2048, 'hop_length': 512, 'n_mels': 128}
                ).to(self.device)
                
                mfcc = mfcc_transform(audio_tensor.unsqueeze(0))
                
                # 3. Spectrogram
                spec_transform = torchaudio.transforms.Spectrogram(
                    n_fft=2048,
                    hop_length=512
                ).to(self.device)
                
                spectrogram = spec_transform(audio_tensor.unsqueeze(0))
                
                # Convert back to CPU for storage
                gpu_processed.append({
                    "mel_spectrogram": mel_spectrogram.cpu().numpy(),
                    "mfcc": mfcc.cpu().numpy(),
                    "spectrogram": spectrogram.cpu().numpy(),
                    "original": audio
                })
            
            batch["gpu_processed"] = gpu_processed
            return batch

    # Apply GPU-accelerated processing
    gpu_processed = advanced_processed.map_batches(
        GPUAudioProcessor,
        compute=ray.data.ActorPoolStrategy(size=4),
        num_gpus=1
    )

Audio Analysis Pipelines
------------------------

Build end-to-end audio analysis pipelines with Ray Data for various applications.

**Speech Recognition Pipeline**

.. code-block:: python

    import speech_recognition as sr
    from typing import Dict, Any
    import ray

    class SpeechRecognizer:
        """Speech recognition using various engines."""
        
        def __init__(self):
            self.recognizer = sr.Recognizer()
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Perform speech recognition on audio files."""
            
            recognition_results = []
            
            for audio_data in batch["audio"]:
                if audio_data is None:
                    recognition_results.append(None)
                    continue
                
                audio = audio_data["audio"]
                
                try:
                    # Convert numpy array to audio file format
                    # This is a simplified example - in practice, you'd save to temp file
                    # or use appropriate audio format
                    
                    # For demonstration, we'll simulate recognition
                    # In real implementation, you'd use:
                    # audio_source = sr.AudioData(audio.tobytes(), sample_rate, 2)
                    # text = self.recognizer.recognize_google(audio_source)
                    
                    # Simulated recognition result
                    text = f"Recognized text from audio of length {len(audio)}"
                    confidence = 0.85
                    
                    recognition_results.append({
                        "text": text,
                        "confidence": confidence,
                        "audio_length": len(audio)
                    })
                    
                except Exception as e:
                    recognition_results.append({
                        "error": str(e),
                        "audio_length": len(audio)
                    })
            
            batch["speech_recognition"] = recognition_results
            return batch

    # Build speech recognition pipeline
    speech_pipeline = (
        gpu_processed
        .map_batches(SpeechRecognizer)
    )

**Audio Classification Pipeline**

.. code-block:: python

    import torch
    import torchaudio
    import torch.nn as nn
    from typing import Dict, Any
    import ray

    class AudioClassifier:
        """Audio classification with pre-trained models."""
        
        def __init__(self):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # Simple audio classification model
            self.model = nn.Sequential(
                nn.Conv1d(1, 64, kernel_size=3, padding=1),
                nn.ReLU(),
                nn.MaxPool1d(2),
                nn.Conv1d(64, 128, kernel_size=3, padding=1),
                nn.ReLU(),
                nn.MaxPool1d(2),
                nn.AdaptiveAvgPool1d(1),
                nn.Flatten(),
                nn.Linear(128, 10)  # 10 classes
            ).to(self.device)
            
            self.model.eval()
            
            # Define class labels
            self.labels = [
                "music", "speech", "noise", "silence", "music_with_speech",
                "ambient", "traffic", "nature", "mechanical", "other"
            ]
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Classify audio samples."""
            
            classifications = []
            
            for audio_data in batch["audio"]:
                if audio_data is None:
                    classifications.append(None)
                    continue
                
                audio = audio_data["audio"]
                
                try:
                    # Preprocess audio for classification
                    # Resize to fixed length
                    target_length = 22050  # 1 second at 22.05kHz
                    if len(audio) > target_length:
                        audio = audio[:target_length]
                    else:
                        audio = np.pad(audio, (0, target_length - len(audio)))
                    
                    # Convert to tensor
                    audio_tensor = torch.from_numpy(audio).float().unsqueeze(0).to(self.device)
                    
                    # Run classification
                    with torch.no_grad():
                        outputs = self.model(audio_tensor)
                        probabilities = torch.nn.functional.softmax(outputs, dim=1)
                        
                        # Get top prediction
                        top_prob, top_class = torch.topk(probabilities, 1)
                        
                        classification = {
                            "predicted_class": self.labels[top_class.item()],
                            "confidence": top_prob.item(),
                            "all_probabilities": probabilities.cpu().numpy().flatten().tolist()
                        }
                    
                    classifications.append(classification)
                    
                except Exception as e:
                    classifications.append({
                        "error": str(e)
                    })
            
            batch["audio_classification"] = classifications
            return batch

    # Build audio classification pipeline
    classification_pipeline = (
        gpu_processed
        .map_batches(
            AudioClassifier,
            compute=ray.data.ActorPoolStrategy(size=2),
            num_gpus=1
        )
    )

**Music Analysis Pipeline**

.. code-block:: python

    import librosa
    import numpy as np
    from typing import Dict, Any
    import ray

    class MusicAnalyzer:
        """Music analysis and feature extraction."""
        
        def __init__(self):
            pass
        
        def __call__(self, batch: Dict[str, Any]) -> Dict[str, Any]:
            """Analyze music characteristics."""
            
            music_analysis = []
            
            for audio_data in batch["audio"]:
                if audio_data is None:
                    music_analysis.append(None)
                    continue
                
                audio = audio_data["audio"]
                
                try:
                    # Extract music-specific features
                    
                    # 1. Tempo and beat analysis
                    tempo, beats = librosa.beat.beat_track(y=audio, sr=22050)
                    beat_frames = librosa.frames_to_time(beats, sr=22050)
                    
                    # 2. Key and mode detection
                    chroma = librosa.feature.chroma_cqt(y=audio, sr=22050)
                    key = np.argmax(np.mean(chroma, axis=1))
                    key_names = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
                    detected_key = key_names[key]
                    
                    # 3. Harmonic analysis
                    harmonic, percussive = librosa.effects.hpss(audio)
                    harmonic_ratio = np.sum(harmonic**2) / np.sum(audio**2)
                    
                    # 4. Spectral features
                    spectral_centroids = librosa.feature.spectral_centroid(y=audio, sr=22050)[0]
                    spectral_rolloff = librosa.feature.spectral_rolloff(y=audio, sr=22050)[0]
                    
                    # 5. Rhythm features
                    onset_strength = librosa.onset.onset_strength(y=audio, sr=22050)
                    onset_frames = librosa.onset.onset_detect(onset_strength=onset_strength, sr=22050)
                    
                    analysis = {
                        "tempo": tempo,
                        "beat_count": len(beats),
                        "beat_times": beat_frames.tolist(),
                        "key": detected_key,
                        "harmonic_ratio": harmonic_ratio,
                        "spectral_centroid_mean": np.mean(spectral_centroids),
                        "spectral_rolloff_mean": np.mean(spectral_rolloff),
                        "onset_count": len(onset_frames)
                    }
                    
                    music_analysis.append(analysis)
                    
                except Exception as e:
                    music_analysis.append({
                        "error": str(e)
                    })
            
            batch["music_analysis"] = music_analysis
            return batch

    # Build music analysis pipeline
    music_pipeline = (
        gpu_processed
        .map_batches(MusicAnalyzer)
    )

Performance Optimization
------------------------

Optimize audio processing pipelines for maximum performance and efficiency.

**Batch Size Optimization**

.. code-block:: python

    from ray.data.context import DataContext
    import ray

    # Configure optimal batch sizes for audio processing
    ctx = DataContext.get_current()
    
    # For audio processing, moderate batch sizes work well
    ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
    
    # Optimize batch sizes based on audio characteristics
    def optimize_audio_batch_size(audio_data):
        """Determine optimal batch size for audio processing."""
        
        # Analyze audio characteristics
        sample_batch = audio_data.take_batch(batch_size=100)
        avg_audio_length = sum(len(a.get("audio", [])) for a in sample_batch["audio"] if a) / len(sample_batch["audio"])
        
        # Calculate optimal batch size
        # Audio is typically 16-bit, so 2 bytes per sample
        target_batch_size = int(128 * 1024 * 1024 / (avg_audio_length * 2))
        
        # Ensure reasonable bounds
        target_batch_size = max(1, min(target_batch_size, 64))
        
        return target_batch_size

    # Apply optimized batch processing
    optimal_batch_size = optimize_audio_batch_size(gpu_processed)
    optimized_pipeline = gpu_processed.map_batches(
        process_audio,
        batch_size=optimal_batch_size
    )

**Memory Management**

.. code-block:: python

    def memory_efficient_audio_processing(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Process audio with memory efficiency."""
        
        # Process in smaller chunks to manage memory
        chunk_size = 32
        results = []
        
        for i in range(0, len(batch["audio"]), chunk_size):
            chunk = batch["audio"][i:i+chunk_size]
            
            # Process chunk
            processed_chunk = process_audio_chunk(chunk)
            results.extend(processed_chunk)
            
            # Explicitly clear chunk from memory
            del chunk
        
        batch["processed_audio"] = results
        return batch

    # Use memory-efficient processing
    memory_optimized = gpu_processed.map_batches(memory_efficient_audio_processing)

**GPU Resource Management**

.. code-block:: python

    # Configure GPU strategy for optimal utilization
    gpu_strategy = ray.data.ActorPoolStrategy(
        size=4,  # Number of GPU workers
        max_tasks_in_flight_per_actor=2  # Pipeline depth per worker
    )

    # Apply GPU-optimized processing
    gpu_optimized = gpu_processed.map_batches(
        GPUAudioProcessor,
        compute=gpu_strategy,
        num_gpus=1,
        batch_size=16  # Optimize for GPU memory
    )

Saving and Exporting Audio
---------------------------

Save processed audio data in various formats for different use cases.

**Audio File Formats**

.. code-block:: python

    import soundfile as sf
    import librosa
    from typing import Dict, Any
    import ray

    def save_audio_files(batch: Dict[str, Any]) -> Dict[str, Any]:
        """Save processed audio in various formats."""
        
        for i, audio_data in enumerate(batch["audio"]):
            if audio_data is None:
                continue
            
            # Save in different formats
            audio = audio_data["audio"]
            
            # Save as WAV
            sf.write(f"output/audio_{i}.wav", audio, 22050)
            
            # Save as MP3 (requires additional libraries)
            # librosa.output.write_wav(f"output/audio_{i}.mp3", audio, 22050)
            
            # Save as FLAC
            sf.write(f"output/audio_{i}.flac", audio, 22050)
        
        return batch

    # Save audio files
    saved_audio = gpu_processed.map_batches(save_audio_files)

**Structured Formats**

.. code-block:: python

    # Save as Parquet with metadata
    processed_audio.write_parquet(
        "s3://output/audio-dataset/",
        compression="snappy"
    )

    # Save as JSON Lines
    processed_audio.write_json(
        "s3://output/audio-metadata.jsonl"
    )

    # Save as NumPy arrays
    processed_audio.write_numpy(
        "s3://output/audio-features/",
        column="extracted_features"
    )

Integration with ML Frameworks
------------------------------

Integrate Ray Data audio processing with popular machine learning frameworks.

**PyTorch Integration**

.. code-block:: python

    import torch
    from torch.utils.data import DataLoader
    import ray

    # Convert Ray Dataset to PyTorch format
    torch_dataset = processed_audio.to_torch(
        label_column="label",
        feature_columns=["audio", "extracted_features"],
        batch_size=32
    )

    # Use with PyTorch training
    model = YourPyTorchAudioModel()
    optimizer = torch.optim.Adam(model.parameters())
    
    for batch in torch_dataset:
        audio = batch["audio"]
        features = batch["extracted_features"]
        labels = batch["label"]
        
        # Training step
        optimizer.zero_grad()
        outputs = model(audio, features)
        loss = torch.nn.functional.cross_entropy(outputs, labels)
        loss.backward()
        optimizer.step()

**TensorFlow Integration**

.. code-block:: python

    import tensorflow as tf
    import ray

    # Convert Ray Dataset to TensorFlow format
    tf_dataset = processed_audio.to_tf(
        label_column="label",
        feature_columns=["audio", "extracted_features"],
        batch_size=32
    )

    # Use with TensorFlow training
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(None,)),
        tf.keras.layers.LSTM(128, return_sequences=True),
        tf.keras.layers.LSTM(64),
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

    from transformers import AutoFeatureExtractor, AutoModelForAudioClassification
    import torch
    import ray

    # Load Hugging Face audio model
    feature_extractor = AutoFeatureExtractor.from_pretrained("facebook/wav2vec2-base")
    model = AutoModelForAudioClassification.from_pretrained("facebook/wav2vec2-base")

    def huggingface_audio_processing(batch):
        """Process audio with Hugging Face models."""
        
        # Extract features using Hugging Face
        inputs = feature_extractor(
            batch["audio"],
            sampling_rate=22050,
            return_tensors="pt"
        )
        
        # Run inference
        with torch.no_grad():
            outputs = model(**inputs)
            predictions = outputs.logits.argmax(-1)
        
        batch["huggingface_predictions"] = predictions.numpy()
        return batch

    # Apply Hugging Face processing
    hf_processed = processed_audio.map_batches(huggingface_audio_processing)

Production Deployment
---------------------

Deploy audio processing pipelines to production with monitoring and optimization.

**Production Pipeline Configuration**

.. code-block:: python

    def production_audio_pipeline():
        """Production-ready audio processing pipeline."""
        
        # Configure for production
        ctx = DataContext.get_current()
        ctx.target_max_block_size = 256 * 1024 * 1024  # 256MB blocks
        ctx.enable_auto_log_stats = True
        ctx.verbose_stats_logs = True
        
        # Load audio data
        audio_data = ray.data.read_audio("s3://input/audio/", include_paths=True)
        
        # Apply processing
        processed = audio_data.map_batches(
            production_audio_processor,
            compute=ray.data.ActorPoolStrategy(size=8),
            num_gpus=2,
            batch_size=32
        )
        
        # Save results
        processed.write_parquet("s3://output/processed-audio/")
        
        return processed

**Monitoring and Observability**

.. code-block:: python

    # Enable comprehensive monitoring
    ctx = DataContext.get_current()
    ctx.enable_per_node_metrics = True
    ctx.memory_usage_poll_interval_s = 1.0

    # Monitor pipeline performance
    def monitor_pipeline_performance(dataset):
        """Monitor audio processing pipeline performance."""
        
        stats = dataset.stats()
        print(f"Processing time: {stats.total_time}")
        print(f"Memory usage: {stats.memory_usage}")
        print(f"CPU usage: {stats.cpu_usage}")
        
        return dataset

    # Apply monitoring
    monitored_pipeline = audio_data.map_batches(
        process_audio
    ).map_batches(monitor_pipeline_performance)

Best Practices
--------------

**1. Audio Format Selection**

* **WAV**: Best for high quality, uncompressed audio
* **MP3**: Good for compressed audio, smaller file sizes
* **FLAC**: Lossless compression, good quality/size balance
* **OGG**: Open format, good compression

**2. Batch Size Optimization**

* Start with batch size 16-32 for GPU processing
* Adjust based on audio length and GPU memory
* Monitor memory usage and adjust accordingly

**3. Memory Management**

* Use streaming execution for large datasets
* Process audio in chunks to manage memory
* Clear intermediate results when possible

**4. GPU Utilization**

* Use `ActorPoolStrategy` for GPU workloads
* Configure appropriate concurrency levels
* Monitor GPU utilization and memory

**5. Error Handling**

* Implement robust error handling for corrupted audio
* Use `max_errored_blocks` to handle failures gracefully
* Log and monitor processing errors

Next Steps
----------

Now that you understand audio processing with Ray Data, explore related topics:

* **Working with AI**: AI and machine learning workflows → :ref:`working-with-ai`
* **Working with PyTorch**: Deep PyTorch integration → :ref:`working-with-pytorch`
* **Working with Images**: Computer vision workflows → :ref:`working-with-images`
* **Performance Optimization**: Optimize audio processing performance → :ref:`performance-optimization`

For practical examples:

* **Audio Analysis Examples**: Real-world audio processing applications → :ref:`audio-analysis-examples`
* **Speech Recognition Examples**: Speech processing and recognition → :ref:`speech-recognition-examples`
* **Music Analysis Examples**: Music processing and analysis → :ref:`music-analysis-examples`
