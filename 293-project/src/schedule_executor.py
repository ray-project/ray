import ray
from ray import serve
import torch
import time
import threading
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import deque
#from queue import Queue
from ray.util.queue import Queue as RayQueue
import os
from datetime import datetime
import torchvision.models as models

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


#os.environ['CUDA_VISIBLE_DEVICES'] = '0,1'


@dataclass
class session:
    """Session class representing a model deployment request"""
    model_name: str
    latency_SLO: float
    request_rate: float
    batch_size: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.model_name, str) or not self.model_name:
            raise ValueError("Model name must be a non-empty string")
        if not isinstance(self.latency_SLO, (int, float)) or self.latency_SLO <= 0:
            raise ValueError("Latency SLO must be a positive number")
        if not isinstance(self.request_rate, (int, float)) or self.request_rate < 0:
            raise ValueError("Request rate must be a non-negative number")
        if self.batch_size is not None and (not isinstance(self.batch_size, int) or self.batch_size <= 0):
            raise ValueError("Batch size must be a positive integer")

        self.batch_size = self.batch_size if self.batch_size is not None else 1
        self.creation_time = datetime.now()

@dataclass
class BatchRequest:
    """Represents a batch of requests for processing"""
    model_name: str
    inputs: List[torch.Tensor]
    batch_size: int
    request_ids: List[str]
    arrival_time: float

class RequestQueue:
    """Enhanced request queue with monitoring capabilities using Ray's Queue"""
    def __init__(self, model_name: str, max_size: int = 1000):
        self.model_name = model_name
        self.queue = RayQueue(maxsize=max_size)
        self._pending_count = 0
        self._total_requests = 0
        self._logger = logging.getLogger(f"Queue-{model_name}")
        
    def empty(self) -> bool:
        """Check if queue is empty"""
        return self.queue.qsize() == 0



    def add_request(self, request_id: str, input_tensor: torch.Tensor) -> bool:
        """Add request to queue with monitoring"""
        try:
            if self.queue.full():
                self._logger.warning(f"Queue full for {self.model_name}")
                return False
                
            self.queue.put((request_id, input_tensor, time.time()))
            self._pending_count += 1
            self._total_requests += 1
            return True
        except Exception as e:
            self._logger.error(f"Error adding request: {e}")
            return False
    
    def get_batch(self, batch_size: int) -> Optional[BatchRequest]:
        """Get batch of requests with timeout handling"""
        requests = []
        inputs = []
        request_ids = []
        earliest_arrival = float('inf')
        
        try:
            available = min(batch_size, self.queue.qsize())
            if available == 0:
                return None
                
            for _ in range(available):
                request_id, input_tensor, arrival_time = self.queue.get_nowait()
                requests.append((request_id, input_tensor))
                request_ids.append(request_id)
                inputs.append(input_tensor)
                earliest_arrival = min(earliest_arrival, arrival_time)
                self._pending_count -= 1
                    
            if inputs:
                return BatchRequest(
                    model_name=self.model_name,
                    inputs=inputs,
                    batch_size=len(inputs),
                    request_ids=request_ids,
                    arrival_time=earliest_arrival
                )

        except Empty:
            pass
        except Exception as e:
            self._logger.error(f"Error creating batch: {e}")
            
        return None
    
    def get_stats(self) -> Dict:
        """Get queue statistics"""
        return {
            'pending_requests': self._pending_count,
            'total_requests': self._total_requests,
            'queue_size': self.queue.qsize(),
            'queue_capacity': self.queue.maxsize
        }


@ray.remote(num_gpus=1)
class GPUWorker:
    """Enhanced Ray actor for GPU computation"""
    def __init__(self, node_id: str, gpu_id: int, sessions: List[Tuple], 
                 duty_cycle: float, model_registry: Dict):
        self.node_id = node_id
        self.gpu_id = 0
        self.duty_cycle = duty_cycle
        self.sessions = deque(sessions)
        self.models = {}
        self.logger = logging.getLogger(f"Worker-{node_id}")
        # Add diagnostic information
        print(f"Worker {node_id} initialization:")
        print(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES')}")
        print(f"Ray GPU IDs: {ray.get_gpu_ids()}")
        print(f"PyTorch GPU count: {torch.cuda.device_count()}")
        print(f"Requested GPU ID: {gpu_id}")
        
        for i in range(torch.cuda.device_count()):
            print(f"GPU {i}: {torch.cuda.get_device_name(i)}")

        print("GPU ID: "+str(self.gpu_id)+"--**--____----++++"+str(gpu_id))
        # Initialize models
        try:
            device = f'cuda:{self.gpu_id}'
            device = 'cuda:0'
            # First check if device is available
            if self.gpu_id >= torch.cuda.device_count():
                raise ValueError(f"GPU {self.gpu_id} not available. Only {torch.cuda.device_count()} GPUs found.")
                
            for session, _ in sessions:
                if session.model_name not in self.models:
                    self.logger.info(f"Loading {session.model_name} on GPU {gpu_id}")
                    model = model_registry[session.model_name]
                    # Move model to CPU first then to specific GPU
                    model = model.cpu()
                    model = model.to(device)
                    model.eval()
                    self.models[session.model_name] = model
                    
        except Exception as e:
            self.logger.error(f"Error initializing models: {e}")
            raise
        
        self.active = True
        self.stats = {
            'processed_batches': 0,
            'total_requests': 0,
            'processing_times': []
        }
    
    def stop(self):
        """Stop the worker gracefully"""
        self.logger.info(f"Stopping worker {self.node_id}")
        self.active = False

    def process_batch(self, batch: BatchRequest) -> Dict:
        """Process batch with enhanced monitoring"""
        try:
            model = self.models[batch.model_name]
            #inputs = torch.stack(batch.inputs).to(f'cuda:{self.gpu_id}')
            inputs = torch.stack(batch.inputs).cuda()  # Just use cuda() since only one GPU is visible

            #with torch.cuda.device(self.gpu_id):
            with torch.cuda.device(0):  # Always use device 0
                torch.cuda.synchronize()  # Ensure GPU is ready
                start_time = time.time()
                
                with torch.no_grad():  # Disable gradient computation
                    outputs = model(inputs)
                
                torch.cuda.synchronize()  # Wait for completion
                processing_time = (time.time() - start_time) * 1000  # ms
                
                self.stats['processed_batches'] += 1
                self.stats['total_requests'] += len(batch.request_ids)
                self.stats['processing_times'].append(processing_time)
                
                return {
                    'outputs': outputs.cpu(),
                    'request_ids': batch.request_ids,
                    'processing_time': processing_time,
                    'latency': time.time() - batch.arrival_time
                }
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
            raise
    

    def execute_schedule(self, request_queues: Dict[str, RequestQueue]):
        """Execute round-robin schedule with enhanced monitoring"""
        self.logger.info(f"Starting schedule execution on {self.node_id}")
    
        while self.active:
            try:
                # Get current session and its occupancy
                session, occupancy = self.sessions[0]
                self.sessions.rotate(-1)
            
                 # Calculate time slice
                time_slice = self.duty_cycle * occupancy
                start_time = time.time()
            
                # Get queue for current model
                queue = request_queues[session.model_name]
            
                # Try to get batch from queue
                batch = queue.get_batch(session.batch_size)
                if batch:
                    # Process batch and measure timing
                    result = self.process_batch(batch)
                    processing_time = result['processing_time']
                
                    # Log processing metrics
                    self.logger.info(
                        f"Processed batch of {batch.batch_size} requests for {session.model_name} "
                        f"in {processing_time:.2f}ms"
                    )
                
                    # Sleep for remaining time if any
                    remaining_time = time_slice - processing_time
                    if remaining_time > 0:
                        time.sleep(remaining_time / 1000)
                else:
                    # No requests, sleep for time slice
                    time.sleep(time_slice / 1000)
                
                # Log execution stats periodically
                if self.stats['processed_batches'] % 100 == 0:
                    self.logger.info(f"Node {self.node_id} stats: {self.get_stats()}")
                
            except Exception as e:
                self.logger.error(f"Error in schedule execution: {e}")
                time.sleep(0.1)
    
    def get_stats(self) -> Dict:
        """Get worker statistics"""
        return {
            'node_id': self.node_id,
            'gpu_id': self.gpu_id,
            'processed_batches': self.stats['processed_batches'],
            'total_requests': self.stats['total_requests'],
            'avg_processing_time': sum(self.stats['processing_times'][-100:]) / 
                                 len(self.stats['processing_times'][-100:])
                                 if self.stats['processing_times'] else 0
        }

class ScheduleExecutor:
    """Enhanced schedule executor with monitoring and management capabilities"""
    def __init__(self, node_schedules: List[Dict], model_registry: Dict,
                 max_queue_size: int = 1000):
        '''if not ray.is_initialized():
            num_gpus = torch.cuda.device_count()
            ray.init(num_gpus=num_gpus)'''
            
        self.logger = logging.getLogger("ScheduleExecutor")
        self.node_schedules = node_schedules
        self.model_registry = model_registry
        
        # Initialize request queues
        self.request_queues = {}
        self._init_queues(max_queue_size)
        
        # Initialize workers
        self.workers = []
        self._init_workers()
        
        # Statistics
        self.start_time = None
        self.stats = {
            'total_requests': 0,
            'failed_requests': 0,
            'queue_stats': {}
        }
    
    def _init_queues(self, max_queue_size: int):
        """Initialize request queues for all models"""
        for schedule in self.node_schedules:
            for session, _ in schedule['sessions']:
                if session.model_name not in self.request_queues:
                    self.request_queues[session.model_name] = RequestQueue(
                        model_name=session.model_name,
                        max_size=max_queue_size
                    )
    
    def _init_workers(self):
        """Initialize GPU workers"""
        available_gpus = torch.cuda.device_count()
        required_gpus = len(set(schedule['gpu_id'] for schedule in self.node_schedules))
        if required_gpus > available_gpus:
            raise RuntimeError(f"Schedule requires {required_gpus} GPUs but only {available_gpus} available")
        
        for schedule in self.node_schedules:
            try:
                worker = GPUWorker.remote(
                    node_id=schedule['node_id'],
                    gpu_id=schedule['gpu_id'],
                    sessions=schedule['sessions'],
                    duty_cycle=schedule['duty_cycle'],
                    model_registry=self.model_registry
                )
                self.workers.append(worker)
            except Exception as e:
                self.logger.error(f"Error initializing worker: {e}")
    
    def start(self):
        """Start schedule execution with monitoring"""
        self.start_time = time.time()
        self.logger.info("Starting schedule execution")
        
        # First validate GPU availability
        available_gpus = torch.cuda.device_count()
        required_gpus = len(set(schedule['gpu_id'] for schedule in self.node_schedules))
        if required_gpus > available_gpus:
            raise RuntimeError(f"Schedule requires {required_gpus} GPUs but only {available_gpus} available")
    
        try:
            # Create ray actors and start execution
            futures = []
            for worker in self.workers:
                futures.append(worker.execute_schedule.remote(self.request_queues))
        
            # Start monitoring thread
            self.monitoring_thread = threading.Thread(target=self._monitor_system)
            self.monitoring_thread.daemon = True
            self.monitoring_thread.start()
        
        except Exception as e:
            self.logger.error(f"Error starting execution: {e}")
            raise
        # Create Ray references to request queues
        '''ray_queues = {
            model: ray.put(queue) for model, queue in self.request_queues.items()
        }
    
        # Start workers with Ray references
        for worker in self.workers:
            ray.get(worker.execute_schedule.remote(ray_queues))
    
        # Start monitoring thread
        self.monitoring_thread = threading.Thread(target=self._monitor_system)
        self.monitoring_thread.daemon = True
        self.monitoring_thread.start()'''
    
    def stop(self):
        """Stop Execution gracefully"""
        self.logger.info("Stopping execution")
        try:
            # stop all workers
            ray.get([worker.stop.remote() for worker in self.workers])

            # wait for execution to complete
            ray.get(self.futures)

            # Clean up
            self.workers = []
            self.futures = []

        except Exception as e:
            self.logger.error(f"Error stopping execution: {e}")
            raise

    def submit_request(self, model_name: str, request_id: str, 
                      input_tensor: torch.Tensor) -> bool:
        """Submit request with error handling"""
        try:
            if model_name not in self.request_queues:
                self.logger.error(f"No queue found for model {model_name}")
                self.stats['failed_requests'] += 1
                return False
            
            success = self.request_queues[model_name].add_request(
                request_id, input_tensor
            )
            
            if success:
                self.stats['total_requests'] += 1
            else:
                self.stats['failed_requests'] += 1
            
            return success
        except Exception as e:
            self.logger.error(f"Error submitting request: {e}")
            self.stats['failed_requests'] += 1
            return False
    
    def _monitor_system(self):
        """Monitor system health and performance"""
        while True:
            try:
                # Collect queue statistics
                for model_name, queue in self.request_queues.items():
                    self.stats['queue_stats'][model_name] = queue.get_stats()
                
                # Collect worker statistics
                worker_stats = ray.get([
                    worker.get_stats.remote() for worker in self.workers
                ])
                
                # Log system status
                self.logger.info("System Status:")
                self.logger.info(f"Total requests: {self.stats['total_requests']}")
                self.logger.info(f"Failed requests: {self.stats['failed_requests']}")
                self.logger.info("Queue stats: " + str(self.stats['queue_stats']))
                self.logger.info("Worker stats: " + str(worker_stats))
                
                time.sleep(10)  # Status update interval
                
            except Exception as e:
                self.logger.error(f"Error in monitoring: {e}")
                time.sleep(1)
    
    def get_system_stats(self) -> Dict:
        """Get comprehensive system statistics"""
        return {
            'uptime': time.time() - self.start_time if self.start_time else 0,
            'total_requests': self.stats['total_requests'],
            'failed_requests': self.stats['failed_requests'],
            'queue_stats': self.stats['queue_stats'],
            'timestamp': datetime.now().isoformat()
        }
