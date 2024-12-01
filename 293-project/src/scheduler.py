import ray
import time
import copy
import torch
import itertools
import traceback
import logging
import os, csv, json
from datetime import datetime, timedelta
from ray.util.queue import Queue as RayQueue
from threading import Lock, Thread
from queue import Queue, Empty
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

from nexus import (
    session,
    node,
    nexus
)

models_config = {
    'vit': {'SLO': 50, 'base_rate':1000},        # (model_name, SLO, initial_rate)
    'resnet': {'SLO': 50, 'base_rate': 2000},
    'shufflenet': {'SLO': 30, 'base_rate': 1500},
    'efficientnet': {'SLO': 40, 'base_rate': 1200}
}

import torchvision.models as models
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
model_registry = {
    'vit': models.vit_b_16(pretrained=True),  # Using direct torchvision import
    'shuffle': models.shufflenet_v2_x1_0(pretrained=True),
    'resnet': models.resnet50(pretrained=True)
}

class TestResultLogger:
    """Handles test result storage and logging"""
    def __init__(self, base_dir: str = "test_results"):
        self.base_dir = Path(base_dir)
        self.test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.test_dir = self.base_dir / self.test_timestamp
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
    def log_metrics(self, test_name: str, metrics: dict):
        """Log metrics to JSON file"""
        file_path = self.test_dir / f"{test_name}_metrics.json"
        with open(file_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)

    def log_changes(self, test_name: str, changes: list):
        """Log schedule changes to CSV"""
        file_path = self.test_dir / f"{test_name}_changes.csv"
        if changes:
            keys = changes[0].keys()
            with open(file_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(changes)

    def log_node_state(self, test_name: str, nodes: dict, timestamp: str):
        """Log node state to text file"""
        file_path = self.test_dir / f"{test_name}_nodes.txt"
        with open(file_path, 'a') as f:
            f.write(f"\nNode State at {timestamp}\n")
            f.write("=" * 50 + "\n")
            for model_name, model_nodes in nodes.items():
                f.write(f"\nModel: {model_name}\n")
                for i, n in enumerate(model_nodes):
                    f.write(f"\nNode {i+1}:\n")
                    f.write(f"Duty Cycle: {n.duty_cycle}ms\n")
                    f.write(f"Occupancy: {n.get_occupancy()*100:.2f}%\n")
                    for s, occ in n.node_sessions:
                        f.write(f"Session: {s.model_name}, "
                              f"Rate: {s.request_rate:.2f}, "
                              f"Batch: {s.batch_size}\n")
            f.write("\n" + "=" * 50 + "\n")

class BatchProfiler:
    """
    Handles batch profiling data loading and management.
    
    Loads and manages model batch profiling data from CSV files containing
    performance metrics like latency and memory usage at different batch sizes.
    """
    @staticmethod
    def load_csv_to_dict(file_path: str) -> Dict[int, Dict[str, float]]:
        """Load CSV data into dictionary"""
        column_list = ['avg_latency_ms', 'peak_memory_mb']
        result = {}
        try:
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                headers = reader.fieldnames
                key_column = headers[0] if headers else None
                for row in reader:
                    key = int(row[key_column])
                    result[key] = {header: float(row[header]) 
                                 for header in headers[1:] 
                                 if header in column_list}
        except (FileNotFoundError, ValueError) as e:
            logging.error(f"Error loading batch profile from {file_path}: {e}")
            return {}
        return result
    
class RequestTracker:
    """
    Tracks and calculates request rates for models over a specified time window.
    
    The tracker maintains a count of requests within a sliding window and calculates
    the current request rate. Thread-safe implementation using locks.
    
    Attributes:
        window_size (float): Size of the sliding window in seconds (default: 60.0)
        request_count (int): Number of requests in current window
        last_reset (datetime): Timestamp of last window reset
        _total_requests (int): Total number of requests processed since initialization
    """
     
    def __init__(self, window_size: float = 60.0):
        """
        Initialize RequestTracker with specified window size.
        
        Args:
            window_size (float): Duration of sliding window in seconds
        """
        self.window_size = window_size
        self.request_count = 0
        self.last_reset = datetime.now()
        self.lock = Lock()
        self._total_requests = 0  # Track total requests for metrics

    def record_request(self) -> None:
        """
        Record a single request occurrence.
        Thread-safe increment of request counters.
        """
        with self.lock:
            self.request_count += 1
            self._total_requests += 1

    def get_request_rate(self) -> float:
        """
        Calculate current request rate within the sliding window.
        
        Returns:
            float: Requests per second over the current window
        """
        with self.lock:
            current_time = datetime.now()
            duration = (current_time - self.last_reset).total_seconds()
            
            if duration >= self.window_size:
                rate = self.request_count / duration
                self.request_count = 0
                self.last_reset = current_time
                return rate
            elif duration > 0:
                return self.request_count / duration
            return 0.0

    def get_total_requests(self) -> int:
        """
        Get total number of requests processed since initialization.
        
        Returns:
            int: Total request count
        """
        with self.lock:
            return self._total_requests
    
class BatchRequest:
    """Represents a batch of requests for processing"""
    model_name: str
    inputs: List[torch.Tensor]
    batch_size: int
    request_ids: List[str]
    arrival_time: float

class RequestQueue:
    """Request queue with monitoring capabilities using Ray's Queue"""
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
                
            batch = self.queue.get_batch(available, timeout=0)
            for (request_id, input_tensor, arrival_time) in batch:
                requests.append((request_id, input_tensor))
                request_ids.append(request_id)
                inputs.append(input_tensor)
                earliest_arrival = min(earliest_arrival, arrival_time)
                self._pending_count -= 1

            # for _ in range(available):
            #     request_id, input_tensor, arrival_time = self.queue.get_nowait()
            #     requests.append((request_id, input_tensor))
            #     request_ids.append(request_id)
            #     inputs.append(input_tensor)
            #     earliest_arrival = min(earliest_arrival, arrival_time)
            #     self._pending_count -= 1
                    
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
    """ Ray actor for GPU computation"""
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

class NexusScheduler:
    """
        This class implements the squishy bin packing algorithm described
        in section 6.1 of nexus paper

        batching profile dictionary needs to be structured as follows:
        key = batch size: { key = column name in csv(latency, memory usage etc): value}
    """
    def __init__(self, batching_profile: Dict[str, Dict[int, Dict[str, float]]], 
                 monitoring_interval: float = 5.0,
                 rate_change_threshold: float = 0.05):
        self.batching_profile = batching_profile
        self.nexus_instance   = nexus(batching_profile)
        self.sessions: Dict[str, session] = {}
        self.nodes: List[node] = []
        # self.nodes: Dict[str, List[node]] = {}
        self.request_trackers: Dict[str, RequestTracker] = {}
        
        self.monitoring_interval = monitoring_interval
        self.rate_change_threshold = rate_change_threshold
        self.lock = Lock()
        self.schedule_changes = Queue()
        
        self.monitoring_thread: Optional[Thread] = None
        self._stop_monitoring = False
        self.logger = logging.getLogger("NexusScheduler")

        # TO DO !!!!!
        # add instances of GPU worker nodes

        # Add metrics tracking
        self.metrics: Dict[str, Dict] = {
            'schedule_updates': 0,
            'total_requests': {},
            'rate_changes': {},
            'node_changes': {}
        }
    
    def start_monitoring(self) -> None:
        """Start the monitoring thread"""
        if self.monitoring_thread is not None:
            return
        
        self._stop_monitoring = False
        self.monitoring_thread = Thread(target=self._monitor_request_rates, daemon=True)
        self.monitoring_thread.start()
        self.logger.info("Request rate monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop the monitoring thread"""
        self._stop_monitoring = True
        if self.monitoring_thread:
            self.monitoring_thread.join()
            self.monitoring_thread = None
        self.logger.info("Request rate monitoring stopped")

    def record_request(self, model_name: str) -> None:
        """Record an incoming request for a model"""
        if model_name not in self.request_trackers:
            with self.lock:
                if model_name not in self.request_trackers:
                    self.request_trackers[model_name] = RequestTracker()
                    self.metrics['total_requests'][model_name] = 0
        self.request_trackers[model_name].record_request()
        self.metrics['total_requests'][model_name] += 1
    
    def _monitor_request_rates(self) -> None:
        """Background monitoring loop"""
        while not self._stop_monitoring:
            try:
                self._check_and_update_schedules()
                time.sleep(self.monitoring_interval)
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    def _check_and_update_schedules(self):
        """Check request rates and update schedules if needed"""
        requires_update = False
        update_info     = {}
        
        with self.lock:
            for model_name, tracker in self.request_trackers.items():
                current_rate = tracker.get_request_rate()
                
                if model_name not in self.sessions:
                    requires_update = True
                    update_info[model_name] = current_rate
                    continue

                previous_rate = self.sessions[model_name].request_rate
                rate_diff = abs(current_rate - previous_rate)

                # Check if rate change exceeds threshold
                if (rate_diff / previous_rate) > self.rate_change_threshold:
                    self.logger.info(f"Rate change detected for {model_name}: {current_rate:.2f} req/s")
                    
                    requires_update = True
                    update_info[model_name] = current_rate 
                    # self._update_schedule(model_name, current_rate)
                    
                    # Update metrics
                    if model_name not in self.metrics['rate_changes']:
                        self.metrics['rate_changes'][model_name] = []
                    self.metrics['rate_changes'][model_name].append({
                        'timestamp': datetime.now(),
                        'old_rate': previous_rate,
                        'new_rate': current_rate
                    }) 

        if requires_update:
            self._update_schedule(update_info)

    def get_transfers(old_node: node, new_node: node):
        transfers = 0
        new_node_models = [s.model_name for s, _ in new_node.node_sessions]
        old_node_models = [s.model_name for s, _ in old_node.node_sessions]
        
        for model in new_node_models:
            if model not in old_node_models:
                transfers += 1
        
        return transfers

    def _update_schedule(self, update_info: dict):
        new_sessions = []
        # update request rates of all old sessions
        for model, old_session in self.sessions.items():
            new_session = copy.deepcopy(old_session)
            if model in update_info:
                new_session.request_rate = update_info[model]
            new_sessions.append(new_session)
        
        # also add sessions that have been created for new models
        for model, request_rate in update_info.items():
            if model not in self.sessions:
                new_session = session(model, models_config[model]['SLO'], request_rate)
                new_sessions.append(new_session)

        old_nodes = self.nodes
        new_nodes = self.nexus_instance.squishyBinPacking(new_sessions)

        # Find a way to update from old arrangement to new arrangement such that
        # the number of model transfers across GPUs is minimized
        l = len(old_nodes)
        n = len(new_nodes)
        final_nodes = []
        if l <= n:
            numbers = range(1, n + 1)
            arrangments = list(itertools.permutations(numbers, l))

            best_arrangment = None
            max_transfers   = None
            for arrangement in arrangments:
                current_transfers = self.get_transfers(old_nodes, [new_nodes[i-1] for i in arrangement])
                if max_transfers is None or max_transfers > current_transfers:
                    best_arrangment = arrangement
                    max_transfers   = current_transfers
            
            final_nodes = [new_nodes[i-1] for i in best_arrangment]
            for i in range(1, n+1):
                if i not in best_arrangment:
                    final_nodes.append(new_nodes[i-1])
        else:
            numbers = range(1, n + 1)
            arrangments = list(itertools.permutations(numbers, l))

            best_arrangment = None
            max_transfers   = None
            for arrangement in arrangments:
                current_transfers = self.get_transfers(old_nodes[:n], [new_nodes[i-1] for i in arrangement])
                if max_transfers is None or max_transfers > current_transfers:
                    best_arrangment = arrangement
                    max_transfers   = current_transfers

            final_nodes = [new_nodes[i-1] for i in best_arrangment]

        self._update_workers(final_nodes)

        for n in final_nodes:
            n.print_node_pretty()

    def _update_workers(self, new_nodes: List[node]):
        # get workers and set new nodes for them
        # they should load and unload models at the end of duty cycle
        # and then carry on executing the new node
        pass
        

def main():
    """
    Main function for running the dynamic scheduling system.
    
    Configuration:
    - Sets up logging and test results directory
    - Loads batch profiles for models (vit, resnet, shufflenet, efficientnet)
    - Initializes scheduler with monitoring interval=5.0s, rate threshold=0.05
    - Configures initial model deployments with SLOs and request rates
    
    Operations:
    - Performs initial scheduling using squishyBinPacking
    - Starts continuous monitoring of request rates 
    - Simulates varying workload patterns for each model
    - Logs metrics, node states and schedule changes every 10s
    
    Monitoring continues until keyboard interrupt (Ctrl+C), then performs
    cleanup and saves final metrics.
    
    Directory structure:
    profiling_dir: Contains model batch profiles (.csv files)
    logger.test_dir: Stores test results and metrics
    """

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Set up result logger
    logger = TestResultLogger()
    logging.info(f"Test results will be stored in: {logger.test_dir}")

    # Load batch profiles
    profiling_dir = "../profiling"

    model_files = {
        'vit': 'vit_g16_20241123_154354_summary.csv',
        'resnet': 'resnet50_20241117_154052_summary.csv',
        'shufflenet': 'shufflenet_20241123_104115_summary.csv',
        'efficientnet': 'efficientnetv2_20241123_125206_summary.csv'
    }

        # Initialize batch profiler and load profiles
    profiler = BatchProfiler()
    batching_profile = {}
    
    for model_name, filename in model_files.items():
        file_path = os.path.join(profiling_dir, filename)
        profile = profiler.load_csv_to_dict(file_path)
        if profile:
            batching_profile[model_name] = profile
            logging.info(f"Loaded profile for {model_name}")
        else:
            logging.error(f"Failed to load profile for {model_name}")
            return
        
    # Create scheduler instance
    scheduler = NexusScheduler(
        batching_profile=batching_profile,
        monitoring_interval=5.0,
        rate_change_threshold=0.05
    )

    # Register initial models


