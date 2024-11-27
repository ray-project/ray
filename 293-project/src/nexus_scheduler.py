"""
Continuous NexusScheduler Implementation
Handles dynamic request rate monitoring and schedule updates
"""

from __future__ import annotations
from bisect import bisect
from bisect import bisect_left
from collections import namedtuple
from operator import attrgetter
import math
import copy
import csv, os, json
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from threading import Lock, Thread
from datetime import datetime, timedelta
import traceback  
from queue import Queue, Empty
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

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

# [Previous TestResultLogger class code remains the same...]

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

"""
    Definition of a session Si is as follows: 
        Si = < Mk, Li, Ri >
        Mk: str,   model name
        Li: float, SLO for model
        Ri: float, request rate for the model
"""

class session:
    """
    Session class representing a model deployment request

    Attributes:
       model_name (str): Name of the model to deploy
       latency_SLO (float): Latency Service Level Objective in milliseconds
       request_rate (float): Expected request rate for the model
       batch_size (int): Current batch size when scheduled (default: 0)
       creation_time (datetime): Timestamp when session was created
    """
    def __init__(self, model_name: str, latency_SLO: float, request_rate: float, batch_size: Optional[int] = None):
        """
        Initialize a new session with validation.

        Args:
            model_name (str): Name of model to deploy
            latency_SLO (float): Latency SLO in milliseconds
            request_rate (float): Expected request rate
            batch_size (Optional[int]): Initial batch size
            
        Raises:
            ValueError: If inputs fail validation
        """
        if not isinstance(model_name, str) or not model_name:
            raise ValueError("Model name must be a non-empty string")
        if not isinstance(latency_SLO, (int, float)) or latency_SLO <= 0:
            raise ValueError("Latency SLO must be a positive number")
        if not isinstance(request_rate, (int, float)) or request_rate < 0:
            raise ValueError("Request rate must be a non-negative number")
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ValueError("Batch size must be a positive integer")

        self.model_name = model_name
        self.latency_SLO = latency_SLO
        self.request_rate = request_rate
        self.batch_size = batch_size if batch_size is not None else 0
        self.creation_time = datetime.now()

    def print_session_pretty(self):
        print(f"Model name: {self.model_name}, SLO: {self.latency_SLO}ms, "
              f"request rate: {self.request_rate}, batch size: {self.batch_size}")

    def to_dict(self) -> dict:
        """Convert session to dictionary for logging"""
        return {
            'model_name': self.model_name,
            'latency_SLO': self.latency_SLO,
            'request_rate': self.request_rate,
            'batch_size': self.batch_size,
            'creation_time': self.creation_time.isoformat()
        }

"""
    Definition of a node:
        Node represents a GPU in hardware and a bin in the squishy bin packing problem.
        For now all nodes are assumed to be homogeeneous is nature, i.e, same gpu memory and gpu type. 
"""
class node:
    """Node class representing a GPU resource"""
    def __init__(self, node_sessions: Optional[List[Tuple[session, float]]] = None,
                 duty_cycle: Optional[float] = None,
                 gpu_type: str = 'A6000',
                 gpu_mem: float = 48.0):
        self.gpu_type = gpu_type
        self.gpu_mem = gpu_mem
        self.node_sessions = node_sessions if node_sessions is not None else []
        self.duty_cycle = duty_cycle if duty_cycle is not None else float('inf')
        self.creation_time = datetime.now()

    def get_occupancy(self) -> float:
        """Calculate total node occupancy"""
        return sum(occ for _, occ in self.node_sessions)

    def print_node_pretty(self):
        print("-" * 75)
        print(f"Node gpu type: {self.gpu_type}, Node gpu memory: {self.gpu_mem}GB")
        print(f"Node duty cycle: {self.duty_cycle}ms")
        print(f"Node sessions: {len(self.node_sessions)}")
        for i, (s, occ) in enumerate(self.node_sessions):
            print(f"Session {i+1} occupancy: {round(occ * 100, 2)}%")
            s.print_session_pretty()
        print("-" * 75)

    def to_dict(self) -> dict:
        """Convert node to dictionary for logging"""
        return {
            'gpu_type': self.gpu_type,
            'gpu_mem': self.gpu_mem,
            'duty_cycle': self.duty_cycle,
            'creation_time': self.creation_time.isoformat(),
            'occupancy': self.get_occupancy(),
            'sessions': [
                {
                    'session': s.to_dict(),
                    'occupancy': occ
                }
                for s, occ in self.node_sessions
            ]
        }

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
        self.sessions: Dict[str, session] = {}
        self.nodes: Dict[str, List[node]] = {}
        self.request_trackers: Dict[str, RequestTracker] = {}
        
        self.monitoring_interval = monitoring_interval
        self.rate_change_threshold = rate_change_threshold
        self.lock = Lock()
        self.schedule_changes = Queue()
        
        self.monitoring_thread: Optional[Thread] = None
        self._stop_monitoring = False
        self.logger = logging.getLogger("NexusScheduler")

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

    def _check_and_update_schedules(self) -> None:
        """Check request rates and update schedules if needed"""
        with self.lock:
            for model_name, tracker in self.request_trackers.items():
                if model_name not in self.sessions:
                    continue

                current_rate = tracker.get_request_rate()
                if current_rate == 0:
                    continue

                current_session = self.sessions[model_name]
                rate_diff = abs(current_rate - current_session.request_rate)
                
                # Check if rate change exceeds threshold
                if rate_diff / current_session.request_rate > self.rate_change_threshold:
                    self.logger.info(f"Rate change detected for {model_name}: {current_rate:.2f} req/s")
                    self._update_schedule(model_name, current_rate)
                    
                    # Update metrics
                    if model_name not in self.metrics['rate_changes']:
                        self.metrics['rate_changes'][model_name] = []
                    self.metrics['rate_changes'][model_name].append({
                        'timestamp': datetime.now(),
                        'old_rate': current_session.request_rate,
                        'new_rate': current_rate
                    })

    def _update_schedule(self, model_name: str, new_rate: float) -> None:
        """Update schedule for a model with new request rate"""
        updated_sessions = []
        for name, s in self.sessions.items():
            new_session = copy.deepcopy(s)
            if name == model_name:
                new_session.request_rate = new_rate
            updated_sessions.append(new_session)

        old_nodes = self.nodes.get(model_name, [])
        new_nodes = self.squishyBinPacking(updated_sessions)

        moves, new_deployments = self._get_schedule_changes(old_nodes, new_nodes)

        self.sessions[model_name].request_rate = new_rate
        self.nodes[model_name] = new_nodes

        change_record = {
            'model_name': model_name,
            'timestamp': datetime.now(),
            'old_rate': self.sessions[model_name].request_rate,
            'new_rate': new_rate,
            'moves': moves,
            'new_deployments': new_deployments
        }
        self.schedule_changes.put(change_record)

        # Update metrics
        self.metrics['schedule_updates'] += 1
        if model_name not in self.metrics['node_changes']:
            self.metrics['node_changes'][model_name] = []
        self.metrics['node_changes'][model_name].append({
            'timestamp': datetime.now(),
            'old_count': len(old_nodes),
            'new_count': len(new_nodes),
            'moves': len(moves),
            'new_deployments': len(new_deployments)
        })

    def squishyBinPacking(self, sessions: List[session]) -> List[node]:
        """Main scheduling algorithm"""
        try:
            nodes, residual_sessions = self.scheduleSaturate(sessions)
            residual_nodes = self.scheduleResidue(residual_sessions)
            nodes.extend(residual_nodes)
            
            # Log scheduling event
            self.logger.debug(f"Scheduling completed: {len(nodes)} nodes allocated, "
                           f"{len(residual_sessions)} residual sessions")
            return nodes
        except Exception as e:
            self.logger.error(f"Error in squishyBinPacking: {e}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            raise

    def scheduleSaturate(self, sessions: List[session]) -> Tuple[List[node], List[session]]:
        """Schedule full nodes at maximum batch size"""
        nodes: List[node] = []
        residual_sessions: List[session] = []

        for s in sessions:
            if s.request_rate == 0:
                continue

            try:
                # Create and sort list of batch sizes and latencies
                latencies = [(key, self.batching_profile[s.model_name][key]['avg_latency_ms']) 
                            for key in self.batching_profile[s.model_name].keys()]
                latencies.sort(key=lambda x: x[1])  # Sort by latency
                
                # Find the largest batch size that meets SLO/2
                target_latency = s.latency_SLO/2
                max_batch_ind = 0
                for i, (batch_size, latency) in enumerate(latencies):
                    if latency > target_latency:
                        break
                    max_batch_ind = i
                
                max_batch_size, max_latency = latencies[max_batch_ind]
                max_throughput = (max_batch_size/max_latency) * 1000

                n, r = divmod(s.request_rate, max_throughput)
                
                if n > 0:
                    new_session = session(s.model_name, s.latency_SLO, max_throughput, max_batch_size)
                    new_nodes = [node([(new_session, 1.0)], duty_cycle=max_latency) for _ in range(int(n))]
                    nodes.extend(new_nodes)

                if r > 0:
                    residual_sessions.append(session(s.model_name, s.latency_SLO, r))

            except Exception as e:
                self.logger.error(f"Error scheduling {s.model_name}: {e}")
                self.logger.error(f"Stack trace: {traceback.format_exc()}")
                continue

        return nodes, residual_sessions

    def scheduleResidue(self, sessions: List[session]) -> List[node]:
        """Schedule residual sessions"""
        nodes: List[node] = []
        single_nodes: List[node] = []

        for s in sessions:
            if s.request_rate == 0:
                continue

            try:
                # Create and sort list with request-adjusted latencies
                request_latencies = [(key, self.batching_profile[s.model_name][key]['avg_latency_ms'] + 
                                   key/s.request_rate)
                                   for key in self.batching_profile[s.model_name].keys()]
                request_latencies.sort(key=lambda x: x[1])  # Sort by latency
                
                # Find the largest batch size that meets SLO
                max_batch_ind = 0
                for i, (batch_size, latency) in enumerate(request_latencies):
                    if latency > s.latency_SLO:
                        break
                    max_batch_ind = i

                max_batch_size, max_latency = request_latencies[max_batch_ind]

                duty_cycle = (max_batch_size/s.request_rate) * 1000
                occupancy = max_latency/duty_cycle

                s.batch_size = max_batch_size
                single_nodes.append(node([(s, occupancy)], duty_cycle=duty_cycle))

            except Exception as e:
                self.logger.error(f"Error scheduling residual {s.model_name}: {e}")
                self.logger.error(f"Stack trace: {traceback.format_exc()}")
                continue

        # Sort nodes by occupancy for better packing
        sorted_nodes = sorted(single_nodes, key=lambda n: n.get_occupancy(), reverse=True)

        # Try to merge nodes
        for residual_node in sorted_nodes:
            max_occupancy = 0
            max_node_ind = None
            max_node = None

            for i, n in enumerate(nodes):
                new_node = self.mergeNodes(n, residual_node)
                if new_node and new_node.get_occupancy() > max_occupancy:
                    max_occupancy = new_node.get_occupancy()
                    max_node_ind = i
                    max_node = new_node

            if max_node:
                nodes[max_node_ind] = max_node
            else:
                nodes.append(residual_node)

        return nodes
    
    def mergeNodes(self, node1: node, node2: node) -> Optional[node]:
        """Merge two nodes if possible"""
        try:
            if node1.duty_cycle < node2.duty_cycle:
                node1, node2 = node2, node1

            new_node = copy.deepcopy(node2)
            duty_cycle = node2.duty_cycle

            for s, occ in node1.node_sessions:
                new_batch = int(math.ceil((duty_cycle * s.request_rate) / 1000))
                
                # Verify batch size exists in profile
                if new_batch not in self.batching_profile[s.model_name]:
                    self.logger.warning(f"Invalid batch size {new_batch} for {s.model_name}")
                    return None

                new_latency = self.batching_profile[s.model_name][new_batch]['avg_latency_ms']
                
                new_node.node_sessions.append((
                    session(s.model_name, s.latency_SLO, s.request_rate, batch_size=new_batch), 
                    new_latency/duty_cycle
                ))

            # Check occupancy constraint
            if new_node.get_occupancy() > 1:
                return None

            # Check memory constraint
            total_memory = 0
            for s, occ in new_node.node_sessions:
                total_memory += float(self.batching_profile[s.model_name][s.batch_size]['peak_memory_mb'])/1024
            if total_memory > new_node.gpu_mem:
                return None

            return new_node

        except Exception as e:
            self.logger.error(f"Error merging nodes: {e}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def _get_schedule_changes(self, old_nodes: List[node], new_nodes: List[node]) -> Tuple[List[Tuple[session, node]], List[Tuple[session, node]]]:
        """Calculate required changes between old and new schedules"""
        moves = []
        new_deployments = []
        
        try:
            old_locations = {s.model_name: n for n in old_nodes for s, _ in n.node_sessions}
            new_locations = {s.model_name: n for n in new_nodes for s, _ in n.node_sessions}

            for model_name, new_node in new_locations.items():
                if model_name in old_locations:
                    if old_locations[model_name] != new_node:
                        session_obj = next(s for s, _ in new_node.node_sessions if s.model_name == model_name)
                        moves.append((session_obj, new_node))
                else:
                    session_obj = next(s for s, _ in new_node.node_sessions if s.model_name == model_name)
                    new_deployments.append((session_obj, new_node))

        except Exception as e:
            self.logger.error(f"Error calculating schedule changes: {e}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")

        return moves, new_deployments

    def get_recent_changes(self, minutes: int = 5) -> List[dict]:
        """Get recent schedule changes"""
        changes = []
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        temp_queue = Queue()
        try:
            while not self.schedule_changes.empty():
                change = self.schedule_changes.get()
                if change['timestamp'] >= cutoff_time:
                    changes.append(change)
                temp_queue.put(change)
                
            while not temp_queue.empty():
                self.schedule_changes.put(temp_queue.get())
                
        except Exception as e:
            self.logger.error(f"Error retrieving recent changes: {e}")
            
        return sorted(changes, key=lambda x: x['timestamp'])

    def get_metrics(self) -> dict:
        """Get current scheduler metrics"""
        with self.lock:
            metrics = copy.deepcopy(self.metrics)
            metrics['current_state'] = {
                'active_models': len(self.sessions),
                'total_nodes': sum(len(nodes) for nodes in self.nodes.values()),
                'request_rates': {
                    model: tracker.get_request_rate()
                    for model, tracker in self.request_trackers.items()
                }
            }
            return metrics

    def get_current_state(self) -> dict:
        """Get complete current state of the scheduler"""
        with self.lock:
            return {
                'sessions': {
                    name: session.to_dict() 
                    for name, session in self.sessions.items()
                },
                'nodes': {
                    model: [n.to_dict() for n in nodes]
                    for model, nodes in self.nodes.items()
                },
                'metrics': self.get_metrics(),
                'timestamp': datetime.now().isoformat()
            }
        
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
    
    profiling_dir = "/Users/sai/Desktop/CSE293P/ray-dynamic-batching/293-project/profiling"
    
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
    models_config = [
        #('vit', 25, 1000),        # (model_name, SLO, initial_rate)
        ('resnet', 50, 2000),
        ('shufflenet', 30, 1500),
        ('efficientnet', 40, 1200)
    ]

    for model_name, slo, rate in models_config:
        scheduler.sessions[model_name] = session(model_name, slo, rate)
        logging.info(f"Registered {model_name} with SLO={slo}ms, rate={rate} req/s")

    # Initial scheduling
    initial_sessions = list(scheduler.sessions.values())
    initial_nodes = scheduler.squishyBinPacking(initial_sessions)
    
    # Group nodes by model
    for model_name in scheduler.sessions:
        scheduler.nodes[model_name] = [
            n for n in initial_nodes 
            if any(s.model_name == model_name for s, _ in n.node_sessions)
        ]

    # Log initial state
    logger.log_node_state('initial', scheduler.nodes, 
                         datetime.now().strftime('%H:%M:%S'))

    print("\nInitial Schedule:")
    for model_name, nodes in scheduler.nodes.items():
        print(f"\n{model_name} nodes:")
        for n in nodes:
            n.print_node_pretty()

    # Start monitoring
    print("\nStarting request monitoring...")
    scheduler.start_monitoring()

    try:
        print("\nSimulating workload...")
        print("Press Ctrl+C to stop")
        
        start_time = time.time()
        last_print_time = start_time
        
        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            # Simulate varying workload for each model
            for model_name, _, base_rate in models_config:
                # Create different patterns for each model
                if model_name == 'vit':
                    variation = math.sin(elapsed_time / 60)  # 1-minute cycle
                elif model_name == 'resnet':
                    variation = math.cos(elapsed_time / 45)  # 45-second cycle
                elif model_name == 'shufflenet':
                    variation = math.sin(elapsed_time / 30)  # 30-second cycle
                else:  # efficientnet
                    variation = math.sin(elapsed_time / 90)  # 90-second cycle

                current_rate = base_rate * (1 + 0.3 * variation)  # ±30% variation
                
                # Record requests
                requests_this_interval = int(current_rate * 0.1)  # Scale down for simulation
                for _ in range(requests_this_interval):
                    scheduler.record_request(model_name)
            
            # Log and print status every 10 seconds
            if current_time - last_print_time >= 10:
                timestamp = datetime.now().strftime('%H:%M:%S')
                print("\n" + "="*80)
                print(f"Status Update at {timestamp}")
                
                # Get current metrics
                metrics = {
                    'timestamp': timestamp,
                    'rates': {
                        model: tracker.get_request_rate()
                        for model, tracker in scheduler.request_trackers.items()
                    },
                    'nodes': {
                        model: len(nodes)
                        for model, nodes in scheduler.nodes.items()
                    },
                    'scheduler_state': scheduler.get_metrics()
                }
                
                # Log metrics
                logger.log_metrics('continuous_run', metrics)
                
                # Log node states
                logger.log_node_state('continuous_run', 
                                    scheduler.nodes,
                                    timestamp)
                
                # Print current request rates
                for model_name, tracker in scheduler.request_trackers.items():
                    current_rate = tracker.get_request_rate()
                    print(f"\n{model_name} current request rate: {current_rate:.2f} req/s")
                
                # Check and log recent changes
                changes = scheduler.get_recent_changes(minutes=1)
                if changes:
                    logger.log_changes('continuous_run', changes)
                    print("\nRecent schedule changes:")
                    for change in changes:
                        print(f"\nModel: {change['model_name']}")
                        print(f"Time: {change['timestamp'].strftime('%H:%M:%S')}")
                        print(f"Rate change: {change['old_rate']:.2f} -> {change['new_rate']:.2f}")
                        print(f"Moves needed: {len(change['moves'])}")
                        print(f"New deployments: {len(change['new_deployments'])}")

                # Print current node allocations
                print("\nCurrent Node Allocations:")
                for model_name, nodes in scheduler.nodes.items():
                    print(f"\n{model_name} nodes:")
                    for n in nodes:
                        n.print_node_pretty()
                
                last_print_time = current_time
            
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
        scheduler.stop_monitoring()
        
        # Log final state
        logger.log_node_state('final', scheduler.nodes, 
                          datetime.now().strftime('%H:%M:%S'))
        logger.log_metrics('final', scheduler.get_metrics())
        
        print(f"Results stored in: {logger.test_dir}")
        print("Shutdown complete")

if __name__ == "__main__":
    main()
