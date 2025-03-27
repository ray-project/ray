"""
Test script for NexusScheduler
Provides comprehensive testing including unit tests, integration tests,
and various workload patterns
"""

import unittest
import time
import random
import threading
import logging
from datetime import datetime
import math
import os
from pathlib import Path
import json
from typing import List, Dict, Optional
import traceback

from nexus_scheduler import (
    NexusScheduler, 
    session, 
    node, 
    RequestTracker, 
    BatchProfiler,
    TestResultLogger
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Sample batch profile for testing
SAMPLE_BATCH_PROFILE = {
    'resnet': {
        1: {'avg_latency_ms': 10.0, 'peak_memory_mb': 1000},
        2: {'avg_latency_ms': 15.0, 'peak_memory_mb': 1800},
        4: {'avg_latency_ms': 25.0, 'peak_memory_mb': 3000},
        8: {'avg_latency_ms': 45.0, 'peak_memory_mb': 5000},
        16: {'avg_latency_ms': 85.0, 'peak_memory_mb': 8000},
    },
    'vit': {
        1: {'avg_latency_ms': 5.0, 'peak_memory_mb': 800},
        2: {'avg_latency_ms': 8.0, 'peak_memory_mb': 1400},
        4: {'avg_latency_ms': 14.0, 'peak_memory_mb': 2400},
        8: {'avg_latency_ms': 25.0, 'peak_memory_mb': 4000},
        16: {'avg_latency_ms': 45.0, 'peak_memory_mb': 6500},
    },
    'shufflenet': {
        1: {'avg_latency_ms': 3.0, 'peak_memory_mb': 500},
        2: {'avg_latency_ms': 5.0, 'peak_memory_mb': 900},
        4: {'avg_latency_ms': 9.0, 'peak_memory_mb': 1600},
        8: {'avg_latency_ms': 16.0, 'peak_memory_mb': 2800},
        16: {'avg_latency_ms': 30.0, 'peak_memory_mb': 5000},
    },
    'efficientnet': {
        1: {'avg_latency_ms': 7.0, 'peak_memory_mb': 1200},
        2: {'avg_latency_ms': 12.0, 'peak_memory_mb': 2000},
        4: {'avg_latency_ms': 20.0, 'peak_memory_mb': 3500},
        8: {'avg_latency_ms': 35.0, 'peak_memory_mb': 6000},
        16: {'avg_latency_ms': 65.0, 'peak_memory_mb': 10000},
    }
}

class WorkloadGenerator:
    """Generates synthetic workload for testing"""
    def __init__(self, scheduler: NexusScheduler, model_name: str, 
                 base_rate: float, pattern: str = 'sinusoidal',
                 pattern_period: float = 60.0):
        self.scheduler = scheduler
        self.model_name = model_name
        self.base_rate = base_rate
        self.pattern = pattern
        self.pattern_period = pattern_period
        self.stop_generation = False
        self.thread = None
        self.stats = {
            'total_requests': 0,
            'start_time': None,
            'end_time': None,
            'rate_history': []
        }

    def start(self):
        """Start generating workload"""
        self.stop_generation = False
        self.stats['start_time'] = datetime.now()
        self.thread = threading.Thread(target=self._generate_workload)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        """Stop generating workload"""
        self.stop_generation = True
        if self.thread:
            self.thread.join()
        self.stats['end_time'] = datetime.now()

    def _generate_workload(self):
        """Generate requests according to the specified pattern"""
        start_time = time.time()
        last_stats_time = start_time
        
        while not self.stop_generation:
            try:
                current_time = time.time()
                elapsed_time = current_time - start_time
                
                # Calculate current rate based on pattern
                if self.pattern == 'sinusoidal':
                    variation = math.sin(2 * math.pi * elapsed_time / self.pattern_period)
                    current_rate = self.base_rate * (1 + 0.3 * variation)
                elif self.pattern == 'step':
                    current_rate = self.base_rate * (2 if int(elapsed_time / 30) % 2 else 1)
                elif self.pattern == 'random':
                    current_rate = self.base_rate * (0.5 + random.random())
                elif self.pattern == 'spike':
                    is_spike = (elapsed_time % self.pattern_period) < (self.pattern_period * 0.1)
                    current_rate = self.base_rate * (3 if is_spike else 1)
                else:  # constant
                    current_rate = self.base_rate

                # Record stats every second
                if current_time - last_stats_time >= 1.0:
                    self.stats['rate_history'].append({
                        'timestamp': datetime.now().isoformat(),
                        'rate': current_rate
                    })
                    last_stats_time = current_time

                # Generate requests
                sleep_time = 1.0 / current_rate
                self.scheduler.record_request(self.model_name)
                self.stats['total_requests'] += 1
                
                time.sleep(sleep_time)

            except Exception as e:
                logging.error(f"Error in workload generation for {self.model_name}: {e}")
                logging.error(traceback.format_exc())
                time.sleep(1)  # Avoid tight loop in case of persistent errors

    def get_stats(self) -> dict:
        """Get workload generation statistics"""
        return {
            'model_name': self.model_name,
            'base_rate': self.base_rate,
            'pattern': self.pattern,
            'pattern_period': self.pattern_period,
            'stats': self.stats
        }
    
class TestRequestTracker(unittest.TestCase):
    """Unit tests for RequestTracker class"""
    
    def setUp(self):
        self.tracker = RequestTracker(window_size=1.0)

    def test_request_recording(self):
        """Test basic request recording"""
        self.tracker.record_request()
        self.assertEqual(self.tracker.request_count, 1)

    def test_rate_calculation(self):
        """Test request rate calculation"""
        for _ in range(100):
            self.tracker.record_request()
        time.sleep(1.1)  # Wait for window to expire
        rate = self.tracker.get_request_rate()
        self.assertGreater(rate, 0)
        self.assertLess(rate, 100)

    def test_window_reset(self):
        """Test rate window resets properly"""
        for _ in range(50):
            self.tracker.record_request()
        time.sleep(1.1)  # Wait for window to expire
        self.tracker.record_request()  # New window
        self.assertEqual(self.tracker.request_count, 1)

class TestSession(unittest.TestCase):
    """Unit tests for session class"""

    def test_valid_session(self):
        """Test valid session creation"""
        s = session("model1", 50.0, 1000.0, 8)
        self.assertEqual(s.model_name, "model1")
        self.assertEqual(s.latency_SLO, 50.0)
        self.assertEqual(s.request_rate, 1000.0)
        self.assertEqual(s.batch_size, 8)

    def test_session_defaults(self):
        """Test session default values"""
        s = session("model1", 50.0, 1000.0)
        self.assertEqual(s.batch_size, 0)

class TestNode(unittest.TestCase):
    """Unit tests for node class"""

    def test_node_creation(self):
        """Test node creation and initialization"""
        s = session("model1", 50.0, 1000.0, 8)
        n = node([(s, 0.5)], duty_cycle=100.0)
        self.assertEqual(len(n.node_sessions), 1)
        self.assertEqual(n.duty_cycle, 100.0)

    def test_occupancy_calculation(self):
        """Test node occupancy calculation"""
        s1 = session("model1", 50.0, 1000.0, 8)
        s2 = session("model2", 50.0, 1000.0, 8)
        n = node([(s1, 0.3), (s2, 0.4)], duty_cycle=100.0)
        self.assertAlmostEqual(n.get_occupancy(), 0.7)

class TestScheduler(unittest.TestCase):
    """Unit tests for NexusScheduler class"""

    def setUp(self):
        self.scheduler = NexusScheduler(SAMPLE_BATCH_PROFILE)
        self.test_session = session('resnet', 50, 1000)
        self.scheduler.sessions['resnet'] = self.test_session

    def test_schedule_creation(self):
        """Test basic schedule creation"""
        nodes = self.scheduler.squishyBinPacking([self.test_session])
        self.assertGreater(len(nodes), 0)

    def test_rate_tracking(self):
        """Test request rate tracking"""
        for _ in range(100):
            self.scheduler.record_request('resnet')
        time.sleep(1)
        rate = self.scheduler.request_trackers['resnet'].get_request_rate()
        self.assertGreater(rate, 0)

    def test_schedule_update(self):
        """Test schedule updates with rate changes"""
        initial_nodes = self.scheduler.squishyBinPacking([self.test_session])
        self.scheduler.nodes['resnet'] = initial_nodes

        # Simulate rate change
        for _ in range(1000):
            self.scheduler.record_request('resnet')
        time.sleep(1)

        self.scheduler.start_monitoring()
        time.sleep(self.scheduler.monitoring_interval * 2)
        self.scheduler.stop_monitoring()

        changes = self.scheduler.get_recent_changes(minutes=1)
        self.assertGreater(len(changes), 0)

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system"""

    def setUp(self):
        self.scheduler = NexusScheduler(SAMPLE_BATCH_PROFILE)
        self.logger = TestResultLogger()

    def test_multi_model_scheduling(self):
        """Test scheduling multiple models simultaneously"""
        models = [
            ('resnet', 50, 1000),
            ('vit', 25, 2000),
            ('shufflenet', 30, 1500)
        ]
        
        # Register models
        for model_name, slo, rate in models:
            self.scheduler.sessions[model_name] = session(model_name, slo, rate)

        # Initial scheduling
        nodes = self.scheduler.squishyBinPacking(list(self.scheduler.sessions.values()))
        self.assertGreater(len(nodes), 0)

        # Group nodes by model
        for model_name in self.scheduler.sessions:
            model_nodes = [n for n in nodes 
                         if any(s.model_name == model_name for s, _ in n.node_sessions)]
            self.scheduler.nodes[model_name] = model_nodes

        self.logger.log_node_state('multi_model_test', 
                                 self.scheduler.nodes,
                                 datetime.now().strftime('%H:%M:%S'))

    def test_dynamic_workload(self):
        """Test system behavior under dynamic workload"""
        # Setup model
        model_name = 'resnet'
        self.scheduler.sessions[model_name] = session(model_name, 50, 1000)
        
        # Start monitoring
        self.scheduler.start_monitoring()
        
        try:
            # Generate varying workload
            start_time = time.time()
            duration = 30  # 30 seconds test
            
            while time.time() - start_time < duration:
                current_time = time.time()
                variation = math.sin(2 * math.pi * (current_time - start_time) / 10)  # 10-second cycle
                current_rate = 1000 * (1 + 0.5 * variation)  # ±50% variation
                
                requests_this_second = int(current_rate / 10)
                for _ in range(requests_this_second):
                    self.scheduler.record_request(model_name)
                
                time.sleep(0.1)
            
            # Verify changes occurred
            changes = self.scheduler.get_recent_changes()
            self.assertGreater(len(changes), 0)
            
        finally:
            self.scheduler.stop_monitoring()

        self.logger.log_metrics('dynamic_workload_test', 
                              self.scheduler.get_metrics())
        

class WorkloadPattern:
    """Base class for different workload patterns"""
    def __init__(self, base_rate: float, duration: float):
        self.base_rate = base_rate
        self.duration = duration

    def get_rate(self, elapsed_time: float) -> float:
        """Get the rate at a given elapsed time"""
        raise NotImplementedError

class SinusoidalPattern(WorkloadPattern):
    def __init__(self, base_rate: float, duration: float, period: float = 60.0, amplitude: float = 0.3):
        super().__init__(base_rate, duration)
        self.period = period
        self.amplitude = amplitude

    def get_rate(self, elapsed_time: float) -> float:
        variation = math.sin(2 * math.pi * elapsed_time / self.period)
        return self.base_rate * (1 + self.amplitude * variation)

class StepPattern(WorkloadPattern):
    def __init__(self, base_rate: float, duration: float, step_duration: float = 30.0, step_factor: float = 2.0):
        super().__init__(base_rate, duration)
        self.step_duration = step_duration
        self.step_factor = step_factor

    def get_rate(self, elapsed_time: float) -> float:
        return self.base_rate * (self.step_factor if int(elapsed_time / self.step_duration) % 2 else 1)

class SpikePattern(WorkloadPattern):
    def __init__(self, base_rate: float, duration: float, spike_interval: float = 60.0, spike_duration: float = 5.0, spike_factor: float = 3.0):
        super().__init__(base_rate, duration)
        self.spike_interval = spike_interval
        self.spike_duration = spike_duration
        self.spike_factor = spike_factor

    def get_rate(self, elapsed_time: float) -> float:
        time_in_cycle = elapsed_time % self.spike_interval
        return self.base_rate * (self.spike_factor if time_in_cycle < self.spike_duration else 1)

def run_workload_test(scheduler: NexusScheduler, 
                     logger: TestResultLogger,
                     test_name: str,
                     workload_configs: List[tuple],
                     duration: float,
                     log_interval: float = 10.0):
    """Run a test with specific workload configurations"""
    logging.info(f"\nStarting {test_name}...")
    
    # Create workload generators
    generators = []
    for model_name, slo, base_rate, pattern_type in workload_configs:
        # Register model if not already registered
        if model_name not in scheduler.sessions:
            scheduler.sessions[model_name] = session(model_name, slo, base_rate)
            
        # Create generator with specified pattern
        generator = WorkloadGenerator(scheduler, model_name, base_rate, pattern_type)
        generators.append(generator)
    
    # Start monitoring and workload generation
    scheduler.start_monitoring()
    for gen in generators:
        gen.start()
    
    try:
        start_time = time.time()
        last_log_time = start_time
        
        while time.time() - start_time < duration:
            current_time = time.time()
            
            # Log status at specified intervals
            if current_time - last_log_time >= log_interval:
                timestamp = datetime.now().strftime('%H:%M:%S')
                
                # Collect metrics
                metrics = {
                    'timestamp': timestamp,
                    'elapsed_time': current_time - start_time,
                    'rates': {
                        model: tracker.get_request_rate()
                        for model, tracker in scheduler.request_trackers.items()
                    },
                    'nodes': {
                        model: len(nodes)
                        for model, nodes in scheduler.nodes.items()
                    },
                    'scheduler_metrics': scheduler.get_metrics(),
                    'workload_stats': {
                        gen.model_name: gen.get_stats()
                        for gen in generators
                    }
                }
                
                # Log everything
                logger.log_metrics(f"{test_name}_metrics", metrics)
                logger.log_node_state(test_name, scheduler.nodes, timestamp)
                
                changes = scheduler.get_recent_changes(minutes=1)
                if changes:
                    logger.log_changes(test_name, changes)
                
                last_log_time = current_time
            
            time.sleep(0.1)
            
    finally:
        for gen in generators:
            gen.stop()
        scheduler.stop_monitoring()
        
        # Log final state
        logger.log_node_state(f"{test_name}_final", 
                            scheduler.nodes,
                            datetime.now().strftime('%H:%M:%S'))
        
    logging.info(f"{test_name} completed")
    return True

def main():
    """Main function to run all tests"""
    # Create results directory
    logger = TestResultLogger()
    logging.info(f"Test results will be stored in: {logger.test_dir}")

    # Load actual profiles if available, otherwise use sample
    profiling_dir = "/Users/sai/Desktop/CSE293P/ray-dynamic-batching/293-project/profiling"

    model_files = {
        'vit': 'vit_g16_20241123_154354_summary.csv',
        'resnet': 'resnet50_20241117_154052_summary.csv',
        'shufflenet': 'shufflenet_20241123_104115_summary.csv',
        'efficientnet': 'efficientnetv2_20241123_125206_summary.csv'
    }

    try:
        # Try to load actual profiles
        profiler = BatchProfiler()
        batching_profile = {}
        for model_name, filename in model_files.items():
            file_path = os.path.join(profiling_dir, filename)
            profile = profiler.load_csv_to_dict(file_path)
            if profile:
                batching_profile[model_name] = profile
                logging.info(f"Loaded profile for {model_name}")
    except Exception as e:
        logging.warning(f"Error loading profiles: {e}. Using sample profiles.")
        batching_profile = SAMPLE_BATCH_PROFILE

    # Create scheduler
    scheduler = NexusScheduler(batching_profile)

    # Define test scenarios
    test_scenarios = [
        {
            'name': 'baseline_test',
            'duration': 60,
            'workloads': [
                ('resnet', 50, 1000, 'constant'),
                ('vit', 25, 2000, 'constant')
            ]
        },
        {
            'name': 'varying_load_test',
            'duration': 180,
            'workloads': [
                ('resnet', 50, 1000, 'sinusoidal'),
                ('vit', 25, 2000, 'step'),
                ('shufflenet', 30, 1500, 'spike'),
                ('efficientnet', 40, 1200, 'random')
            ]
        },
        {
            'name': 'stress_test',
            'duration': 120,
            'workloads': [
                ('resnet', 50, 1000, 'spike'),
                ('vit', 25, 2000, 'spike'),
                ('shufflenet', 30, 1500, 'spike'),
                ('efficientnet', 40, 1200, 'spike')
            ]
        }
    ]

    # Run unit tests
    logging.info("\nRunning unit tests...")
    unittest.main(argv=['dummy'], exit=False)

    # Run test scenarios
    logging.info("\nRunning test scenarios...")
    for scenario in test_scenarios:
        try:
            run_workload_test(
                scheduler=scheduler,
                logger=logger,
                test_name=scenario['name'],
                workload_configs=scenario['workloads'],
                duration=scenario['duration']
            )
            time.sleep(5)  # Pause between scenarios
        except Exception as e:
            logging.error(f"Error in scenario {scenario['name']}: {e}")
            logging.error(traceback.format_exc())

    logging.info("\nAll tests completed")
    logging.info(f"Results are stored in: {logger.test_dir}")

if __name__ == "__main__":
    main()
