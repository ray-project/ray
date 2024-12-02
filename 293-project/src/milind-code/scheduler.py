"""
    Code for the main scheduler

    Listens to request on port 5555, schedules them on the GPUs and reports results on port 5556
"""

import os
import ray
import zmq
import time
import torch
import logging

import threading
from threading import Lock
from ray.util.queue import Queue as RayQueue
import torchvision.transforms as transforms
from PIL import Image

class RequestHandle:
    request_queue = {}
    request_rate  = {}
    request_count = {}
    img_transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
    ])

    def __init__(self, max_size=1000):
        # ipc info
        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.PULL)
        self.socket.bind("tcp://*:5555")

        # queue info
        self.max_size = max_size
        self.lock     = Lock()

        # logger info
        self._logger = logging.getLogger(f"RequestHandle")

        # start request rate monitor
        threading.Thread(target=self.update_request_rate, daemon=True).start()

        # start listening to requests
        threading.Thread(target=self.listen_for_requests, daemon=True).start()

    """
        function running to update request rate of all models every 100ms
    """
    def update_request_rate(self, time_period=1000):
        while True:
            with self.lock:
                for model in self.request_count.keys():
                    self.request_rate[model]  = self.request_count[model] * (1000 / time_period) # converting to per second
                    self.request_count[model] = 0 # reset count

            time.sleep(time_period / 1000)

    """
        get request rate
    """
    def get_request_rate(self):
        rate_copy = {}
        with self.lock:
            for model in self.request_rate:
                rate_copy[model] = self.request_rate[model]

        return rate_copy

    """
        listen to requests from port
    """
    def listen_for_requests(self):
        while True:
            request = self.socket.recv_json()
            self.process_request(request)

    """
        process request by adding to corresponding queue
    """
    def process_request(self, request):
        """Add request to queue with monitoring"""
        try:
            model_name = request['model_name']
            if self.request_queue[model_name].full():
                self._logger.warning(f"Queue full for {self.model_name}")
                return False
            
            request_id   = request['request_id']
            input_tensor = torch.rand(3, 224, 224)
            # input_tensor = self.img_transform(Image.open(request['image_path']))
            SLO          = request['SLO']
            self.request_queue[model_name].put((request_id, input_tensor, time.time(), SLO))
            with self.lock:
                self.request_count[model_name] += 1
            return True
        except Exception as e:
            self._logger.error(f"Error adding request: {e}")
            return False

    """
        add queue for model
    """
    def add_model(self, model_name):
        if model_name not in self.request_queue:
            self.request_queue[model_name] = RayQueue(maxsize=self.max_size)
        
        if model_name not in self.request_count:
            self.request_count[model_name] = 0
        
        if model_name not in self.request_rate:
            self.request_rate[model_name] = 0
    
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    request_handle = RequestHandle()

    request_handle.add_model('resnet')
    request_handle.add_model('vit')

    print(f"finished setting up request handle")
    while True:
        clear_screen()
        request_rate = request_handle.get_request_rate()
        for model in request_rate:
            print(f"{model}, {request_rate[model]}")
        time.sleep(0 / 1000)

if __name__ == "__main__":
    main()