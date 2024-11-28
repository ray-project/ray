"""
    Dynamic request simulator. Allows to change request rate through terminal commands
"""

import os
import zmq
import time
import random
import threading

class RequestSimulator:
    def __init__(self, model_name: str, request_rate: float = 0, dataset: str = '../dataset'):
        # ipc info
        self.context = zmq.Context()
        self.socket  = self.context.socket(zmq.PUSH)
        self.socket.connect("tcp://localhost:5555")

        # model info
        self.model_name   = model_name
        self.dataset      = [os.path.join(dataset, f) for f in os.listdir(dataset)]
        self.dataset_size = len(self.dataset)
        self.request_rate = request_rate

        # state info
        self.is_running = False

    def send_requests(self):
        while self.is_running:
            request = {
                'timestamp': time.time(),
                'model_name': self.model_name,
                'image_path': self.dataset[random.randint(0, self.dataset_size - 1)]
            }

            self.socket.send_json(request)
            time.sleep(1 / self.request_rate)

    def start(self):
        if self.is_running:
            return
        
        self.is_running = True
        threading.Thread(target=self.send_requests, daemon=True).start()

    def stop(self):
        self.is_running = False

    def update_request_rate(self, new_request_rate: float):
        if new_request_rate == 0:
            self.stop()
            return
        
        self.request_rate = new_request_rate

    def print_state(self):
        return f"Model: {self.model_name} Request Rate: {self.request_rate}"

allowed_model_list = [
    'resnet',
    'vit'
]

def get_user_input():
    # get and verify model name
    model_name = input("Enter the model name: ").strip()
    if model_name not in allowed_model_list:
        raise ValueError(f"{model_name} not in allowed model list\nPlease choose model from: {allowed_model_list}")
    
    # get and verify request rate
    while True:
        try:
            request_rate = float(input("Enter request rate in seconds: "))
            if request_rate < 0:
                raise ValueError(f"Request rate can't be negative")
            else:
                break
        except ValueError:
            raise ValueError("Invalid input, please enter valid number")

    return model_name, request_rate

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_current_state(simulators):
    clear_screen()
    print("-"*50)
    print("Current State: ")
    for sim in simulators.values():
        print(f" --> {sim.print_state()}")
    print("-"*50)

def main():
    simulator_list = {}
    while True:
        try:
            model_name, request_rate = get_user_input()

            if model_name not in simulator_list:
                simulator_list[model_name] = RequestSimulator(model_name, request_rate)
                simulator_list[model_name].start()
            else:
                simulator_list[model_name].update_request_rate(request_rate)

            print_current_state(simulator_list)

        except ValueError as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    main()