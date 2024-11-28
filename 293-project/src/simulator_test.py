import zmq
import time
import threading

def process_request(request):
    print(f"Received request: {request}")

def listen_for_requests():
    context = zmq.Context()
    socket  = context.socket(zmq.PULL)
    socket.bind("tcp://*:5555")

    while True:
        request = socket.recv_json()
        process_request(request)

if __name__ == '__main__':
    threading.Thread(target=listen_for_requests, daemon=True).start()

    while True:
        time.sleep(1)