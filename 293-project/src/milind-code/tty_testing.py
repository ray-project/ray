import math
import time

def generate_sinusoid():
    t = 0
    while True:
        # Generate a sinusoid value
        value = math.sin(t)
        
        # Print the value
        print(f"{value:.4f}")
        
        # Flush stdout to ensure immediate output
        print(flush=True)
        
        # Increment time
        t += 0.1
        
        # Sleep for 1/10th of a second
        time.sleep(0.1)

if __name__ == "__main__":
    try:
        generate_sinusoid()
    except KeyboardInterrupt:
        print("\nStopped by user")