import skein
import sys
from urllib.parse import urlparse

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dashboard.py <dashboard-address>")
        sys.exit(1)
    address = sys.argv[1]
    # Check if the address is a valid URL
    result = urlparse(address)
    if not all([result.scheme, result.netloc]):
        print("Error: Invalid dashboard address. Please provide a valid URL.")
        sys.exit(1)

    print("Registering dashboard " + address + " on skein.")
    app = skein.ApplicationClient.from_current()
    app.ui.add_page("ray-dashboard", address, "Ray Dashboard")
