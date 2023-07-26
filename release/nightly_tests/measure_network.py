import psutil
import time
import argparse


def main():
    parser = argparse.ArgumentParser(description="Process and system network monitor")
    parser.add_argument("timeframe", metavar="T", type=int, help="Timeframe in seconds")

    args = parser.parse_args()

    net_io_start = psutil.net_io_counters(pernic=True)
    s = time.time()

    print(f"Running for {args.timeframe} seconds")
    for i in range(args.timeframe):
        net_io_end = psutil.net_io_counters(pernic=True)
        for device, value in net_io_end.items():
            # Ignore loopback device
            if device == "lo":
                continue

            s_v = net_io_start.get(device, 0)
            elapsed = time.time() - s
            sent_mb = (value.bytes_sent - s_v.bytes_sent) / 1024 / 1024
            recv_mb = (value.bytes_recv - s_v.bytes_recv) / 1024 / 1024

            if sent_mb > 0:
                print(f"{device} Total system bytes sent in last {elapsed} seconds.")
                print(f"\t sent cumulative: {sent_mb} MB")
                print(f"\t sent throughput {sent_mb / (elapsed)} MB/s")
            if recv_mb > 0:
                print(f"{device} Total system bytes received in last {elapsed} seconds")
                print(f"\t recv cumulative: {recv_mb} MB")
                print(f"\t recv throughput: {recv_mb / (elapsed)} MB/s\n")
        time.sleep(1)


if __name__ == "__main__":
    main()
