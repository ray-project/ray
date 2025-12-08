#!/usr/bin/env python3
"""
Monitor Ray health check metrics in real-time.

Run this script in a separate terminal while running the reproduction script
to see health check latencies spike.
"""

import time
import requests
import argparse
from collections import deque


def parse_metrics(text):
    """Parse Prometheus metrics format."""
    metrics = {}
    for line in text.split('\n'):
        if line.startswith('#') or not line.strip():
            continue
        try:
            parts = line.split()
            if len(parts) >= 2:
                metric_name = parts[0].split('{')[0]
                value = float(parts[-1])
                metrics[metric_name] = value
        except:
            pass
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Monitor Ray health check metrics")
    parser.add_argument("--port", type=int, default=8080, help="Metrics export port")
    parser.add_argument("--interval", type=float, default=1.0, help="Poll interval in seconds")
    args = parser.parse_args()
    
    url = f"http://localhost:{args.port}/metrics"
    latencies = deque(maxlen=20)
    
    print("="*80)
    print("Monitoring Ray Health Check Latency")
    print("="*80)
    print(f"Metrics URL: {url}")
    print(f"Poll interval: {args.interval}s")
    print(f"\n{'Time':<12} {'Latency (ms)':<15} {'Status':<20} {'Sparkline':<30}")
    print("-"*80)
    
    try:
        while True:
            try:
                response = requests.get(url, timeout=2)
                metrics = parse_metrics(response.text)
                
                # Look for health check latency
                latency = None
                for key in metrics:
                    if 'health_check_rpc_latency_ms' in key:
                        latency = metrics[key]
                        break
                
                if latency is not None:
                    latencies.append(latency)
                    
                    # Determine status
                    if latency < 1000:
                        status = "✓ HEALTHY"
                    elif latency < 5000:
                        status = "⚠️  ELEVATED"
                    else:
                        status = "🔴 CRITICAL"
                    
                    # Create sparkline
                    max_latency = max(latencies) if latencies else 1
                    sparkline = ""
                    for l in latencies:
                        height = int((l / max_latency) * 8)
                        sparkline += "▁▂▃▄▅▆▇█"[min(height, 7)]
                    
                    timestamp = time.strftime("%H:%M:%S")
                    print(f"{timestamp:<12} {latency:>10.1f} ms   {status:<20} {sparkline:<30}")
                else:
                    timestamp = time.strftime("%H:%M:%S")
                    print(f"{timestamp:<12} {'N/A':<15} {'No metrics yet':<20}")
                
            except requests.exceptions.ConnectionError:
                timestamp = time.strftime("%H:%M:%S")
                print(f"{timestamp:<12} {'ERROR':<15} {'Cannot connect to metrics':<20}")
            except Exception as e:
                timestamp = time.strftime("%H:%M:%S")
                print(f"{timestamp:<12} {'ERROR':<15} {str(e)[:40]:<20}")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped")


if __name__ == "__main__":
    main()
