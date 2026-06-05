#!/usr/bin/env python3
# ABOUTME: Converts collapsed stack format (from stackcollapse-perf.pl) to speedscope JSON.
# ABOUTME: Output can be loaded in speedscope UI or analyzed with analyze-pyspy-profile.

"""
Usage:
    collapsed-to-speedscope input.collapsed.txt [-o output.speedscope.json] [--name PROFILE_NAME]

The collapsed stack format is: semicolon;delimited;stack weight
Each unique first frame is treated as a separate thread/profile.

Examples:
    collapsed-to-speedscope perf_gcs_collapsed.txt
    collapsed-to-speedscope perf_gcs_collapsed.txt -o gcs.speedscope.json
    collapsed-to-speedscope perf_gcs_collapsed.txt --name "GCS Server"
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

_OFFSET_RE = re.compile(r"\+0x[0-9a-f]+$")


def _clean_frame(name):
    """Strip hex offsets (+0x...) from a frame name."""
    return _OFFSET_RE.sub("", name)


def parse_collapsed(path):
    """Parse collapsed stack file into list of (stack_frames, weight) tuples."""
    samples = []
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip()
            if not line:
                continue
            # Last space-separated token is the weight
            last_space = line.rfind(" ")
            if last_space == -1:
                print(f"WARNING: skipping malformed line {lineno}", file=sys.stderr)
                continue
            stack_str = line[:last_space]
            try:
                weight = int(line[last_space + 1 :])
            except ValueError:
                try:
                    weight = float(line[last_space + 1 :])
                except ValueError:
                    print(
                        f"WARNING: skipping line {lineno} (bad weight)", file=sys.stderr
                    )
                    continue
            frames = [_clean_frame(f) for f in stack_str.split(";")]
            samples.append((frames, weight))
    return samples


def build_speedscope(samples, profile_name, freq=99):
    """Convert parsed samples to speedscope JSON format.

    Groups samples by their first frame (thread name in perf output)
    into separate profiles.
    """
    # Group by thread (first frame in stack)
    by_thread = defaultdict(list)
    for frames, weight in samples:
        thread = frames[0] if frames else "(unknown)"
        by_thread[thread].append((frames, weight))

    # Build shared frame table
    frame_index = {}
    frame_list = []

    def get_frame_idx(name):
        if name not in frame_index:
            frame_index[name] = len(frame_list)
            frame_list.append({"name": name})
        return frame_index[name]

    # Detect if weights are nanosecond periods (from stackcollapse-perf.pl)
    # vs simple counts. Perf periods are typically >= 1,000,000 (1ms+).
    all_weights = [w for _, w in samples]
    median_weight = sorted(all_weights)[len(all_weights) // 2] if all_weights else 1
    # If median weight > 100,000, assume nanoseconds and convert to seconds.
    if median_weight > 100_000:
        weight_scale = 1e-9
        print(
            f"  Detected nanosecond weights (median={median_weight}), converting to seconds"
        )
    else:
        # Simple counts: each sample represents 1/freq seconds.
        weight_scale = 1.0 / freq
        print(
            f"  Detected count weights (median={median_weight}), using {freq} Hz -> {weight_scale:.4f}s per sample"
        )

    # Build profiles (one per thread)
    profiles = []
    for thread_name, thread_samples in sorted(by_thread.items()):
        profile_samples = []
        profile_weights = []
        for frames, weight in thread_samples:
            indices = [get_frame_idx(f) for f in frames]
            profile_samples.append(indices)
            profile_weights.append(weight * weight_scale)

        name = thread_name
        profiles.append(
            {
                "type": "sampled",
                "name": name,
                "unit": "seconds",
                "startValue": 0,
                "endValue": sum(profile_weights),
                "samples": profile_samples,
                "weights": profile_weights,
            }
        )

    return {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {"frames": frame_list},
        "profiles": profiles,
        "name": profile_name or os.path.basename(sys.argv[0]),
        "exporter": "collapsed-to-speedscope",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Convert collapsed stack format to speedscope JSON"
    )
    parser.add_argument(
        "input", help="Collapsed stack file (semicolon;delimited;stack weight)"
    )
    parser.add_argument(
        "-o", "--output", help="Output path (default: input with .speedscope.json)"
    )
    parser.add_argument("--name", help="Profile name (default: input filename)")
    parser.add_argument(
        "--freq",
        type=int,
        default=99,
        help="Sample frequency in Hz for count-based weights (default: 99)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found", file=sys.stderr)
        sys.exit(1)

    output = args.output
    if not output:
        base = args.input
        for ext in (".collapsed.txt", "_collapsed.txt", ".txt"):
            if base.endswith(ext):
                base = base[: -len(ext)]
                break
        output = base + ".speedscope.json"

    name = args.name or os.path.splitext(os.path.basename(args.input))[0]

    print(f"Reading {args.input}...")
    samples = parse_collapsed(args.input)
    print(f"  {len(samples):,} unique stacks")

    speedscope = build_speedscope(samples, name, args.freq)
    print(f"  {len(speedscope['shared']['frames']):,} unique frames")
    print(f"  {len(speedscope['profiles'])} profiles (threads)")
    for p in speedscope["profiles"]:
        print(f"    - {p['name']}: {len(p['samples']):,} samples")

    with open(output, "w") as f:
        json.dump(speedscope, f)
    size = os.path.getsize(output)
    print(f"Wrote {output} ({size:,} bytes)")


if __name__ == "__main__":
    main()
