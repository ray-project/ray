#!/usr/bin/env python
"""Plot Claude Code request and sticky-session distributions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from aiperf.dataset.memory_map_utils import MemoryMapDatasetClient  # noqa: E402

RC = {
    "figure.facecolor": "#16181d",
    "axes.facecolor": "#20242b",
    "axes.edgecolor": "#c9d1d9",
    "axes.labelcolor": "#f0f6fc",
    "axes.titlecolor": "#f0f6fc",
    "xtick.color": "#d0d7de",
    "ytick.color": "#d0d7de",
    "text.color": "#f0f6fc",
    "font.size": 10,
}
ISL_COLOR = "#6495ED"
OSL_COLOR = "#DDA0DD"
GRID_COLOR = "#6e7681"


def compact_tokens(value: float) -> str:
    """Format a token count for plot labels without hiding useful precision."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:.0f}"


def percentile(values: np.ndarray, q: float) -> float:
    return float(np.quantile(values, q, method="linear"))


def draw_percentile_guides(ax, *, p50: float, p90: float) -> None:
    """Draw the percentile guides; their shared key is in the figure subtitle."""
    for value, linestyle in ((p50, "-"), (p90, ":")):
        ax.axvline(value, color="#f0f6fc", linewidth=1.2, linestyle=linestyle, alpha=0.92)


def load_source_records(data_dir: Path) -> dict[str, dict]:
    """Read the selected Weka roots indexed by source trace ID."""
    paths = sorted(data_dir.glob("*.json"))
    if not paths:
        raise SystemExit(f"no selected Weka JSON roots found under {data_dir}")
    records: dict[str, dict] = {}
    for path in paths:
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"could not parse {path}: {exc}") from exc
        trace_id = record.get("id")
        if not isinstance(trace_id, str) or not trace_id:
            raise SystemExit(f"{path}: missing Weka trace id")
        if trace_id in records:
            raise SystemExit(f"duplicate Weka trace id {trace_id!r}")
        records[trace_id] = record
    return records


def request_record(source: dict, turn: Any, *, session_id: str, turn_position: int) -> dict:
    """Resolve a reconstructed AIPerf turn to its raw Weka ``in``/``out`` row."""
    try:
        record = source[turn.source_trace_id]["requests"][turn.source_outer_idx]
        if turn.source_inner_idx is not None:
            record = record["requests"][turn.source_inner_idx]
        isl, osl = int(record["in"]), int(record["out"])
    except (KeyError, TypeError, IndexError, ValueError) as exc:
        raise SystemExit(
            "could not resolve raw Weka token counts for "
            f"session={session_id!r}, position={turn_position}, "
            f"trace={turn.source_trace_id!r}, outer={turn.source_outer_idx!r}, "
            f"inner={turn.source_inner_idx!r}"
        ) from exc
    if isl <= 0 or osl <= 0:
        raise SystemExit(
            f"invalid Weka token counts for session={session_id!r}, "
            f"position={turn_position}: ISL={isl}, OSL={osl}"
        )
    # The reconstructed replay must retain the source OSL cap.  This catches a
    # cache/source mismatch instead of silently plotting a different corpus.
    if turn.max_tokens is not None and int(turn.max_tokens) != osl:
        raise SystemExit(
            f"OSL mismatch for session={session_id!r}, position={turn_position}: "
            f"cache cap={turn.max_tokens}, source OSL={osl}"
        )
    return {"isl": isl, "osl": osl, "source_trace_id": turn.source_trace_id}


def load_replay_rows(data_dir: Path, mmap_cache: Path) -> pd.DataFrame:
    source = load_source_records(data_dir)
    data_file = mmap_cache / "dataset.dat"
    index_file = mmap_cache / "index.dat"
    if not data_file.is_file() or not index_file.is_file():
        raise SystemExit(
            "AIPerf reconstructed mmap cache not found; expected "
            f"{data_file} and {index_file}. Pass --mmap-cache for the cache "
            "created by this selected corpus."
        )

    rows: list[dict] = []
    with MemoryMapDatasetClient(data_file, index_file) as client:
        for session_id in client.index.conversation_ids:
            conversation = client.get_conversation(session_id)
            for turn_position, turn in enumerate(conversation.turns):
                if turn.no_request:
                    continue
                row = request_record(
                    source, turn, session_id=session_id, turn_position=turn_position
                )
                rows.append(
                    {
                        "session_id": session_id,
                        "turn_position": turn_position,
                        "is_root_session": conversation.is_root,
                        **row,
                    }
                )
    if not rows:
        raise SystemExit("the selected AIPerf replay has no requests")
    frame = pd.DataFrame(rows)
    if frame["session_id"].nunique() < 2:
        raise SystemExit("expected more than one reconstructed sticky session")
    return frame


def draw_histogram(
    ax,
    values: np.ndarray,
    *,
    color: str,
    title: str,
    noun: str,
    stats_loc: str = "upper right",
) -> None:
    values = np.asarray(values, dtype=float)
    p50 = percentile(values, 0.50)
    p90 = percentile(values, 0.90)
    # Equal-width linear bins would hide the long tail; geometric bins retain
    # both the small one-shot sessions and the multi-million-token elephants.
    bins = np.geomspace(values.min(), values.max(), 34)
    ax.hist(values, bins=bins, color=color, edgecolor="#c9d1d9", linewidth=0.35, alpha=0.88)
    draw_percentile_guides(ax, p50=p50, p90=p90)
    if stats_loc not in {"upper left", "upper right"}:
        raise ValueError(f"unsupported stats_loc={stats_loc!r}")
    x = 0.025 if stats_loc == "upper left" else 0.975
    alignment = "left" if stats_loc == "upper left" else "right"
    ax.text(
        x,
        0.94,
        f"p50  {compact_tokens(p50)}\np90  {compact_tokens(p90)}\nmax  {compact_tokens(values.max())}",
        transform=ax.transAxes,
        ha=alignment,
        va="top",
        fontsize=9,
        bbox={"facecolor": "#16181d", "edgecolor": "#6e7681", "alpha": 0.88, "pad": 4},
    )
    ax.set_xscale("log")
    ax.set(title=title, xlabel="Tokens (log scale)", ylabel=f"Number of {noun}")
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.32)
    ax.set_axisbelow(True)


def draw_turn_count(ax, turns_per_session: np.ndarray) -> None:
    """Show how many requests each strict-hash conversation owns."""
    values = np.asarray(turns_per_session, dtype=int)
    p50 = percentile(values, 0.50)
    p90 = percentile(values, 0.90)
    bins = np.arange(values.min() - 0.5, values.max() + 1.5, 1.0)
    ax.hist(values, bins=bins, color="#8ee78c", edgecolor="#c9d1d9", linewidth=0.35, alpha=0.88)
    draw_percentile_guides(ax, p50=p50, p90=p90)
    ax.text(
        0.975,
        0.94,
        f"p50  {p50:.0f}\np90  {p90:.0f}\nmax  {values.max():.0f}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox={"facecolor": "#16181d", "edgecolor": "#6e7681", "alpha": 0.88, "pad": 4},
    )
    ax.set(
        title="Turns per sticky session",
        xlabel="Number of turns in a session",
        ylabel="Number of sessions",
    )
    ax.grid(axis="y", color=GRID_COLOR, alpha=0.32)
    ax.set_axisbelow(True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--mmap-cache", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    if args.out.exists():
        raise SystemExit(f"refusing to overwrite existing plot: {args.out}")
    frame = load_replay_rows(args.data_dir, args.mmap_cache)
    session_totals = (
        frame.groupby("session_id", as_index=False)
        .agg(
            session_isl=("isl", "sum"),
            session_osl=("osl", "sum"),
            turns=("isl", "size"),
        )
        .sort_values("session_id")
    )

    plt.rcParams.update(RC)
    fig = plt.figure(figsize=(18, 10))
    # The top row needs three equal panels.  Spanning two columns per panel and
    # offsetting the bottom pair by one column leaves equal outer margins, so
    # the session distributions are centered rather than left-aligned.
    grid = fig.add_gridspec(2, 6)
    axes = {
        "request_isl": fig.add_subplot(grid[0, 0:2]),
        "request_osl": fig.add_subplot(grid[0, 2:4]),
        "turns": fig.add_subplot(grid[0, 4:6]),
        "session_isl": fig.add_subplot(grid[1, 1:3]),
        "session_osl": fig.add_subplot(grid[1, 3:5]),
    }
    draw_histogram(
        axes["request_isl"],
        frame["isl"].to_numpy(),
        color=ISL_COLOR,
        title="Individual request ISL",
        noun="requests",
        stats_loc="upper left",
    )
    draw_histogram(
        axes["request_osl"],
        frame["osl"].to_numpy(),
        color=OSL_COLOR,
        title="Individual request OSL",
        noun="requests",
    )
    draw_turn_count(axes["turns"], session_totals["turns"].to_numpy())
    draw_histogram(
        axes["session_isl"],
        session_totals["session_isl"].to_numpy(),
        color=ISL_COLOR,
        title="Session lifetime ISL",
        noun="sessions",
    )
    draw_histogram(
        axes["session_osl"],
        session_totals["session_osl"].to_numpy(),
        color=OSL_COLOR,
        title="Session lifetime OSL",
        noun="sessions",
    )

    fig.subplots_adjust(left=0.055, right=0.985, bottom=0.08, top=0.84, hspace=0.37, wspace=0.27)
    fig.suptitle("Weka Claude Code trace and session distributions", fontsize=18, y=0.975)
    fig.text(
        0.5,
        0.915,
        "71 eligible roots at the 120k context cap | 249 AIPerf reconstructed sessions | "
        "Session totals sum every turn in one sticky session; strict consistent hashing routes all of those turns to one replica\n"
        "Solid vertical line = p50 | dotted vertical line = p90",
        ha="center",
        fontsize=11,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"[plot] wrote {args.out}")


if __name__ == "__main__":
    main()
