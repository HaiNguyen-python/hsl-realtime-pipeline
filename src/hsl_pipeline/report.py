"""Run the production transformations on the synthetic sample and chart the result.

Writes docs/img/delay_windows.png and docs/results.md. Uses the same ``parse_events``
and ``delay_windows`` functions as the streaming job (Spark runs them in batch mode here:
the watermark is ignored and de-duplication works like ``dropDuplicates``).

Usage: python -m hsl_pipeline.report   (needs `pip install ".[report]"`)
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from .router import MessageRouter
from .sample import generate

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
BLUE, MUTED = "#2a78d6", "#b8b7b1"
HIGHLIGHT_ROUTE = "1004"


def run_sample(minutes: int = 30, window: str = "2 minutes"):
    from pyspark.sql import SparkSession

    from .streaming_job import delay_windows, foreach_batch, parse_events

    # The very-late burst is left out here: it exists to test watermark behaviour,
    # and would show up as a fake spike on one line in this chart.
    records = generate(minutes=minutes, late_burst=False)
    valid: list[str] = []
    router = MessageRouter(lambda t, k, v: valid.append(v) if t == "main" else None, "main", "dlq")
    for r in records:
        router.handle(r["topic"], r["payload"])

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("hsl-report")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # A real streaming query (file source, run once) so the watermark and de-duplication
    # behave exactly as in production. The sink keeps the latest row per window, like the
    # Postgres upsert does.
    latest: dict[tuple, tuple] = {}

    def sink(rows: list[tuple], batch_id: int) -> None:
        for r in rows:
            latest[(r[0], r[2], r[3])] = r

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src"
        src.mkdir()
        (src / "events.json").write_text("\n".join(valid) + "\n")
        query = (
            delay_windows(parse_events(spark.readStream.text(str(src))), "2 minutes", window)
            .writeStream.outputMode("update")
            .foreachBatch(foreach_batch(sink))
            .option("checkpointLocation", str(Path(tmp) / "chk"))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
    spark.stop()

    import pandas as pd

    cols = [
        "window_start",
        "window_end",
        "mode",
        "route",
        "line",
        "avg_delay_s",
        "p90_delay_s",
        "max_delay_s",
        "vehicles",
        "events",
    ]
    windows = pd.DataFrame(list(latest.values()), columns=cols)
    for col in ("window_start", "window_end"):
        windows[col] = pd.to_datetime(windows[col], utc=True).dt.tz_convert("Europe/Helsinki")
    stats = {
        "raw_messages": len(records),
        "valid": router.stats["valid"],
        "dead_letter": router.stats["dead_letter"],
        "unique_events": len({json.loads(v)["event_id"] for v in valid}),
        "counted_events": int(windows["events"].sum()),
    }
    return windows.sort_values("window_start"), stats


def plot(windows, path: Path) -> None:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=150)
    others = windows[windows["route"] != HIGHLIGHT_ROUTE]
    for _route, g in others.groupby("route"):
        ax.plot(g["window_start"], g["avg_delay_s"], color=MUTED, linewidth=1.2)
    jam = windows[windows["route"] == HIGHLIGHT_ROUTE]
    ax.plot(
        jam["window_start"], jam["avg_delay_s"], color=BLUE, linewidth=2.5, marker="o", markersize=4
    )

    first, last = jam["avg_delay_s"].iloc[0], jam["avg_delay_s"].iloc[-1]
    spread = others.groupby("route")["avg_delay_s"].agg(["min", "max"])
    lo, hi = spread["min"].min(), spread["max"].max()
    ax.set_facecolor(SURFACE)
    fig.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.set_title(
        f"Route {HIGHLIGHT_ROUTE} drifts from {first:.0f} s to {last:.0f} s late; "
        f"other lines stay between {lo:.0f} and {hi:.0f} s",
        loc="left",
        fontsize=12.5,
        fontweight="bold",
        color=INK,
        pad=24,
    )
    ax.text(
        0,
        1.02,
        "Average delay per line, 2-minute windows, seconds late, Helsinki time. "
        "Synthetic sample, 30 trams, run through the Spark job's transformations.",
        transform=ax.transAxes,
        fontsize=9.5,
        color=INK_2,
    )
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz="Europe/Helsinki"))
    ax.axhline(0, color=INK_2, linewidth=0.8)
    end = jam.iloc[-1]
    ax.annotate(
        f"Route {HIGHLIGHT_ROUTE}",
        (end["window_start"], end["avg_delay_s"]),
        xytext=(8, 0),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=INK,
    )
    last_other = others[others["window_start"] == others["window_start"].max()]
    ax.annotate(
        "9 other lines",
        (last_other["window_start"].iloc[0], last_other["avg_delay_s"].median()),
        xytext=(8, 0),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=INK_2,
    )
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def results_markdown(stats: dict) -> str:
    dropped = stats["valid"] - stats["unique_events"]
    return "\n".join(
        [
            "Synthetic HFP sample (30 minutes, 30 trams), run through the same validation and "
            "Spark transformations as production:",
            "",
            "| Step | Count |",
            "| --- | --- |",
            f"| Raw MQTT messages | {stats['raw_messages']:,} |",
            f"| Sent to dead-letter topic (bad JSON, GPS errors, missing fields) "
            f"| {stats['dead_letter']:,} |",
            f"| Valid events | {stats['valid']:,} |",
            f"| Duplicate deliveries removed | {dropped:,} |",
            f"| Events counted in delay windows | {stats['counted_events']:,} |",
        ]
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="hsl-report")
    parser.add_argument("--out", type=Path, default=Path("docs"))
    args = parser.parse_args(argv)
    windows, stats = run_sample()
    plot(windows, args.out / "img" / "delay_windows.png")
    text = results_markdown(stats)
    (args.out / "results.md").write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
