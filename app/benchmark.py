import time
import csv
from pathlib import Path
import pyarrow.flight as fl
import pyarrow.parquet as pq


def fetch_once(
    client: fl.FlightClient,
    descriptor: fl.FlightDescriptor,
    output_dir: Path,
    parquet_filename: str | None = None,
    write_parquet: bool = False,
):
    """
    Perform a single Flight do_get call.
    Optionally write the result to a Parquet file.

    Returns:
        rows (int), duration_ms (float), parquet_bytes (int | None)
    """
    info = client.get_flight_info(descriptor)

    start = time.time()
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    duration_ms = (time.time() - start) * 1000.0

    parquet_bytes = None
    if write_parquet and parquet_filename is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / parquet_filename
        pq.write_table(table, out_path)
        parquet_bytes = out_path.stat().st_size

    return table.num_rows, duration_ms, parquet_bytes


def benchmark_query(
    client: fl.FlightClient,
    label: str,
    descriptor: fl.FlightDescriptor,
    output_dir: Path,
    parquet_filename: str | None = None,
    runs: int = 10,
    warmup: int = 1,
    verbose: bool = True,
):
    """
    Run a query multiple times, compute timing stats, and write Parquet once.
    Warmup runs are not included in the stats.

    Returns:
        dict with stats:
        {label, rows, runs, warmup, avg_ms, min_ms, max_ms, parquet_bytes}
    """
    if verbose:
        print(f"Benchmarking {label} ({runs} runs, warmup={warmup})...")

    # warmup
    for i in range(warmup):
        rows, dur, _ = fetch_once(client, descriptor, output_dir, write_parquet=False)
        if verbose:
            print(f"  warmup {i+1}/{warmup}: {rows} rows in {dur:.1f} ms")

    durations = []
    rows_seen = None
    parquet_bytes = None

    for i in range(runs):
        write_parquet = (i == 0 and parquet_filename is not None)
        rows, dur, pb = fetch_once(
            client,
            descriptor,
            output_dir,
            parquet_filename=parquet_filename,
            write_parquet=write_parquet,
        )

        if rows_seen is None:
            rows_seen = rows
        elif rows_seen != rows and verbose:
            print(f"WARNING: row count changed for {label}: {rows_seen} -> {rows}")

        if parquet_bytes is None and pb is not None:
            parquet_bytes = pb

        durations.append(dur)
        if verbose:
            print(f"  run {i+1}/{runs}: {rows} rows in {dur:.1f} ms")

    avg_ms = sum(durations) / len(durations)
    min_ms = min(durations)
    max_ms = max(durations)

    if verbose:
        print(
            f"==> {label}: {rows_seen} rows | avg {avg_ms:.1f} ms | "
            f"min {min_ms:.1f} ms | max {max_ms:.1f} ms\n"
        )

    return {
        "label": label,
        "rows": rows_seen,
        "runs": runs,
        "warmup": warmup,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "max_ms": max_ms,
        "parquet_bytes": parquet_bytes,
    }


def write_stats_csv(output_dir: Path, stats_list, filename: str = "benchmarks.csv"):
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query_label", "rows", "runs", "warmup", "avg_ms", "min_ms", "max_ms", "parquet_bytes"])
        for s in stats_list:
            writer.writerow([
                s["label"],
                s["rows"],
                s["runs"],
                s["warmup"],
                f"{s['avg_ms']:.3f}",
                f"{s['min_ms']:.3f}",
                f"{s['max_ms']:.3f}",
                s["parquet_bytes"] if s["parquet_bytes"] is not None else "",
            ])

    print(f"Saved benchmark stats to {path}")
