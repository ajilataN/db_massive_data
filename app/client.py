import os
import pyarrow.flight as fl
import pyarrow.parquet as pq
from pathlib import Path
import time
import csv

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://flight-server:8815")
OUTPUT_DIR = Path("/app/output")


def fetch_once(descriptor, parquet_filename: str | None = None, write_parquet: bool = False):
    """
    Perform a single Flight do_get call.
    Write the result to a Parquet file from the first run.

    Returns:
        rows (int), duration_ms (float)
    """
    client = fl.FlightClient(FLIGHT_URI)
    info = client.get_flight_info(descriptor)

    start = time.time()
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    duration_ms = (time.time() - start) * 1000.0

    rows = table.num_rows

    if write_parquet and parquet_filename is not None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, OUTPUT_DIR / parquet_filename)
        print(f"Wrote {OUTPUT_DIR / parquet_filename}")

    return rows, duration_ms


def benchmark_query(label: str, descriptor, parquet_filename: str | None = None, runs: int = 10):
    """
    Run a query multiple times, compute timing stats, and write Parquet once.

    Returns:
        dict with stats: {label, rows, runs, avg_ms, min_ms, max_ms}
    """
    print(f"Benchmarking {label} ({runs} runs)...")

    durations = []
    rows_seen = None

    for i in range(runs):
        write_parquet = (i == 0 and parquet_filename is not None)
        rows, duration_ms = fetch_once(
            descriptor,
            parquet_filename=parquet_filename,
            write_parquet=write_parquet
        )

        if rows_seen is None:
            rows_seen = rows
        elif rows_seen != rows:
            print(f"WARNING: row count changed between runs for {label}: {rows_seen} -> {rows}")

        durations.append(duration_ms)
        print(f"  run {i+1}/{runs}: {rows} rows in {duration_ms:.1f} ms")

    avg_ms = sum(durations) / len(durations)
    min_ms = min(durations)
    max_ms = max(durations)

    print(f"==> {label}: {rows_seen} rows | avg {avg_ms:.1f} ms | min {min_ms:.1f} ms | max {max_ms:.1f} ms\n")

    return {
        "label": label,
        "rows": rows_seen,
        "runs": runs,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "max_ms": max_ms,
    }


def write_stats_csv(stats_list):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stats_path = OUTPUT_DIR / "benchmarks.csv"

    with stats_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query_label", "rows", "runs", "avg_ms", "min_ms", "max_ms"])
        for s in stats_list:
            writer.writerow([
                s["label"],
                s["rows"],
                s["runs"],
                f"{s['avg_ms']:.3f}",
                f"{s['min_ms']:.3f}",
                f"{s['max_ms']:.3f}",
            ])

    print(f"Saved benchmark stats to {stats_path}")


def main():
    client = fl.FlightClient(FLIGHT_URI)
    stats = []

    # Trips Overview
    print(">> Trips Overview")
    # limit = 1000
    desc_trips_limit = fl.FlightDescriptor.for_path(b"trips_overview", b"1000")
    stats.append(
        benchmark_query(
            label="trips_overview_limit_1000",
            descriptor=desc_trips_limit,
            parquet_filename="trips_overview_limit_1000.parquet",
            runs=10,
        )
    )
    # no limit
    desc_trips_all = fl.FlightDescriptor.for_path(b"trips_overview")
    stats.append(
        benchmark_query(
            label="trips_overview_full",
            descriptor=desc_trips_all,
            parquet_filename="trips_overview_full.parquet",
            runs=10,
        )
    )

    # User History
    print(">> User History (user 1)")
    # limit = 500
    desc_user_limit = fl.FlightDescriptor.for_path(b"user_history", b"1", b"500")
    stats.append(
        benchmark_query(
            label="user_1_history_limit_500",
            descriptor=desc_user_limit,
            parquet_filename="user_1_history_limit_500.parquet",
            runs=10,
        )
    )
    # no limit
    desc_user_all = fl.FlightDescriptor.for_path(b"user_history", b"1")
    stats.append(
        benchmark_query(
            label="user_1_history_full",
            descriptor=desc_user_all,
            parquet_filename="user_1_history_full.parquet",
            runs=10,
        )
    )

    # Company Stats
    print(">> Company Stats")
    # limit = 365
    desc_company_limit = fl.FlightDescriptor.for_path(b"company_stats", b"1", b"365")
    stats.append(
        benchmark_query(
            label="company_1_stats_limit_365",
            descriptor=desc_company_limit,
            parquet_filename="company_1_stats_limit_365.parquet",
            runs=10,
        )
    )
    # no limit
    desc_company_all = fl.FlightDescriptor.for_path(b"company_stats", b"1")
    stats.append(
        benchmark_query(
            label="company_1_stats_full",
            descriptor=desc_company_all,
            parquet_filename="company_1_stats_full.parquet",
            runs=10,
        )
    )
    write_stats_csv(stats)


if __name__ == "__main__":
    main()
