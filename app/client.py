import os
from pathlib import Path
import pyarrow.flight as fl

from benchmark import benchmark_query, write_stats_csv
from ingest import (
    do_put_table,
    fetch_pools,
    make_trips_table_from_pools,
    make_trip_participants_table_from_pools,
)

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://flight-server:8815")
OUTPUT_DIR = Path("/app/output")


def _as_single_run_stat(label: str, rows: int, ms: float) -> dict:
    """Helper: represent one-off timings in the same schema as query benchmarks."""
    return {
        "label": label,
        "rows": rows,
        "runs": 1,
        "warmup": 0,
        "avg_ms": ms,
        "min_ms": ms,
        "max_ms": ms,
        "parquet_bytes": None,
    }


def main():
    client = fl.FlightClient(FLIGHT_URI)

    pools = fetch_pools(client, pool_size=5000)

    stats = []  # <-- now collects both inserts and query benchmarks

    # --- DoPut: insert trips
    trips_table = make_trips_table_from_pools(pools, n=1000, company_id=1)
    put_trips = do_put_table(
        client,
        descriptor=fl.FlightDescriptor.for_path(b"insert_trip"),
        table=trips_table,
        batch_size=5000,
    )
    print(f"Inserted trips via DoPut: {put_trips['rows']} rows in {put_trips['ms']:.1f} ms")
    stats.append(_as_single_run_stat("insert_trip", put_trips["rows"], put_trips["ms"]))

    # --- DoPut: insert participants
    participants_table = make_trip_participants_table_from_pools(
        pools,
        n=5000,
        spread_across_trips=True,
    )
    put_parts = do_put_table(
        client,
        descriptor=fl.FlightDescriptor.for_path(b"insert_trip_participant"),
        table=participants_table,
        batch_size=5000,
    )
    print(f"Inserted participants via DoPut: {put_parts['rows']} rows in {put_parts['ms']:.1f} ms")
    stats.append(_as_single_run_stat("insert_trip_participant", put_parts["rows"], put_parts["ms"]))

    suite = [
    # --- Trips overview
    {
        "label": "trips_overview_limit_1000",
        "descriptor": fl.FlightDescriptor.for_path(b"trips_overview", b"1000"),
        "parquet": "trips_overview_limit_1000.parquet",
    },
    {
        "label": "trips_overview_limit_10000",
        "descriptor": fl.FlightDescriptor.for_path(b"trips_overview", b"10000"),
        "parquet": "trips_overview_limit_10000.parquet",
    },
    {
        "label": "trips_overview_limit_100000",
        "descriptor": fl.FlightDescriptor.for_path(b"trips_overview", b"100000"),
        "parquet": "trips_overview_limit_100000.parquet",
    },
    {
        "label": "trips_overview_limit_400000",
        "descriptor": fl.FlightDescriptor.for_path(b"trips_overview", b"400000"),
        "parquet": "trips_overview_limit_400000.parquet",
    },

    # --- User history
    {
        "label": "user_1_history_limit_500",
        "descriptor": fl.FlightDescriptor.for_path(b"user_history", b"1", b"500"),
        "parquet": "user_1_history_limit_500.parquet",
    },
    {
        "label": "user_1_history_limit_1000",
        "descriptor": fl.FlightDescriptor.for_path(b"user_history", b"1", b"1000"),
        "parquet": "user_1_history_limit_1000.parquet",
    },
    {
        "label": "user_1_history_limit_5000",
        "descriptor": fl.FlightDescriptor.for_path(b"user_history", b"1", b"5000"),
        "parquet": "user_1_history_limit_5000.parquet",
    },

    # --- Company stats (unchanged)
    {
        "label": "company_1_stats_limit_365",
        "descriptor": fl.FlightDescriptor.for_path(b"company_stats", b"1", b"365"),
        "parquet": "company_1_stats_limit_365.parquet",
    },
    {
        "label": "company_1_stats_full",
        "descriptor": fl.FlightDescriptor.for_path(b"company_stats", b"1"),
        "parquet": "company_1_stats_full.parquet",
    },
    ]

    for item in suite:
        stats.append(
            benchmark_query(
                client=client,
                label=item["label"],
                descriptor=item["descriptor"],
                output_dir=OUTPUT_DIR,
                parquet_filename=item["parquet"],
                runs=10,
                warmup=1,
                verbose=True,
            )
        )

    write_stats_csv(OUTPUT_DIR, stats)


if __name__ == "__main__":
    main()
