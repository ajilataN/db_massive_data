import os
from pathlib import Path
import pyarrow.flight as fl

from benchmark import benchmark_query, write_stats_csv

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://flight-server:8815")
OUTPUT_DIR = Path("/app/output")


def main():
    client = fl.FlightClient(FLIGHT_URI)

    suite = [
        # Trips overview: 1k, 10k, full
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
            "label": "trips_overview_full",
            "descriptor": fl.FlightDescriptor.for_path(b"trips_overview"),
            "parquet": "trips_overview_full.parquet",
        },

        # User history
        {
            "label": "user_1_history_limit_500",
            "descriptor": fl.FlightDescriptor.for_path(b"user_history", b"1", b"500"),
            "parquet": "user_1_history_limit_500.parquet",
        },
        {
            "label": "user_1_history_full",
            "descriptor": fl.FlightDescriptor.for_path(b"user_history", b"1"),
            "parquet": "user_1_history_full.parquet",
        },

        # Company stats
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

    stats = []

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
