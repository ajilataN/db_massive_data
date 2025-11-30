import os
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
from pathlib import Path

FLIGHT_URI = os.environ.get("FLIGHT_URI", "grpc://flight-server:8815")

def main():
    client = fl.FlightClient(FLIGHT_URI)
    flights = list(client.list_flights())
    print(f"Found {len(flights)} flights")
    if not flights:
        return
    info = flights[0]
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    print(f"Received {table.num_rows} rows")
    print(table.schema)

    Path("/app/output").mkdir(parents=True, exist_ok=True)
    pq.write_table(table, "/app/output/trips_overview.parquet")
    print("Wrote /app/output/trips_overview.parquet")

if __name__ == "__main__":
    main()
