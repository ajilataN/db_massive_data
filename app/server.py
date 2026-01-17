import os
import pyarrow as pa
import pyarrow.flight as fl
import psycopg2

from queries import (
    fetch_trips_overview,
    fetch_user_history,
    fetch_company_daily_stats,
    insert_trips_rows,
    insert_trip_participants_rows,
    fetch_vehicle_ids,
    fetch_driver_ids,
    fetch_user_ids,
    fetch_location_ids,
    fetch_trip_ids
)


FLIGHT_PORT = int(os.environ.get("FLIGHT_PORT", "8815"))

FLIGHTS = (
    "trips_overview", "user_history", "company_stats",
    "ids_vehicle",
    "ids_driver",
    "ids_user",
    "ids_location_home",
    "ids_location_office",
    "ids_location_pickup",
    "ids_trip",
)

class CommuteFlightServer(fl.FlightServerBase):
    def __init__(self, host: str = "0.0.0.0", port: int = FLIGHT_PORT):
        location = fl.Location.for_grpc_tcp(host, port)
        super().__init__(location)
        self._location = location

    def list_flights(self, context, criteria):
        for name in FLIGHTS:
            descriptor = fl.FlightDescriptor.for_path(name.encode())
            tbl = self._get_table_for_descriptor(descriptor)
            yield fl.FlightInfo(
                schema=tbl.schema,
                descriptor=descriptor,
                endpoints=[fl.FlightEndpoint(name.encode(), [self._location])],
                total_records=tbl.num_rows,
                total_bytes=0,
            )

    def _get_table_for_descriptor(self, descriptor: fl.FlightDescriptor) -> pa.Table:
        try:
            parts = [p.decode() for p in (descriptor.path or [])]
            if not parts:
                return pa.table({})

            kind = parts[0]

            if kind == "trips_overview":
                limit = int(parts[1]) if len(parts) > 1 else None
                return fetch_trips_overview(limit)

            if kind == "user_history":
                if len(parts) < 2:
                    print("user_history requires user_id")
                    return pa.table({})
                user_id = int(parts[1])
                limit = int(parts[2]) if len(parts) > 2 else None
                return fetch_user_history(user_id, limit)

            if kind == "company_stats":
                if len(parts) < 2:
                    print("company_stats requires company_id")
                    return pa.table({})
                company_id = int(parts[1])
                limit = int(parts[2]) if len(parts) > 2 else None
                return fetch_company_daily_stats(company_id, limit)

            if kind == "ids_vehicle":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_vehicle_ids(limit)

            if kind == "ids_driver":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_driver_ids(limit)

            if kind == "ids_user":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_user_ids(limit)

            if kind == "ids_location_home":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_location_ids("HOME", limit)

            if kind == "ids_location_office":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_location_ids("OFFICE", limit)

            if kind == "ids_location_pickup":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_location_ids("PICKUP_POINT", limit)

            if kind == "ids_trip":
                limit = int(parts[1]) if len(parts) > 1 else 5000
                return fetch_trip_ids(limit)

            print("Unknown query kind:", kind)
            return pa.table({})

        except (ValueError, IndexError) as e:
            print("Bad descriptor parameters:", e)
            return pa.table({})
        except psycopg2.Error as e:
            print("DB error:", e)
            return pa.table({})
        except Exception as e:
            print("Unexpected error:", e)
            return pa.table({})

    def get_flight_info(self, context, descriptor):
        tbl = self._get_table_for_descriptor(descriptor)
        ticket = fl.Ticket(b"|".join(descriptor.path or [b"unknown"]))
        return fl.FlightInfo(
            schema=tbl.schema,
            descriptor=descriptor,
            endpoints=[fl.FlightEndpoint(ticket, [self._location])],
            total_records=tbl.num_rows,
            total_bytes=0,
        )

    def do_get(self, context, ticket):
        parts = ticket.ticket.split(b"|")
        descriptor = fl.FlightDescriptor.for_path(*parts)
        tbl = self._get_table_for_descriptor(descriptor)
        return fl.RecordBatchStream(tbl)


    def do_put(self, context, descriptor, reader, writer):
        try:
            parts = [p.decode() for p in (descriptor.path or [])]
            if not parts:
                raise ValueError("Missing descriptor path for DoPut")

            kind = parts[0]
            total_inserted = 0

            for chunk in reader:
                batch = chunk.data
                if batch is None:
                    continue

                tbl = pa.Table.from_batches([batch])
                data = tbl.to_pydict()
                n = tbl.num_rows

                if kind == "insert_trip":
                    required = [
                        "vehicle_id", "driver_id", "company_id",
                        "start_time", "end_time",
                        "start_location_id", "end_location_id",
                        "status"
                    ]
                    for col in required:
                        if col not in data:
                            raise ValueError(f"Missing column '{col}' for insert_trip")

                    rows = [
                        (
                            data["vehicle_id"][i],
                            data["driver_id"][i],
                            data["company_id"][i],
                            data["start_time"][i],
                            data["end_time"][i],
                            data["start_location_id"][i],
                            data["end_location_id"][i],
                            data["status"][i],
                        )
                        for i in range(n)
                    ]
                    total_inserted += insert_trips_rows(rows)

                elif kind == "insert_trip_participant":
                    required = [
                        "trip_id", "user_id",
                        "pickup_location_id", "dropoff_location_id",
                        "status"
                    ]
                    for col in required:
                        if col not in data:
                            raise ValueError(f"Missing column '{col}' for insert_trip_participant")

                    rows = [
                        (
                            data["trip_id"][i],
                            data["user_id"][i],
                            data["pickup_location_id"][i],
                            data["dropoff_location_id"][i],
                            data["status"][i],
                        )
                        for i in range(n)
                    ]
                    total_inserted += insert_trip_participants_rows(rows)

                else:
                    raise ValueError(f"Unknown DoPut endpoint: {kind}")

            print(f"DoPut finished: kind={kind}, inserted={total_inserted} rows")

        except Exception as e:
            print("DoPut error:", e)
            raise


def run_server():
    server = CommuteFlightServer()
    print(f"Starting Flight server on grpc://0.0.0.0:{FLIGHT_PORT}")
    server.serve()


if __name__ == "__main__":
    run_server()
