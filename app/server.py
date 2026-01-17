import os
import pyarrow as pa
import pyarrow.flight as fl
import psycopg2

from queries import (
    fetch_trips_overview,
    fetch_user_history,
    fetch_company_daily_stats,
)

FLIGHT_PORT = int(os.environ.get("FLIGHT_PORT", "8815"))

# “API list” (discoverable endpoints)
FLIGHTS = ("trips_overview", "user_history", "company_stats")


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


def run_server():
    server = CommuteFlightServer()
    print(f"Starting Flight server on grpc://0.0.0.0:{FLIGHT_PORT}")
    server.serve()


if __name__ == "__main__":
    run_server()
