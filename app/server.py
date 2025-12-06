import os
import pyarrow as pa
import pyarrow.flight as fl
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONN = os.environ.get("DB_CONN", "postgres://demo:demo@db:5432/demo")
FLIGHT_PORT = 8815
FLIGHTS = ("trips_overview", "user_history", "company_stats")


def run_query(sql: str, params: dict | None = None) -> pa.Table:
    conn = psycopg2.connect(DB_CONN)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params or {})
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return pa.table({})

    cols = rows[0].keys()
    data = {col: [r[col] for r in rows] for col in cols}
    return pa.Table.from_pydict(data)


def fetch_trips_overview(limit: int | None = None) -> pa.Table:
    sql = """
        SELECT
            t.id AS trip_id,
            t.status,
            t.start_time,
            t.end_time,
            d.name AS driver_name,
            d.surname AS driver_surname,
            v.id AS vehicle_id,
            vt.type AS vehicle_type,
            vt.capacity AS vehicle_capacity,
            sl.city AS start_city,
            sl.street AS start_street,
            el.city AS end_city,
            el.street AS end_street,
            COALESCE(p.passenger_count, 0) AS passenger_count
        FROM trip t
        JOIN "user" d
            ON t.driver_id = d.id
        JOIN vehicle v
            ON t.vehicle_id = v.id
        JOIN vehicle_type vt
            ON v.vehicle_type_id = vt.id
        JOIN location sl
            ON t.start_location_id = sl.id
        JOIN location el
            ON t.end_location_id = el.id
        LEFT JOIN (
            SELECT trip_id, COUNT(*) AS passenger_count
            FROM trip_participant
            GROUP BY trip_id
        ) p
            ON p.trip_id = t.id
        ORDER BY t.start_time DESC
    """
    params: dict = {}
    if limit is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = limit

    return run_query(sql, params if params else None)


def fetch_user_history(user_id: int, limit: int | None = None) -> pa.Table:
    sql = """
        SELECT
            t.id AS trip_id,
            t.start_time,
            t.end_time,
            t.status,
            d.name AS driver_name,
            d.surname AS driver_surname,
            CASE WHEN t.driver_id = %(uid)s THEN TRUE ELSE FALSE END AS is_driver
        FROM trip t
        JOIN "user" d ON t.driver_id = d.id
        JOIN trip_participant tp ON tp.trip_id = t.id
        WHERE tp.user_id = %(uid)s
        ORDER BY t.start_time DESC
    """
    params: dict = {"uid": user_id}
    if limit is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = limit

    return run_query(sql, params)


def fetch_company_daily_stats(company_id: int, limit: int | None = None) -> pa.Table:
    sql = """
        SELECT
            date_trunc('day', t.start_time) AS day,
            COUNT(*) AS trips,
            AVG(p_count) AS avg_passengers
        FROM trip t
        LEFT JOIN (
            SELECT trip_id, COUNT(*)::float AS p_count
            FROM trip_participant
            GROUP BY trip_id
        ) p ON p.trip_id = t.id
        WHERE t.company_id = %(cid)s
        GROUP BY day
        ORDER BY day DESC
    """
    params: dict = {"cid": company_id}
    if limit is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = limit

    return run_query(sql, params)


class CommuteFlightServer(fl.FlightServerBase):
    def __init__(self, host: str = "0.0.0.0", port: int = FLIGHT_PORT):
        location = fl.Location.for_grpc_tcp(host, port)
        super().__init__(location)
        self._location = location

    def list_flights(self, context, criteria):
        """
        The client can discover:
          - trips_overview
          - user_history - expects user_id
          - company_stats - expects company_id
        """
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

        except Exception as e:
            print("Error in _get_table_for_descriptor:", e)
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
