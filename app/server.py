import os
import pyarrow as pa
import pyarrow.flight as fl
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONN = os.environ.get("DB_CONN", "postgres://demo:demo@db:5432/demo")
FLIGHT_PORT = 8815


def fetch_trips_overview():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = """
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
        ORDER BY t.start_time DESC;
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return pa.table({})

    cols = rows[0].keys()
    data = {col: [r[col] for r in rows] for col in cols}
    return pa.Table.from_pydict(data)
class CommuteFlightServer(fl.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=FLIGHT_PORT):
        location = fl.Location.for_grpc_tcp(host, port)
        super().__init__(location)
        self._location = location
        self._descriptor = fl.FlightDescriptor.for_path(b"trips_overview")

    def _get_table(self):
        try:
            return fetch_trips_overview()
        except psycopg2.OperationalError:
            return pa.table({})

    def list_flights(self, context, criteria):
        tbl = self._get_table()
        return [fl.FlightInfo(
            schema=tbl.schema,
            descriptor=self._descriptor,
            endpoints=[fl.FlightEndpoint(b"trips_ticket", [self._location])],
            total_records=tbl.num_rows,
            total_bytes=0
        )]

    def get_flight_info(self, context, descriptor):
        return self.list_flights(context, None)[0]

    def do_get(self, context, ticket):
        tbl = self._get_table()
        return fl.RecordBatchStream(tbl)

def run_server():
    server = CommuteFlightServer()
    print(f"Starting Flight server on grpc://0.0.0.0:{FLIGHT_PORT}")
    server.serve()

if __name__ == "__main__":
    run_server()
