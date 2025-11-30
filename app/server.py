import os
import pyarrow as pa
import pyarrow.flight as fl
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONN = os.environ.get("DB_CONN", "postgres://demo:demo@db:5432/demo")
FLIGHT_PORT = 8815

def fetch_table():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT sensor_id, ts, value FROM sensor_readings ORDER BY ts;")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return pa.table({})

    cols = rows[0].keys()
    data = {col: [r[col] for r in rows] for col in cols}
    return pa.Table.from_pydict(data)

class SensorFlightServer(fl.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=FLIGHT_PORT):
        location = fl.Location.for_grpc_tcp(host, port)
        super().__init__(location)
        self._location = location
        self._descriptor = fl.FlightDescriptor.for_path(b"sensor_readings")

    def _get_table(self):
        try:
            return fetch_table()
        except psycopg2.OperationalError:
            return pa.table({})

    def list_flights(self, context, criteria):
        tbl = self._get_table()
        return [fl.FlightInfo(
            schema=tbl.schema,
            descriptor=self._descriptor,
            endpoints=[fl.FlightEndpoint(b"sensor_ticket", [self._location])],
            total_records=tbl.num_rows,
            total_bytes=0
        )]

    def get_flight_info(self, context, descriptor):
        return self.list_flights(context, None)[0]

    def do_get(self, context, ticket):
        tbl = self._get_table()
        return fl.RecordBatchStream(tbl)

def run_server():
    server = SensorFlightServer()
    print(f"Starting Flight server on grpc://0.0.0.0:{FLIGHT_PORT}")
    server.serve()

if __name__ == "__main__":
    run_server()
