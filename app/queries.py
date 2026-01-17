import os
import pyarrow as pa
import psycopg2
from psycopg2.extras import RealDictCursor

DB_CONN = os.environ.get("DB_CONN", "postgres://demo:demo@db:5432/demo")


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
        JOIN "user" d ON t.driver_id = d.id
        JOIN vehicle v ON t.vehicle_id = v.id
        JOIN vehicle_type vt ON v.vehicle_type_id = vt.id
        JOIN location sl ON t.start_location_id = sl.id
        JOIN location el ON t.end_location_id = el.id
        LEFT JOIN (
            SELECT trip_id, COUNT(*) AS passenger_count
            FROM trip_participant
            GROUP BY trip_id
        ) p ON p.trip_id = t.id
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
