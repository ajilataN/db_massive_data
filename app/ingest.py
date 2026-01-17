import time
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as fl
import random
import datetime


def _fetch_id_list(client: fl.FlightClient, descriptor: fl.FlightDescriptor) -> list[int]:
    info = client.get_flight_info(descriptor)
    reader = client.do_get(info.endpoints[0].ticket)
    tbl = reader.read_all()
    if tbl.num_rows == 0:
        return []
    return [int(x) for x in tbl.column(0).to_pylist() if x is not None]


def fetch_pools(client: fl.FlightClient, pool_size: int = 5000) -> dict:
    pools = {
        "vehicle_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_vehicle", str(pool_size).encode())),
        "driver_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_driver", str(pool_size).encode())),
        "user_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_user", str(pool_size).encode())),
        "home_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_location_home", str(pool_size).encode())),
        "office_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_location_office", str(pool_size).encode())),
        "pickup_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_location_pickup", str(pool_size).encode())),
        "trip_ids": _fetch_id_list(client, fl.FlightDescriptor.for_path(b"ids_trip", str(pool_size).encode())),
    }

    for k, v in pools.items():
        if not v:
            raise RuntimeError(f"ID pool '{k}' is empty. Check server endpoint or database data.")

    return pools


def _chunk_table(table: pa.Table, batch_size: int) -> list[pa.RecordBatch]:
    return table.to_batches(max_chunksize=batch_size)


def do_put_table(
    client: fl.FlightClient,
    descriptor: fl.FlightDescriptor,
    table: pa.Table,
    batch_size: int = 5000,
) -> dict:
    """
    Send an Arrow table to the Flight server using DoPut.
    Returns: {rows, batches, ms}
    """
    batches = _chunk_table(table, batch_size=batch_size)

    start = time.time()
    writer, _ = client.do_put(descriptor, table.schema)
    for b in batches:
        writer.write_batch(b)
    writer.close()
    ms = (time.time() - start) * 1000.0

    return {"rows": table.num_rows, "batches": len(batches), "ms": ms}


def make_trips_table_from_pools(
    pools: dict,
    n: int,
    company_id: int = 1,
    status: str = "COMPLETED",
    trip_duration_minutes: int = 20,
) -> pa.Table:
    """
    Create an Arrow table for DoPut endpoint: insert_trip
    Columns required by server:
      vehicle_id, driver_id, company_id, start_time, end_time,
      start_location_id, end_location_id, status
    Uses pools so we don't hardcode IDs.
    """
    base = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    vehicle_ids = random.choices(pools["vehicle_ids"], k=n)
    driver_ids = random.choices(pools["driver_ids"], k=n)
    company_ids = [company_id] * n

    start_candidates = pools["home_ids"] + pools["pickup_ids"]
    start_loc_ids = random.choices(start_candidates, k=n)
    end_loc_ids = random.choices(pools["office_ids"], k=n)

    start_times = [base - datetime.timedelta(minutes=i) for i in range(n)]
    end_times = [t + datetime.timedelta(minutes=trip_duration_minutes) for t in start_times]
    statuses = [status] * n

    return pa.table({
        "vehicle_id": pa.array(vehicle_ids, type=pa.int64()),
        "driver_id": pa.array(driver_ids, type=pa.int64()),
        "company_id": pa.array(company_ids, type=pa.int64()),
        "start_time": pa.array(start_times, type=pa.timestamp("us", tz="UTC")),
        "end_time": pa.array(end_times, type=pa.timestamp("us", tz="UTC")),
        "start_location_id": pa.array(start_loc_ids, type=pa.int64()),
        "end_location_id": pa.array(end_loc_ids, type=pa.int64()),
        "status": pa.array(statuses, type=pa.string()),
    })


def make_trip_participants_table_from_pools(
    pools: dict,
    n: int,
    status: str = "JOINED",
    spread_across_trips: bool = True,
) -> pa.Table:
    """
    Create an Arrow table for DoPut endpoint: insert_trip_participant
    Columns required by server:
      trip_id, user_id, pickup_location_id, dropoff_location_id, status

    By default, distributes participants across random trips (spread_across_trips=True).
    """
    if spread_across_trips:
        trip_ids = random.choices(pools["trip_ids"], k=n)
    else:
        chosen_trip = random.choice(pools["trip_ids"])
        trip_ids = [chosen_trip] * n

    user_ids = random.choices(pools["user_ids"], k=n)

    pickup_candidates = pools["home_ids"] + pools["pickup_ids"]
    pickup_ids = random.choices(pickup_candidates, k=n)
    dropoff_ids = random.choices(pools["office_ids"], k=n)

    statuses = [status] * n

    return pa.table({
        "trip_id": pa.array(trip_ids, type=pa.int64()),
        "user_id": pa.array(user_ids, type=pa.int64()),
        "pickup_location_id": pa.array(pickup_ids, type=pa.int64()),
        "dropoff_location_id": pa.array(dropoff_ids, type=pa.int64()),
        "status": pa.array(statuses, type=pa.string()),
    })
