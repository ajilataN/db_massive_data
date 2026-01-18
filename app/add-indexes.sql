-- ============================================================
-- Indexes for Arrow Flight benchmark workloads (PostgreSQL)
-- Schema: company/location/user/vehicle/trip/trip_participant
-- ============================================================

BEGIN;

-- ----------------------------
-- TRIP: support ordering + joins + filters
-- ----------------------------

-- ORDER BY t.start_time DESC (trips_overview, user_history)
CREATE INDEX IF NOT EXISTS idx_trip_start_time_desc
ON trip (start_time DESC);

-- WHERE t.company_id = ? AND GROUP BY day(start_time) (company_stats)
-- Also useful for ordering within a company
CREATE INDEX IF NOT EXISTS idx_trip_company_start_time_desc
ON trip (company_id, start_time DESC);

-- Join support (often redundant if FK referenced tables are indexed,
-- but indexing the FK columns helps joining from trip -> dimension tables)
CREATE INDEX IF NOT EXISTS idx_trip_driver_id
ON trip (driver_id);

CREATE INDEX IF NOT EXISTS idx_trip_vehicle_id
ON trip (vehicle_id);

CREATE INDEX IF NOT EXISTS idx_trip_start_location_id
ON trip (start_location_id);

CREATE INDEX IF NOT EXISTS idx_trip_end_location_id
ON trip (end_location_id);

-- Optional: if you often query by status later (not in current benchmarks)
-- CREATE INDEX IF NOT EXISTS idx_trip_status ON trip (status);


-- ----------------------------
-- TRIP_PARTICIPANT: support user_history and passenger counts
-- ----------------------------

-- Critical for user_history: WHERE tp.user_id = ? then JOIN tp.trip_id
-- This composite index matches the filter+join pattern.
CREATE INDEX IF NOT EXISTS idx_tp_user_trip
ON trip_participant (user_id, trip_id);

-- Critical for passenger counts aggregation: GROUP BY trip_id
CREATE INDEX IF NOT EXISTS idx_tp_trip_id
ON trip_participant (trip_id);

-- Optional: if you later filter by participant status
-- CREATE INDEX IF NOT EXISTS idx_tp_status ON trip_participant (status);


-- ----------------------------
-- USER: support driver id pool selection
-- ----------------------------

-- Used by your ID pool query for drivers:
-- SELECT id FROM "user" WHERE has_drivers_license = TRUE LIMIT ...
-- Partial index is perfect here.
CREATE INDEX IF NOT EXISTS idx_user_has_drivers_license_true
ON "user" (id)
WHERE has_drivers_license = TRUE;


-- ----------------------------
-- LOCATION: support type-based id pools
-- ----------------------------

-- Used by ID pool queries:
-- SELECT id FROM location WHERE type = 'OFFICE'/'HOME'/'PICKUP_POINT' LIMIT ...
CREATE INDEX IF NOT EXISTS idx_location_type_id
ON location (type, id);


-- ----------------------------
-- VEHICLE: support joins from trip -> vehicle
-- ----------------------------

-- trip.vehicle_id joins to vehicle.id (already PK on vehicle.id)
-- but sometimes you may filter by company later; harmless and potentially useful:
CREATE INDEX IF NOT EXISTS idx_vehicle_company_id
ON vehicle (company_id);

-- vehicle.vehicle_type_id joins to vehicle_type.id (PK exists on vehicle_type.id)
-- No extra index needed for current workload.


-- ----------------------------
-- Refresh planner statistics
-- ----------------------------
ANALYZE;

COMMIT;
