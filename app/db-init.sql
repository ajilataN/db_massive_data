CREATE TABLE sensor_readings (
    id SERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

INSERT INTO sensor_readings (sensor_id, ts, value)
SELECT
    'sensor-' || (1 + (random() * 3)::int),
    NOW() - (interval '1 hour' * (random() * 10)),
    random() * 100
FROM generate_series(1, 1000);
