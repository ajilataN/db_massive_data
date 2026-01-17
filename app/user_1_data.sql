INSERT INTO trip (
    vehicle_id,
    driver_id,
    company_id,
    start_time,
    end_time,
    start_location_id,
    end_location_id,
    status
)
SELECT
    (SELECT id FROM vehicle ORDER BY random() LIMIT 1),
    (SELECT id FROM "user" WHERE has_drivers_license ORDER BY random() LIMIT 1),
    1,
    NOW()
      - ((g % 365) * INTERVAL '1 day')
      - ((g % 24) * INTERVAL '1 hour'),
    NOW()
      - ((g % 365) * INTERVAL '1 day')
      - (((g % 24) - 1) * INTERVAL '1 hour'),
    (SELECT id FROM location WHERE type IN ('HOME','PICKUP_POINT') ORDER BY random() LIMIT 1),
    (SELECT id FROM location WHERE type = 'OFFICE' ORDER BY random() LIMIT 1),
    'COMPLETED'
FROM generate_series(1, 10000) AS g;


INSERT INTO trip_participant (
    trip_id,
    user_id,
    pickup_location_id,
    dropoff_location_id,
    status
)
SELECT
    t.id,
    1,
    t.start_location_id,
    t.end_location_id,
    'JOINED'
FROM trip t
WHERE t.company_id = 1
  AND random() < 0.6;


INSERT INTO trip_participant (
    trip_id,
    user_id,
    pickup_location_id,
    dropoff_location_id,
    status
)
SELECT
    t.id,
    (SELECT id FROM "user" ORDER BY random() LIMIT 1),
    t.start_location_id,
    t.end_location_id,
    'JOINED'
FROM trip t
JOIN generate_series(1, 3) g ON TRUE
WHERE random() < 0.4;
