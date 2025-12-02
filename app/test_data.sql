INSERT INTO company (name)
SELECT 'Company_' || g
FROM generate_series(3, 22) AS g;


INSERT INTO location (city, postal_code, street, street_no, country, type)
SELECT
  'Ljubljana',
  1000 + (g % 10),
  'Street_' || (g % 50),
  (1 + (g % 120))::text,
  'Slovenia',
  CASE WHEN g % 3 = 0 THEN 'OFFICE' ELSE 'HOME' END
FROM generate_series(7, 506) AS g;


INSERT INTO company_location (company_id, location_id, is_primary)
SELECT
  c.id,
  l.id,
  (row_number() OVER (PARTITION BY c.id ORDER BY random()) = 1) AS is_primary
FROM company c
JOIN LATERAL (
    SELECT id
    FROM location
    ORDER BY random()
    LIMIT 3
) l ON TRUE;


INSERT INTO "user" (name, surname, has_drivers_license, company_id, home_location_id, company_location_id)
SELECT
  'User_' || g,
  'Surname_' || g,
  (g % 2 = 0),
  (SELECT id FROM company ORDER BY random() LIMIT 1),
  (SELECT id FROM location WHERE type = 'HOME' ORDER BY random() LIMIT 1),
  (SELECT id FROM company_location ORDER BY random() LIMIT 1)
FROM generate_series(5, 10004) AS g;


INSERT INTO vehicle_type (type, capacity)
VALUES ('CAR', 4), ('VAN', 7)
ON CONFLICT DO NOTHING;

INSERT INTO vehicle (owned_by_employee, current_location_id, company_id, employee_id, vehicle_type_id)
SELECT
  (g % 3 = 0) AS owned_by_employee,
  (SELECT id FROM location ORDER BY random() LIMIT 1),
  (SELECT id FROM company ORDER BY random() LIMIT 1),
  CASE
    WHEN g % 3 = 0 THEN
      (SELECT id FROM "user" WHERE has_drivers_license ORDER BY random() LIMIT 1)
    ELSE NULL
  END,
  (SELECT id FROM vehicle_type ORDER BY random() LIMIT 1)
FROM generate_series(4, 203) AS g;


INSERT INTO trip (vehicle_id, driver_id, company_id, start_time, end_time, start_location_id, end_location_id, status)
SELECT
  (SELECT id FROM vehicle ORDER BY random() LIMIT 1) AS vehicle_id,
  (SELECT id FROM "user" WHERE has_drivers_license ORDER BY random() LIMIT 1) AS driver_id,
  (SELECT id FROM company ORDER BY random() LIMIT 1) AS company_id,
  NOW() - ((g % 120) * INTERVAL '1 day') - ((g % 24) * INTERVAL '1 hour') AS start_time,
  NOW() - ((g % 120) * INTERVAL '1 day') - (((g % 24) - 1) * INTERVAL '1 hour') AS end_time,
  (SELECT id FROM location WHERE type = 'HOME' ORDER BY random() LIMIT 1) AS start_location_id,
  (SELECT id FROM location WHERE type = 'OFFICE' ORDER BY random() LIMIT 1) AS end_location_id,
  CASE
    WHEN g % 10 = 0 THEN 'CANCELLED'
    ELSE 'COMPLETED'
  END
FROM generate_series(4, 20003) AS g;


INSERT INTO trip_participant (trip_id, user_id, pickup_location_id, dropoff_location_id, status)
SELECT
  t.id AS trip_id,
  (SELECT id FROM "user" ORDER BY random() LIMIT 1),
  t.start_location_id,
  t.end_location_id,
  'JOINED'
FROM trip t
JOIN generate_series(1, 4) AS g ON TRUE
WHERE random() < 0.8;
