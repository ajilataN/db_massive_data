-- clean for re-run
DROP TABLE IF EXISTS trip_participant;
DROP TABLE IF EXISTS trip;
DROP TABLE IF EXISTS vehicle;
DROP TABLE IF EXISTS vehicle_type;
DROP TABLE IF EXISTS "user";
DROP TABLE IF EXISTS company_location;
DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS location;

CREATE TABLE company (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL
);

CREATE TABLE location (
    id          SERIAL PRIMARY KEY,
    city        TEXT NOT NULL,
    postal_code INT NOT NULL,
    street      TEXT NOT NULL,
    street_no   TEXT NOT NULL,
    country     TEXT NOT NULL,
    type        TEXT NOT NULL  -- 'HOME', 'OFFICE', 'PICKUP_POINT'
);

CREATE TABLE company_location (
    id              SERIAL PRIMARY KEY,
    company_id      INT NOT NULL REFERENCES company(id),
    location_id     INT NOT NULL REFERENCES location(id),
    is_primary      BOOLEAN DEFAULT FALSE
);

CREATE TABLE "user" (
    id                      SERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    surname                 TEXT NOT NULL,
    has_drivers_license     BOOLEAN NOT NULL DEFAULT FALSE,
    company_id              INT NOT NULL REFERENCES company(id),
    home_location_id        INT REFERENCES location(id),
    company_location_id     INT REFERENCES company_location(id)
);

CREATE TABLE vehicle_type (
    id          SERIAL PRIMARY KEY,
    type        TEXT NOT NULL,
    capacity    INT NOT NULL
);

CREATE TABLE vehicle (
    id                      SERIAL PRIMARY KEY,
    owned_by_employee       BOOLEAN NOT NULL DEFAULT FALSE,
    current_location_id     INT REFERENCES location(id),
    company_id              INT REFERENCES company(id),
    employee_id             INT REFERENCES "user"(id),
    vehicle_type_id         INT NOT NULL REFERENCES vehicle_type(id)
);

CREATE TABLE trip (
    id                  SERIAL PRIMARY KEY,
    vehicle_id          INT NOT NULL REFERENCES vehicle(id),
    driver_id           INT NOT NULL REFERENCES "user"(id),
    company_id          INT NOT NULL REFERENCES company(id),
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,
    start_location_id   INT NOT NULL REFERENCES location(id),
    end_location_id     INT NOT NULL REFERENCES location(id),
    status              TEXT NOT NULL  -- 'PLANNED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED'
);

CREATE TABLE trip_participant (
    id                      SERIAL PRIMARY KEY,
    trip_id                 INT NOT NULL REFERENCES trip(id),
    user_id                 INT NOT NULL REFERENCES "user"(id),
    pickup_location_id      INT NOT NULL REFERENCES location(id),
    dropoff_location_id     INT NOT NULL REFERENCES location(id),
    status                  TEXT NOT NULL  -- 'JOINED', 'CANCELLED', 'NO_SHOW'
);


-- mock data for testing purposes

INSERT INTO company (name)
VALUES ('TechCorp'), ('DataSolutions');

INSERT INTO location (city, postal_code, street, street_no, country, type)
VALUES
  ('Ljubljana', 1000, 'Dunajska cesta', '1', 'Slovenia', 'OFFICE'),
  ('Ljubljana', 1000, 'Celovška cesta', '25', 'Slovenia', 'OFFICE'),
  ('Ljubljana', 1000, 'Trzaska cesta', '100', 'Slovenia', 'HOME'),
  ('Ljubljana', 1000, 'Slovenska cesta', '50', 'Slovenia', 'HOME'),
  ('Ljubljana', 1000, 'Dunajska cesta', '50', 'Slovenia', 'HOME'),
  ('Ljubljana', 1000, 'BTC', '1', 'Slovenia', 'PICKUP_POINT');

INSERT INTO company_location (company_id, location_id, is_primary)
VALUES
  (1, 1, TRUE),
  (1, 2, FALSE),
  (2, 1, TRUE);

INSERT INTO "user" (name, surname, has_drivers_license, company_id, home_location_id, company_location_id)
VALUES
  ('Ana',  'Novak',  TRUE,  1, 3, 1),
  ('Boris','Kovac',  TRUE,  1, 4, 1),
  ('Cene', 'Horvat', FALSE, 1, 5, 1),
  ('Dora', 'Kralj',  TRUE,  2, 5, 3);

INSERT INTO vehicle_type (type, capacity)
VALUES
  ('CAR', 4),
  ('VAN', 7);

INSERT INTO vehicle (owned_by_employee, current_location_id, company_id, employee_id, vehicle_type_id)
VALUES
  (FALSE, 1, 1, NULL, 1),
  (TRUE,  3, 1, 1,    1),
  (TRUE,  4, 1, 2,    1);

INSERT INTO trip (vehicle_id, driver_id, company_id, start_time, end_time, start_location_id, end_location_id, status)
VALUES
  (1, 1, 1, NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2.5 hours', 3, 1, 'COMPLETED'),
  (2, 2, 1, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1.5 hours', 4, 1, 'COMPLETED'),
  (3, 2, 1, NOW() - INTERVAL '1 hours', NULL,                         5, 1, 'IN_PROGRESS');

INSERT INTO trip_participant (trip_id, user_id, pickup_location_id, dropoff_location_id, status)
VALUES
  (1, 1, 3, 1, 'JOINED'),
  (1, 3, 5, 1, 'JOINED'),
  (2, 2, 4, 1, 'JOINED'),
  (2, 1, 3, 1, 'JOINED'),
  (3, 2, 5, 1, 'JOINED');
