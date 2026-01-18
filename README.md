# Apache Arrow Flight Performance Evaluation

This project evaluates Apache Arrow Flight as a data transfer layer between a PostgreSQL database and a client application.  
The setup uses Docker Compose and consists of a PostgreSQL database, an Arrow Flight server, and an Arrow Flight client.

The project is designed to measure end-to-end performance for read and write workloads using Arrow Flight in a distributed setup.

---

## System Overview

The system consists of three main components:

- **PostgreSQL database**  
  Stores all application data and executes SQL queries.

- **Apache Arrow Flight server**  
  Connects to PostgreSQL, executes queries, converts results to Arrow tables, and exposes them via Flight RPC.

- **Apache Arrow Flight client**  
  Requests data from the server, measures performance, and saves results and benchmarks.

The database and Flight server usually run on one machine, while the client can run on the same machine or on a different machine in the same local network.

---

## Running the System
### Requirements

To run the system locally, the following requirements must be met:

- **Docker** (version 20.10 or newer)
- **Docker Compose** (v2, included with recent Docker installations)

### 1. Clean and Build the Environment

To stop all containers and remove the database volume (fresh start):

```bash
docker compose down -v
```

To build all Docker images:

```bash
docker compose build
```

### 2. Start Database and Flight Server
Run PostgreSQL and the Arrow Flight server in the background:
```bash
docker compose up -d db flight-server
```

What happens at this step:
* PostgreSQL starts and initializes the schema using db-init.sql

* Arrow Flight server starts and connects to the database

* Only few test examples are inserted

## Populating the Database with Test Data
### 3. Insert Main Test Dataset - 

To populate the database with a large amount of test data, run:

```bash
type .\app\test_data.sql | docker exec -i proj_db-db-1 psql -U demo -d demo
```

#### Approximate data generated per run

One execution of test_data.sql generates approximately:

| Table | Approximate rows |
|------|------------------|
| company | ~50 |
| location | ~5,000 |
| user | ~50,000 |
| vehicle | ~2,000 |
| trip | ~200,000 |
| trip_participant | ~400,000 |

You can run this script multiple times if you want an even larger dataset.

### 4. Insert Additional Data for User Statistics

Some benchmarks require consistent data for user_id = 1.
To gwt meaningful results for user history queries, run:
```bash
type .\app\user_1_data.sql | docker exec -i proj_db-db-1 psql -U demo -d demo
```

## Running Benchmarks (Without Indexes)
### 5. Run the Flight Client (Same Machine)

To run benchmarks on the same machine as the server:
```bash
docker compose run --rm flight-client
```

This command:

* Inserts new trips and trip participants using Arrow Flight DoPut

* Executes benchmark queries using DoGet

* Measures end-to-end performance

* Saves results to output/benchmarks.csv

* Stores query results as Parquet files

### 6. Run the Flight Client (Different Machine)

To run the client on a different machine in the same local network, set the Flight server IP address:
```bash
docker compose run --rm -e FLIGHT_URI=grpc://<IP_ADDRESS>:8815 flight-client
```
Replace <IP_ADDRESS> with the server machine’s LAN IP address

## Running Benchmarks With Indexes
### 7. Add Database Indexes

To evaluate the impact of indexing on performance, run this script on the server machine:
```bash
type .\app\add-indexes.sql | docker exec -i proj_db-db-1 psql -U demo -d demo
```
This script adds indexes on frequently joined and filtered columns.

### 8. Re-run Benchmarks

After indexes are added, run the client again:
```bash
docker compose run --rm flight-client
```
