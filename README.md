# Urban SDK Take-Home Assignment

This project implements the requested FastAPI geospatial traffic microservice backed by PostgreSQL + PostGIS, plus a Jupyter notebook for Mapbox visualization.

Architecture diagram asset:

![Architecture Diagram](docs/architecture-diagram.svg)

## Quickstart

The local `.env` used during development is machine-specific. If you run this project on another machine, create your own `.env` from [`.env.example`](.env.example) and update `DATABASE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and related values to match your local PostgreSQL setup.

```bash
source .venv/bin/activate
uvicorn app.main:app --reload
```

If `.venv` does not exist yet:

```bash
make venv
source .venv/bin/activate
make install
```

If you prefer `make`, the common path is:

```bash
make db-start
make db-init
make ingest
make test
make test-all
make integration-test
make run
```

## Stack

- FastAPI for the REST API
- SQLAlchemy ORM + GeoAlchemy2 for database access
- PostgreSQL + PostGIS for spatial storage and filtering
- Pandas + PyArrow for parquet ingestion
- MapboxGL in Jupyter for visualization

## Project Structure

```text
app/
  api/
  core/
  db/
  models/
  schemas/
docs/
notebooks/
scripts/
tests/
.env
.env.example
docker-compose.yml
Dockerfile
Makefile
pyproject.toml
requirements.txt
README.md
```

## Setup

Two setup paths work:

- Docker Compose for a containerized PostGIS database
- Homebrew PostgreSQL + PostGIS for a native local database on macOS

Before using either path on a different machine, create `.env` from [`.env.example`](.env.example) and edit it to match your local database user, password, host, and database name.

If you are setting this up from scratch on another machine, copy `.env.example` to `.env` and edit the values there.

For development and testing, the preferred workflow is:

- activate the local virtual environment
- run the local `make` commands
- use the native PostgreSQL/PostGIS setup

Docker Compose is supported, but it is better treated as an optional alternative runtime than the primary development path for this project.

### Python Environment

1. Create the virtual environment if needed:

```bash
make venv
```

2. Activate it:

```bash
source .venv/bin/activate
```

3. Install dependencies:

```bash
make install
```

### Database Setup

#### Option A: Docker Compose

```bash
docker compose up -d db
```

For Docker Compose, `.env` will usually need values like:

```dotenv
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/urbansdk
POSTGRES_DB=urbansdk
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

#### Option B: Homebrew On macOS

Install PostgreSQL 17 and PostGIS:

```bash
brew install postgresql@17 postgis
```

Start PostgreSQL:

```bash
make db-start
```

`make db-start` is safe to rerun. If PostgreSQL is already running, it exits cleanly without failing.

Create the database and enable PostGIS:

```bash
make db-init
```

If you run this on another machine, make sure the PostgreSQL user in `.env` matches the user that owns your local PostgreSQL cluster. If needed, adjust commands like `createdb urbansdk` so they run under the correct local PostgreSQL user.

### Load Data

Load the supplied parquet files into PostgreSQL/PostGIS:

```bash
make ingest
```

Expected result:

```text
Ingested 100924 links and 1239946 speed records.
```

### Run The API

```bash
make run
```

The API will be available at [http://localhost:8000/docs](http://localhost:8000/docs).

### Notebook Visualization

The notebook is located at [notebooks/traffic_speed_visualization.ipynb](notebooks/traffic_speed_visualization.ipynb).

Before opening it, export a valid Mapbox token in your shell:

```bash
export MAPBOX_TOKEN="your_mapbox_token_here"
```

The notebook reads `MAPBOX_TOKEN` from the environment instead of hardcoding credentials. If your API is running somewhere other than `http://localhost:8000`, you can also set:

```bash
export BASE_URL="http://localhost:8000"
```

Then launch Jupyter from the activated virtual environment and open the notebook:

```bash
jupyter notebook
```

Note: if you save an executed notebook with Mapbox output embedded, the saved notebook may also contain your token in the rendered HTML/JS. Treat executed notebook artifacts as sensitive unless you clear outputs first.

### Smoke Test

```bash
make smoke-test
```

### Automated Tests

Run the fast database-independent API suite:

```bash
make test
```

Run the full test suite, including both fast tests and live integration tests:

```bash
make test-all
```

Run the live database-backed integration suite after PostgreSQL is running and `make ingest` has completed:

```bash
make integration-test
```

Stop the local Homebrew PostgreSQL server when you are done:

```bash
make db-stop
```

## Endpoints

- `GET /aggregates/?day=Monday&period=AM Peak`
- `GET /aggregates/{link_id}?day=Monday&period=AM Peak`
- `GET /patterns/slow_links/?period=AM Peak&threshold=20&min_days=1`
- `POST /aggregates/spatial_filter/`

## Notes And Assumptions

- The source link geometry is delivered as single-part `MultiLineString` GeoJSON, and is normalized to PostGIS `LINESTRING` during ingestion.
- `day_of_week` is assumed to follow `1=Sunday, 2=Monday, ..., 7=Saturday`, based on the provided timestamp coverage.
- The provided speed dataset only covers Monday, January 1, 2024. The `slow_links` endpoint is implemented generically, but with this sample data only `min_days=1` can return results.
- API responses expose geometry as GeoJSON so the notebook can render segments directly.
- The tested local native runtime used PostgreSQL 17 and PostGIS 3.6 via Homebrew.
- The checked-in `.env` is intentionally local to this workspace and should be edited before reuse on another machine.
- Production hardening concerns such as API rate limiting, retry/backoff policy, and similar operational safeguards were not added in this take-home for the sake of time; the goal here was to show the core service design, data model, query layer, testing approach, and visualization workflow in the initial code push.

## Architecture

Primary diagram:

![Architecture Diagram](docs/architecture-diagram.svg)

Fallback text version:

```mermaid
flowchart LR
    A["Parquet Files"] --> B["Ingestion Script<br/>scripts/ingest_data.py"]
    B --> C["PostgreSQL + PostGIS"]
    C --> D["FastAPI Service"]
    D --> E["REST Endpoints"]
    E --> F["Jupyter Notebook<br/>MapboxGL Visualization"]
```

The same diagram is also described in [docs/architecture.md](docs/architecture.md).
