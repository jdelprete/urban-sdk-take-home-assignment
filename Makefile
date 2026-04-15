SHELL := /bin/zsh
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
UVICORN := $(VENV)/bin/uvicorn

DATABASE_URL ?= postgresql+psycopg://$(USER)@localhost:5432/urbansdk

.PHONY: help venv install test test-all integration-test db-start db-init ingest run smoke-test db-stop

help:
	@echo "Available targets:"
	@echo "  make venv        Create the local virtual environment"
	@echo "  make install     Install requirements and the project package into .venv"
	@echo "  make test        Run the fast API unit-style pytest suite"
	@echo "  make test-all    Run both the fast and integration pytest suites"
	@echo "  make integration-test Run the live database-backed API integration suite"
	@echo "  make db-start    Start Homebrew PostgreSQL 17 locally"
	@echo "  make db-init     Create the urbansdk database and enable PostGIS"
	@echo "  make ingest      Load the parquet data into PostgreSQL/PostGIS"
	@echo "  make run         Start the FastAPI application"
	@echo "  make smoke-test  Run HTTP checks against the live API"
	@echo "  make db-stop     Stop the local PostgreSQL server"

venv:
	python3 -m venv $(VENV)

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	$(PYTHON) -m pytest -m "not integration"

test-all:
	$(PYTHON) -m pytest

integration-test:
	$(PYTHON) -m pytest -m integration

db-start:
	@if /opt/homebrew/opt/postgresql@17/bin/pg_ctl -D /opt/homebrew/var/postgresql@17 status >/dev/null 2>&1; then \
		echo "PostgreSQL is already running."; \
	else \
		/opt/homebrew/opt/postgresql@17/bin/pg_ctl -D /opt/homebrew/var/postgresql@17 -l /tmp/urbansdk-postgres.log start; \
	fi

db-init:
	-/opt/homebrew/opt/postgresql@17/bin/createdb urbansdk
	/opt/homebrew/opt/postgresql@17/bin/psql -d urbansdk -c "CREATE EXTENSION IF NOT EXISTS postgis;"

ingest:
	DATABASE_URL="$(DATABASE_URL)" $(PYTHON) scripts/ingest_data.py

run:
	DATABASE_URL="$(DATABASE_URL)" $(UVICORN) app.main:app --reload

smoke-test:
	$(PYTHON) -c 'import requests; base = "http://127.0.0.1:8000"; checks = [requests.get(f"{base}/health", timeout=30), requests.get(f"{base}/aggregates/", params={"day": "Monday", "period": "AM Peak"}, timeout=60), requests.get(f"{base}/aggregates/16981048", params={"day": "Monday", "period": "AM Peak"}, timeout=60), requests.get(f"{base}/patterns/slow_links/", params={"period": "AM Peak", "threshold": 20, "min_days": 1}, timeout=60), requests.post(f"{base}/aggregates/spatial_filter/", json={"day": "Monday", "period": "AM Peak", "bbox": [-81.8, 30.1, -81.6, 30.3]}, timeout=60)]; [print(response.request.method, response.request.url, response.status_code) for response in checks]'

db-stop:
	@if /opt/homebrew/opt/postgresql@17/bin/pg_ctl -D /opt/homebrew/var/postgresql@17 status >/dev/null 2>&1; then \
		/opt/homebrew/opt/postgresql@17/bin/pg_ctl -D /opt/homebrew/var/postgresql@17 stop; \
	else \
		echo "PostgreSQL is not running."; \
	fi
