# Infra runs in Docker (fits a 2 CPU / 4 GB VM); Spark, dbt, GX and Airflow run from local venvs.
PY      ?= .venv/bin/python
DBT     ?= .venv/bin/dbt
AF      ?= .venv-airflow/bin/airflow
COMPOSE ?= $(shell command -v docker-compose >/dev/null 2>&1 && echo docker-compose || echo docker compose)
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export SPARK_LOCAL_IP ?= 127.0.0.1
export AIRFLOW_HOME ?= $(CURDIR)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER ?= $(CURDIR)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES ?= False

.PHONY: install up down connect topics simulate evolve stream stream-once warehouse dbt quality \
        batch airflow-test demo benchmark test lint reset

install:        ## project venv + separate Airflow venv (Airflow pins many libraries)
	python3.12 -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements.txt
	python3.12 -m venv .venv-airflow && .venv-airflow/bin/pip install -U pip && \
	  .venv-airflow/bin/pip install "apache-airflow==3.3.2" apache-airflow-providers-standard \
	  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

up:             ## Postgres + Kafka (KRaft) + Debezium Connect + S3 (SeaweedFS)
	$(COMPOSE) up -d
	@echo "Connect: http://localhost:8083   S3: http://localhost:8333   Postgres: localhost:5462"

down:           ## stop and delete all data volumes
	$(COMPOSE) down -v

connect:        ## register the Debezium Postgres connector (idempotent)
	$(PY) -m lakehouse.connect.register

topics:         ## list topics and peek at one change event
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic shop.public.orders --max-messages 1 --timeout-ms 10000 || true

simulate:       ## write inserts/updates/deletes at 50 changes/s for 2 minutes
	$(PY) -m lakehouse.simulator.orders --rate 50 --seconds 120

evolve:         ## F7: add products.weight_kg upstream while streaming
	$(PY) -m lakehouse.simulator.orders --evolve-schema

stream:         ## Kafka -> Bronze -> Silver, continuous (Ctrl-C to stop; restart resumes exactly)
	$(PY) -m lakehouse.spark.pipeline

stream-once:    ## process everything available, then exit
	$(PY) -m lakehouse.spark.pipeline --available-now

warehouse:      ## Silver -> DuckDB raw schema (WAREHOUSE_TARGET=snowflake for Snowflake)
	$(PY) -m lakehouse.warehouse.load

dbt:            ## dbt build (run + test) on the warehouse
	cd dbt && ../$(DBT) build --profiles-dir .

quality:        ## Great Expectations checkpoints (silver, gold)
	$(PY) -m lakehouse.quality.checks silver --freshness-minutes 60
	$(PY) -m lakehouse.quality.checks gold

airflow-test:   ## run the hourly DAG once, in-process (airflow dags test)
	$(AF) db migrate >/dev/null && $(AF) dags test lakehouse_hourly

demo: up connect simulate stream-once warehouse dbt quality   ## end to end, batch-style

benchmark:      ## throughput, p95 latency, schema evolution, kill -9 recovery -> docs/METRICS.md
	$(PY) scripts/benchmark.py all --rate 50 --seconds 180

test:           ## Spark unit tests + dbt build on fixture data
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check . && $(PY) -m ruff format --check . && \
	.venv/bin/sqlfluff lint dbt/models --dialect duckdb --templater jinja

reset:          ## wipe the lake + checkpoints (keeps Kafka/Postgres) to rebuild Silver from scratch
	$(COMPOSE) exec -T s3 sh -c "echo 'fs.rm -r /buckets/lakehouse/silver' | weed shell -master=localhost:9333; \
	  echo 'fs.rm -r /buckets/lakehouse/bronze' | weed shell -master=localhost:9333; \
	  echo 'fs.rm -r /buckets/lakehouse/_checkpoints' | weed shell -master=localhost:9333"
