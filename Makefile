.PHONY: help install migrate introspect backfill realtime reconcile check fix reset reprocess status test build

PY ?= .venv/bin/python

help:
	@echo "Targets:"
	@echo "  install     create .venv and install requirements"
	@echo "  migrate     run SQL migrations"
	@echo "  introspect  regenerate registry + typed DDL (+ drift diff)"
	@echo "  backfill    historical backfill (ENTITIES=a,b CONCURRENCY=4)"
	@echo "  realtime    continuous ingestion"
	@echo "  reconcile   delete detection (id-diff tombstones)"
	@echo "  check       maintenance: report state/gaps/failed pages"
	@echo "  fix         maintenance: recover + re-queue (+ ranges)"
	@echo "  reset       maintenance: requeue/clear backfill state"
	@echo "  reprocess   re-derive typed tables from raw (no API)"
	@echo "  status      progress overview"
	@echo "  test        run unit tests"
	@echo "  build       docker build"

install:
	python3 -m venv .venv && .venv/bin/pip install -U pip && .venv/bin/pip install -r requirements.txt

migrate:
	$(PY) -m src.main migrate

introspect:
	$(PY) -m src.main introspect

backfill:
	$(PY) -m src.main load backfill $(if $(ENTITIES),--entities $(ENTITIES),) $(if $(CONCURRENCY),--concurrency $(CONCURRENCY),)

realtime:
	$(PY) -m src.main load realtime $(if $(ENTITIES),--entities $(ENTITIES),)

reconcile:
	$(PY) -m src.main reconcile $(if $(ENTITIES),--entities $(ENTITIES),)

check:
	$(PY) -m src.main maintain check $(if $(ENTITIES),--entities $(ENTITIES),)

fix:
	$(PY) -m src.main maintain fix $(if $(ENTITIES),--entities $(ENTITIES),) $(if $(BLOCK_RANGE),--block-range $(BLOCK_RANGE),) $(if $(ID_RANGE),--id-range $(ID_RANGE),)

reset:
	$(PY) -m src.main maintain reset $(if $(ENTITIES),--entities $(ENTITIES),) $(if $(STATUS),--status $(STATUS),)

reprocess:
	$(PY) -m src.main maintain reprocess $(if $(ENTITIES),--entities $(ENTITIES),)

status:
	$(PY) -m src.main status

test:
	$(PY) -m pytest -q

build:
	docker build -t envio_ga-indexer .
