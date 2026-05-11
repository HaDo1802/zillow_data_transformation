SHELL := /bin/zsh

.PHONY: help sync format lint check python-check dbt-check

help:
	@echo "Available targets:"
	@echo "  make sync         Install/update local dependencies via uv"
	@echo "  make format       Format Python with ruff"
	@echo "  make lint         Lint Python with ruff"
	@echo "  make check        Run Python syntax checks and dbt parse/test"
	@echo "  make python-check Run Python syntax checks"
	@echo "  make dbt-check    Run dbt parse and dbt test from zillow_transformation/"

sync:
	uv sync --group dbt --group streamlit --group dev

format:
	uv run ruff format app.py scripts dags

lint:
	uv run ruff check app.py scripts dags

check: python-check dbt-check

python-check:
	@PYTHONPYCACHEPREFIX=/tmp uv run python -m py_compile app.py

dbt-check:
	@cd zillow_transformation && uv run dbt parse --target dev && uv run dbt test --target dev
