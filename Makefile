SHELL := /bin/zsh

.PHONY: help format lint check python-check dbt-check

help:
	@echo "Available targets:"
	@echo "  make format       Format Python with ruff/black if installed"
	@echo "  make lint         Lint Python with ruff/flake8 if installed"
	@echo "  make check        Run Python syntax checks and dbt parse/test if dbt is installed"
	@echo "  make python-check Run Python syntax checks"
	@echo "  make dbt-check    Run dbt parse and dbt test from zillow_transformation/"

format:
	@if command -v ruff >/dev/null 2>&1; then \
		ruff format app.py scripts airflow; \
	elif command -v black >/dev/null 2>&1; then \
		black app.py scripts airflow; \
	else \
		echo "No formatter found. Install 'ruff' or 'black' to use make format."; \
		exit 1; \
	fi

lint:
	@if command -v ruff >/dev/null 2>&1; then \
		ruff check app.py scripts airflow; \
	elif command -v flake8 >/dev/null 2>&1; then \
		flake8 app.py scripts airflow; \
	else \
		echo "No linter found. Install 'ruff' or 'flake8' to use make lint."; \
		exit 1; \
	fi

check: python-check dbt-check

python-check:
	@PYTHONPYCACHEPREFIX=/tmp python3 -m py_compile app.py

dbt-check:
	@if command -v dbt >/dev/null 2>&1; then \
		cd zillow_transformation && dbt parse --target dev && dbt test --target dev; \
	else \
		echo "dbt not found. Skipping dbt checks."; \
	fi
