.PHONY: test lint demo-quakes demo-economic demo-multi docker-test docker-run clean

test:
	PYTHONPATH=src pytest tests/ -v --tb=short

lint:
	ruff check src/ tests/ --select F --ignore F401

demo-quakes:
	python examples/collect_earthquakes.py

demo-economic:
	python examples/collect_economic_indicators.py

demo-multi:
	python examples/multi_source_pipeline.py

docker-test:
	docker compose run --rm test

docker-run:
	docker compose run --rm multi-source

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name '*.pyc' -delete 2>/dev/null; true
