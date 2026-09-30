# Short commands for FleetPulse. Run "make help" to list them.
.PHONY: help env up down reset ps logs test dashboards report numbers

help:
	@echo "make up          build images and start the whole stack"
	@echo "make ps          show the health of every service"
	@echo "make logs        follow the JSON logs of the pipeline services"
	@echo "make test        run the pytest suite in its own container"
	@echo "make down        stop the stack (data is kept)"
	@echo "make reset       stop the stack and delete all data"
	@echo "make dashboards  regenerate the Grafana dashboard JSON files"
	@echo "make numbers     refresh the measured values used by the report (stack must be running)"
	@echo "make report      build report/main.pdf with LaTeX"

env:
	@test -f .env || cp .env.example .env

up: env
	docker compose up -d --build

ps:
	docker compose ps

logs:
	docker compose logs -f --no-log-prefix stream-producer batch-producer spark-streaming api

test:
	docker compose --profile test run --rm tests

down:
	docker compose stop

reset:
	docker compose down -v

dashboards:
	python3 observability/grafana/build_dashboards.py

numbers:
	python3 report/tools/make_numbers.py

report:
	cd report && pdflatex -interaction=nonstopmode main.tex >/dev/null && pdflatex -interaction=nonstopmode main.tex >/dev/null && echo "report/main.pdf"
