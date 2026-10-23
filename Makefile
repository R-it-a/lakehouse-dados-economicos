.PHONY: instalar testes lint pipeline subir descer

instalar:
	pip install -e ".[dev]"

testes:
	pytest

lint:
	ruff check .

pipeline:  ## roda o pipeline completo sem Airflow
	lakehouse

subir:  ## sobe o Airflow em http://localhost:8080
	docker compose up --build -d

descer:
	docker compose down
