"""Painel mensal (gold), recalculado sempre que uma das tabelas silver é atualizada.

Não tem horário fixo: o Airflow dispara a DAG quando os Assets da silver mudam.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

from assets import GOLD_PAINEL, SILVER_SGS, SILVER_SIDRA
from lakehouse import pipelines


@dag(
    schedule=(SILVER_SGS | SILVER_SIDRA),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold"],
    doc_md=__doc__,
)
def gold_painel():
    @task(outlets=[GOLD_PAINEL])
    def construir() -> int:
        return pipelines.construir_painel()

    construir()


gold_painel()
