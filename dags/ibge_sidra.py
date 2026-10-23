"""Coleta do número-índice do IPCA no SIDRA (IBGE).

O IPCA sai por volta do dia 10 de cada mês, então a DAG roda diariamente nessa
janela. Rodar de novo sem dado novo não duplica nada, porque a silver usa MERGE.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from assets import SILVER_SIDRA
from lakehouse import pipelines


@dag(
    schedule="0 12 8-15 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=30)},
    tags=["ibge", "sidra", "bronze", "silver"],
    doc_md=__doc__,
)
def ibge_sidra():
    @task
    def ingerir(run_id: str | None = None) -> int:
        return pipelines.ingerir_ipca_sidra(lote=run_id)

    @task(outlets=[SILVER_SIDRA])
    def atualizar_silver(linhas: int, run_id: str | None = None) -> int:
        return pipelines.atualizar_silver_sidra(lote=run_id)

    atualizar_silver(ingerir())


ibge_sidra()
