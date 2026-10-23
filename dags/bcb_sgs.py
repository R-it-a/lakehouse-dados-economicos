"""Coleta diária das séries do Banco Central (SGS).

Uma task por série via dynamic task mapping. Cada uma anexa a resposta bruta
na bronze; depois uma única task faz o MERGE do lote na silver e valida.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from assets import SILVER_SGS
from lakehouse import pipelines
from lakehouse.config import SERIES_SGS


@dag(
    schedule="0 21 * * 1-5",  # dias úteis, depois da atualização diária do BCB
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["bcb", "sgs", "bronze", "silver"],
    doc_md=__doc__,
)
def bcb_sgs():
    @task
    def listar_series() -> list[int]:
        return [serie.codigo for serie in SERIES_SGS]

    @task(max_active_tis_per_dagrun=4, multiple_outputs=False)
    def ingerir(codigo: int, run_id: str | None = None):
        return pipelines.ingerir_serie_sgs(codigo, lote=run_id)

    @task
    def atualizar_silver(resumos: list, run_id: str | None = None) -> int:
        return pipelines.atualizar_silver_sgs(lote=run_id)

    @task(outlets=[SILVER_SGS])
    def validar() -> None:
        pipelines.validar_silver_sgs()

    resumos = ingerir.expand(codigo=listar_series())
    atualizar_silver(resumos) >> validar()


bcb_sgs()
