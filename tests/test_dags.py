"""Garante que as DAGs carregam no Airflow sem erros. Só roda com o Airflow instalado."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

PASTA_DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    sys.path.insert(0, str(PASTA_DAGS))  # no Airflow, a pasta de DAGs já fica no sys.path

    return DagBag(dag_folder=str(PASTA_DAGS), include_examples=False)


def test_dags_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}


def test_dags_esperadas(dagbag):
    assert set(dagbag.dag_ids) == {"bcb_sgs", "ibge_sidra", "gold_painel"}


def test_bcb_sgs_mapeia_uma_task_por_serie(dagbag):
    dag = dagbag.get_dag("bcb_sgs")
    assert {"listar_series", "ingerir", "atualizar_silver", "validar"} <= set(dag.task_ids)
