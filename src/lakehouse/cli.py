"""Roda o pipeline completo sem Airflow: `lakehouse` ou `python -m lakehouse.cli`."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from lakehouse import pipelines, storage
from lakehouse.config import SERIES_SGS


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    lote = f"manual__{datetime.now(UTC):%Y%m%dT%H%M%S}"

    for serie in SERIES_SGS:
        pipelines.ingerir_serie_sgs(serie.codigo, lote)
    pipelines.atualizar_silver_sgs(lote)
    pipelines.validar_silver_sgs()

    pipelines.ingerir_ipca_sidra(lote)
    pipelines.atualizar_silver_sidra(lote)

    pipelines.construir_painel()
    painel = storage.ler(storage.caminho_tabela("gold", pipelines.GOLD_PAINEL))
    with pl.Config(tbl_rows=12, tbl_cols=12):
        print(painel.tail(12))


if __name__ == "__main__":
    main()
