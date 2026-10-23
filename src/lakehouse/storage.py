"""Leitura e escrita das tabelas Delta do lakehouse."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from lakehouse.config import raiz_lake


def caminho_tabela(camada: str, tabela: str, raiz: Path | None = None) -> str:
    """Caminho de uma tabela, organizado por camada: <raiz>/<camada>/<tabela>."""
    if camada not in {"bronze", "silver", "gold"}:
        raise ValueError(f"Camada desconhecida: {camada}")
    return str((raiz or raiz_lake()) / camada / tabela)


def existe(caminho: str) -> bool:
    return DeltaTable.is_deltatable(caminho)


def ler(caminho: str) -> pl.DataFrame:
    return pl.read_delta(caminho)


def anexar(df: pl.DataFrame, caminho: str, particionar_por: list[str] | None = None) -> None:
    """Append-only, usado na camada bronze para guardar cada carga como chegou."""
    opcoes = {"partition_by": particionar_por} if particionar_por else None
    df.write_delta(caminho, mode="append", delta_write_options=opcoes)


def upsert(df: pl.DataFrame, caminho: str, chaves: list[str]) -> None:
    """MERGE pelas chaves de negócio: atualiza o que mudou e insere o que é novo.

    Rodar duas vezes com os mesmos dados não duplica linhas (idempotente).
    """
    if not existe(caminho):
        df.write_delta(caminho, mode="overwrite")
        return
    predicado = " AND ".join(f"alvo.{chave} = origem.{chave}" for chave in chaves)
    (
        df.write_delta(
            caminho,
            mode="merge",
            delta_merge_options={
                "predicate": predicado,
                "source_alias": "origem",
                "target_alias": "alvo",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def sobrescrever(df: pl.DataFrame, caminho: str) -> None:
    """Reescrita completa, usada na camada gold, que é pequena e derivada da silver."""
    df.write_delta(caminho, mode="overwrite", delta_write_options={"schema_mode": "overwrite"})
