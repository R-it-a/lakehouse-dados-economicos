"""Checagens de qualidade de dados.

Todas seguem o mesmo princípio: se algo está errado, a execução falha e avisa,
em vez de gravar um dado incorreto em silêncio.
"""

from __future__ import annotations

from datetime import date

import polars as pl


class FalhaDeQualidade(Exception):
    """Os dados não passaram em uma checagem de qualidade."""


def checar_nao_vazio(df: pl.DataFrame, nome: str) -> None:
    if df.height == 0:
        raise FalhaDeQualidade(f"{nome}: nenhuma linha recebida")


def checar_colunas(df: pl.DataFrame, esperado: dict[str, pl.DataType], nome: str) -> None:
    faltando = set(esperado) - set(df.columns)
    if faltando:
        raise FalhaDeQualidade(f"{nome}: colunas ausentes {sorted(faltando)}")
    tipos_errados = {
        coluna: (str(df.schema[coluna]), str(tipo))
        for coluna, tipo in esperado.items()
        if df.schema[coluna] != tipo
    }
    if tipos_errados:
        raise FalhaDeQualidade(f"{nome}: tipos inesperados (atual, esperado) {tipos_errados}")


def checar_chave_unica(df: pl.DataFrame, chaves: list[str], nome: str) -> None:
    duplicadas = df.group_by(chaves).len().filter(pl.col("len") > 1)
    if duplicadas.height:
        raise FalhaDeQualidade(f"{nome}: {duplicadas.height} chaves duplicadas em {chaves}")


def checar_sem_nulos(df: pl.DataFrame, colunas: list[str], nome: str) -> None:
    nulos = {coluna: df[coluna].null_count() for coluna in colunas if df[coluna].null_count()}
    if nulos:
        raise FalhaDeQualidade(f"{nome}: valores nulos {nulos}")


def checar_intervalo(
    df: pl.DataFrame, coluna: str, minimo: float, maximo: float, nome: str
) -> None:
    fora = df.filter((pl.col(coluna) < minimo) | (pl.col(coluna) > maximo))
    if fora.height:
        raise FalhaDeQualidade(
            f"{nome}: {fora.height} valores de {coluna} fora de [{minimo}, {maximo}]"
        )


def checar_atualizacao(
    df: pl.DataFrame, coluna_data: str, atraso_maximo_dias: int, hoje: date, nome: str
) -> None:
    """Falha se o dado mais recente for mais antigo que o atraso aceitável."""
    ultima = df[coluna_data].max()
    if ultima is None:
        raise FalhaDeQualidade(f"{nome}: sem datas para checar a atualização")
    atraso = (hoje - ultima).days
    if atraso > atraso_maximo_dias:
        raise FalhaDeQualidade(
            f"{nome}: último dado em {ultima}, há {atraso} dias (limite: {atraso_maximo_dias})"
        )
