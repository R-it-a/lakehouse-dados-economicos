from datetime import date

import polars as pl
import pytest

from lakehouse import quality
from lakehouse.quality import FalhaDeQualidade

DF = pl.DataFrame(
    {"chave": [1, 2, 3], "data": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
     "valor": [1.0, 2.0, 3.0]}
)


def test_nao_vazio():
    quality.checar_nao_vazio(DF, "t")
    with pytest.raises(FalhaDeQualidade, match="nenhuma linha"):
        quality.checar_nao_vazio(DF.clear(), "t")


def test_colunas_e_tipos():
    quality.checar_colunas(DF, {"chave": pl.Int64, "valor": pl.Float64}, "t")
    with pytest.raises(FalhaDeQualidade, match="ausentes"):
        quality.checar_colunas(DF, {"outra": pl.Int64}, "t")
    with pytest.raises(FalhaDeQualidade, match="tipos"):
        quality.checar_colunas(DF, {"valor": pl.String}, "t")


def test_chave_unica():
    quality.checar_chave_unica(DF, ["chave"], "t")
    with pytest.raises(FalhaDeQualidade, match="duplicadas"):
        quality.checar_chave_unica(pl.concat([DF, DF.head(1)]), ["chave"], "t")


def test_sem_nulos():
    with pytest.raises(FalhaDeQualidade, match="nulos"):
        quality.checar_sem_nulos(DF.with_columns(valor=pl.lit(None)), ["valor"], "t")


def test_intervalo():
    quality.checar_intervalo(DF, "valor", 0, 10, "t")
    with pytest.raises(FalhaDeQualidade, match="fora"):
        quality.checar_intervalo(DF, "valor", 0, 2, "t")


def test_atualizacao():
    quality.checar_atualizacao(DF, "data", 5, date(2024, 1, 8), "t")
    with pytest.raises(FalhaDeQualidade, match="há 7 dias"):
        quality.checar_atualizacao(DF, "data", 5, date(2024, 1, 10), "t")
