"""Transformações entre as camadas bronze, silver e gold.

Todas as funções aqui são puras: recebem DataFrames e devolvem DataFrames,
sem acesso a rede, disco ou relógio. Isso as torna fáceis de testar.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from lakehouse.config import SerieSGS

TIMESTAMP = pl.Datetime("us", "UTC")

ESQUEMA_BRONZE_SGS = {
    "serie_codigo": pl.Int64,
    "serie_nome": pl.String,
    "data_raw": pl.String,
    "valor_raw": pl.String,
    "lote": pl.String,
    "ingerido_em": TIMESTAMP,
}

ESQUEMA_SILVER_SGS = {
    "serie_codigo": pl.Int64,
    "serie_nome": pl.String,
    "data": pl.Date,
    "valor": pl.Float64,
    "atualizado_em": TIMESTAMP,
}

ESQUEMA_BRONZE_SIDRA = {
    "tabela": pl.Int64,
    "variavel": pl.Int64,
    "periodo_codigo": pl.String,
    "valor_raw": pl.String,
    "lote": pl.String,
    "ingerido_em": TIMESTAMP,
}

ESQUEMA_SILVER_SIDRA = {
    "tabela": pl.Int64,
    "variavel": pl.Int64,
    "data": pl.Date,
    "valor": pl.Float64,
    "atualizado_em": TIMESTAMP,
}


# --- carga incremental ------------------------------------------------------


def inicio_incremental(
    ultima_data: date | None, data_inicio: date, janela_revisao_dias: int
) -> date:
    """Data a partir da qual buscar: tudo na primeira carga, e depois só o período recente.

    A janela de revisão relê os últimos dias para capturar valores que a fonte corrigiu.
    """
    if ultima_data is None:
        return data_inicio
    return max(data_inicio, ultima_data - timedelta(days=janela_revisao_dias))


# --- bronze -----------------------------------------------------------------


def sgs_para_bronze(
    registros: list[dict[str, Any]], serie: SerieSGS, lote: str, ingerido_em: datetime
) -> pl.DataFrame:
    """Guarda a resposta do SGS como chegou (texto), com os metadados da carga."""
    return pl.DataFrame(
        {
            "serie_codigo": [serie.codigo] * len(registros),
            "serie_nome": [serie.nome] * len(registros),
            "data_raw": [str(r.get("data")) for r in registros],
            "valor_raw": [str(r.get("valor")) for r in registros],
            "lote": [lote] * len(registros),
            "ingerido_em": [ingerido_em] * len(registros),
        },
        schema=ESQUEMA_BRONZE_SGS,
    )


def _chave_do_periodo(cabecalho: dict[str, str]) -> str:
    """Encontra no cabeçalho do SIDRA a coluna com o código do período (mês, trimestre ou ano)."""
    for chave, rotulo in cabecalho.items():
        if chave.endswith("C") and rotulo.split(" (")[0] in {"Mês", "Trimestre", "Ano"}:
            return chave
    raise ValueError(f"Cabeçalho do SIDRA sem coluna de período: {cabecalho}")


def sidra_para_bronze(
    resposta: list[dict[str, Any]],
    tabela: int,
    variavel: int,
    lote: str,
    ingerido_em: datetime,
) -> pl.DataFrame:
    """Guarda as linhas do SIDRA como chegaram, descartando o cabeçalho."""
    cabecalho, linhas = resposta[0], resposta[1:]
    chave_periodo = _chave_do_periodo(cabecalho)
    return pl.DataFrame(
        {
            "tabela": [tabela] * len(linhas),
            "variavel": [variavel] * len(linhas),
            "periodo_codigo": [str(linha[chave_periodo]) for linha in linhas],
            "valor_raw": [str(linha.get("V")) for linha in linhas],
            "lote": [lote] * len(linhas),
            "ingerido_em": [ingerido_em] * len(linhas),
        },
        schema=ESQUEMA_BRONZE_SIDRA,
    )


# --- silver -----------------------------------------------------------------


def _mais_recente_por_chave(df: pl.DataFrame, chaves: list[str]) -> pl.DataFrame:
    """Quando a mesma chave aparece mais de uma vez, fica a versão ingerida por último."""
    return df.sort("ingerido_em").unique(subset=chaves, keep="last", maintain_order=True)


def sgs_bronze_para_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    """Tipa datas e valores, remove observações vazias e deduplica por série e data."""
    tipado = bronze.with_columns(
        pl.col("data_raw").str.strptime(pl.Date, "%d/%m/%Y", strict=True).alias("data"),
        pl.col("valor_raw").str.replace(",", ".").cast(pl.Float64, strict=False).alias("valor"),
    ).filter(pl.col("valor").is_not_null())
    return (
        _mais_recente_por_chave(tipado, ["serie_codigo", "data"])
        .select(
            "serie_codigo",
            "serie_nome",
            "data",
            "valor",
            pl.col("ingerido_em").alias("atualizado_em"),
        )
        .sort("serie_codigo", "data")
    )


def sidra_bronze_para_silver(bronze: pl.DataFrame) -> pl.DataFrame:
    """Converte o período AAAAMM em data e descarta valores indisponíveis ("...", "-", "X")."""
    tipado = bronze.with_columns(
        pl.col("periodo_codigo").str.strptime(pl.Date, "%Y%m", strict=True).alias("data"),
        pl.col("valor_raw").cast(pl.Float64, strict=False).alias("valor"),
    ).filter(pl.col("valor").is_not_null())
    return (
        _mais_recente_por_chave(tipado, ["tabela", "variavel", "data"])
        .select(
            "tabela",
            "variavel",
            "data",
            "valor",
            pl.col("ingerido_em").alias("atualizado_em"),
        )
        .sort("data")
    )


# --- gold -------------------------------------------------------------------


def _mensal(
    silver: pl.DataFrame, serie_nome: str, coluna: str, agregacao: pl.Expr
) -> pl.DataFrame:
    return (
        silver.filter(pl.col("serie_nome") == serie_nome)
        .sort("data")
        .group_by(pl.col("data").dt.truncate("1mo").alias("mes"), maintain_order=True)
        .agg(agregacao.alias(coluna))
    )


def painel_mensal(sgs: pl.DataFrame, ipca_indice: pl.DataFrame | None = None) -> pl.DataFrame:
    """Painel mensal de indicadores, pronto para análise.

    Colunas:
      selic_meta       meta da Selic no fim do mês (% a.a.)
      ipca_mensal      variação do IPCA no mês (%)
      ipca_12m         IPCA acumulado em 12 meses (%)
      igpm_mensal      variação do IGP-M no mês (%)
      dolar_medio      dólar médio do mês (R$)
      dolar_fim        dólar no último dia útil do mês (R$)
      juro_real_ex_post  Selic deflacionada pelo IPCA de 12 meses (%)
      ipca_indice      número-índice do IPCA (IBGE), quando disponível
    """
    partes = [
        _mensal(sgs, "selic_meta", "selic_meta", pl.col("valor").last()),
        _mensal(sgs, "ipca_mensal", "ipca_mensal", pl.col("valor").last()),
        _mensal(sgs, "igpm_mensal", "igpm_mensal", pl.col("valor").last()),
        _mensal(sgs, "dolar_venda", "dolar_medio", pl.col("valor").mean()),
        _mensal(sgs, "dolar_venda", "dolar_fim", pl.col("valor").last()),
    ]
    if ipca_indice is not None and ipca_indice.height:
        partes.append(
            ipca_indice.select(
                pl.col("data").dt.truncate("1mo").alias("mes"),
                pl.col("valor").alias("ipca_indice"),
            )
        )

    painel = partes[0]
    for parte in partes[1:]:
        painel = painel.join(parte, on="mes", how="full", coalesce=True)

    painel = painel.sort("mes").with_columns(
        (
            (
                (pl.col("ipca_mensal") / 100).log1p().rolling_sum(window_size=12).exp() - 1
            )
            * 100
        ).alias("ipca_12m")
    )
    return painel.with_columns(
        (
            ((1 + pl.col("selic_meta") / 100) / (1 + pl.col("ipca_12m") / 100) - 1) * 100
        ).alias("juro_real_ex_post")
    ).select(
        "mes",
        "selic_meta",
        "ipca_mensal",
        "ipca_12m",
        "igpm_mensal",
        "dolar_medio",
        "dolar_fim",
        "juro_real_ex_post",
        *(["ipca_indice"] if "ipca_indice" in painel.columns else []),
    )
