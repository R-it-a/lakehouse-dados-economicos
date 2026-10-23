"""Etapas do pipeline, independentes do orquestrador.

As DAGs do Airflow e a linha de comando chamam estas funções. Clientes, data
de referência e diretório do lake são injetáveis, o que permite testar tudo
sem rede.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from lakehouse import quality, storage, transforms
from lakehouse.clients import ClienteSGS, ClienteSidra
from lakehouse.config import (
    DATA_INICIO,
    IPCA_INDICE,
    JANELA_REVISAO_DIAS,
    SERIES_POR_CODIGO,
    SERIES_SGS,
)

log = logging.getLogger(__name__)

BRONZE_SGS = "bcb_sgs"
SILVER_SGS = "bcb_sgs"
BRONZE_SIDRA = "ibge_sidra"
SILVER_SIDRA = "ibge_sidra"
GOLD_PAINEL = "painel_mensal"


def _agora() -> datetime:
    return datetime.now(UTC)


def _ultima_data(caminho: str, filtro: pl.Expr) -> date | None:
    if not storage.existe(caminho):
        return None
    return storage.ler(caminho).filter(filtro)["data"].max()


# --- Banco Central (SGS) ----------------------------------------------------


def ingerir_serie_sgs(
    codigo: int,
    lote: str,
    cliente: ClienteSGS | None = None,
    hoje: date | None = None,
    raiz: Path | None = None,
) -> dict[str, object]:
    """Busca uma série no SGS (incremental) e anexa a resposta bruta na bronze."""
    serie = SERIES_POR_CODIGO[codigo]
    hoje = hoje or date.today()
    cliente = cliente or ClienteSGS()

    ultima = _ultima_data(
        storage.caminho_tabela("silver", SILVER_SGS, raiz), pl.col("serie_codigo") == codigo
    )
    inicio = transforms.inicio_incremental(ultima, DATA_INICIO, JANELA_REVISAO_DIAS)
    registros = cliente.buscar_serie(codigo, inicio, hoje)

    bronze = transforms.sgs_para_bronze(registros, serie, lote, _agora())
    quality.checar_nao_vazio(bronze, f"bronze {serie.nome}")
    storage.anexar(
        bronze, storage.caminho_tabela("bronze", BRONZE_SGS, raiz), ["serie_codigo"]
    )
    log.info("%s: %d registros de %s a %s", serie.nome, bronze.height, inicio, hoje)
    return {"codigo": codigo, "serie": serie.nome, "linhas": bronze.height, "inicio": str(inicio)}


def atualizar_silver_sgs(lote: str, raiz: Path | None = None) -> int:
    """Transforma o lote da bronze e faz MERGE na silver."""
    bronze = (
        pl.scan_delta(storage.caminho_tabela("bronze", BRONZE_SGS, raiz))
        .filter(pl.col("lote") == lote)
        .collect()
    )
    silver = transforms.sgs_bronze_para_silver(bronze)
    quality.checar_nao_vazio(silver, "silver bcb_sgs")
    quality.checar_colunas(silver, transforms.ESQUEMA_SILVER_SGS, "silver bcb_sgs")
    quality.checar_chave_unica(silver, ["serie_codigo", "data"], "silver bcb_sgs")
    storage.upsert(
        silver, storage.caminho_tabela("silver", SILVER_SGS, raiz), ["serie_codigo", "data"]
    )
    return silver.height


def validar_silver_sgs(hoje: date | None = None, raiz: Path | None = None) -> None:
    """Checa a tabela silver inteira: chaves, faixas de valores e atualização de cada série."""
    hoje = hoje or date.today()
    silver = storage.ler(storage.caminho_tabela("silver", SILVER_SGS, raiz))
    quality.checar_chave_unica(silver, ["serie_codigo", "data"], "silver bcb_sgs")
    quality.checar_sem_nulos(silver, ["data", "valor"], "silver bcb_sgs")
    for serie in SERIES_SGS:
        dados = silver.filter(pl.col("serie_codigo") == serie.codigo)
        quality.checar_nao_vazio(dados, serie.nome)
        quality.checar_intervalo(dados, "valor", serie.minimo, serie.maximo, serie.nome)
        quality.checar_atualizacao(dados, "data", serie.atraso_maximo_dias, hoje, serie.nome)


# --- IBGE (SIDRA) -----------------------------------------------------------


def ingerir_ipca_sidra(
    lote: str, cliente: ClienteSidra | None = None, raiz: Path | None = None
) -> int:
    """Busca o número-índice do IPCA: histórico completo na primeira carga, depois 24 meses."""
    cliente = cliente or ClienteSidra()
    caminho_silver = storage.caminho_tabela("silver", SILVER_SIDRA, raiz)
    periodos = "last%2024" if storage.existe(caminho_silver) else "all"
    resposta = cliente.buscar_tabela(IPCA_INDICE.tabela, IPCA_INDICE.variavel, periodos)

    bronze = transforms.sidra_para_bronze(
        resposta, IPCA_INDICE.tabela, IPCA_INDICE.variavel, lote, _agora()
    )
    quality.checar_nao_vazio(bronze, "bronze ipca_indice")
    storage.anexar(bronze, storage.caminho_tabela("bronze", BRONZE_SIDRA, raiz))
    return bronze.height


def atualizar_silver_sidra(lote: str, raiz: Path | None = None) -> int:
    bronze = (
        pl.scan_delta(storage.caminho_tabela("bronze", BRONZE_SIDRA, raiz))
        .filter(pl.col("lote") == lote)
        .collect()
    )
    silver = transforms.sidra_bronze_para_silver(bronze)
    chaves = ["tabela", "variavel", "data"]
    quality.checar_nao_vazio(silver, "silver ibge_sidra")
    quality.checar_colunas(silver, transforms.ESQUEMA_SILVER_SIDRA, "silver ibge_sidra")
    quality.checar_chave_unica(silver, chaves, "silver ibge_sidra")
    storage.upsert(silver, storage.caminho_tabela("silver", SILVER_SIDRA, raiz), chaves)
    return silver.height


# --- gold -------------------------------------------------------------------


def construir_painel(raiz: Path | None = None) -> int:
    """Recalcula o painel mensal a partir da silver."""
    sgs = storage.ler(storage.caminho_tabela("silver", SILVER_SGS, raiz))
    caminho_sidra = storage.caminho_tabela("silver", SILVER_SIDRA, raiz)
    ipca_indice = storage.ler(caminho_sidra) if storage.existe(caminho_sidra) else None

    painel = transforms.painel_mensal(sgs, ipca_indice)
    quality.checar_nao_vazio(painel, "gold painel_mensal")
    quality.checar_chave_unica(painel, ["mes"], "gold painel_mensal")
    storage.sobrescrever(painel, storage.caminho_tabela("gold", GOLD_PAINEL, raiz))
    return painel.height
