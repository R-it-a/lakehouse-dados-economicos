"""Teste de ponta a ponta: bronze -> silver -> gold, com fontes simuladas."""

from datetime import date

import polars as pl
import pytest

from lakehouse import pipelines, storage
from lakehouse.config import DATA_INICIO, SERIES_SGS
from lakehouse.quality import FalhaDeQualidade

from .conftest import SGSFalso, SidraFalso

HOJE = date(2024, 6, 14)
MESES = [f"{ano}{mes:02d}" for ano in (2023, 2024) for mes in range(1, 13)][:17]


def _rodar_sgs(cliente, lote, raiz, hoje=HOJE):
    for serie in SERIES_SGS:
        pipelines.ingerir_serie_sgs(serie.codigo, lote, cliente=cliente, hoje=hoje, raiz=raiz)
    return pipelines.atualizar_silver_sgs(lote, raiz=raiz)


def test_fluxo_completo(tmp_path):
    sgs = SGSFalso()
    _rodar_sgs(sgs, "lote-1", tmp_path)
    pipelines.validar_silver_sgs(hoje=HOJE, raiz=tmp_path)

    pipelines.ingerir_ipca_sidra("lote-1", cliente=SidraFalso(MESES), raiz=tmp_path)
    pipelines.atualizar_silver_sidra("lote-1", raiz=tmp_path)
    linhas = pipelines.construir_painel(raiz=tmp_path)

    painel = storage.ler(storage.caminho_tabela("gold", pipelines.GOLD_PAINEL, tmp_path))
    assert linhas == painel.height
    maio = painel.filter(pl.col("mes") == date(2024, 5, 1)).row(0, named=True)
    assert maio["selic_meta"] == 10.5
    assert maio["ipca_indice"] == 6016.0
    assert maio["ipca_12m"] == pytest.approx((1.004**12 - 1) * 100)


def test_segunda_carga_e_incremental_e_aplica_revisoes(tmp_path):
    _rodar_sgs(SGSFalso(), "lote-1", tmp_path)
    caminho = storage.caminho_tabela("silver", pipelines.SILVER_SGS, tmp_path)
    linhas_antes = storage.ler(caminho).height

    revisado = SGSFalso(valores={432: 10.75})
    _rodar_sgs(revisado, "lote-2", tmp_path)

    silver = storage.ler(caminho)
    assert silver.height == linhas_antes  # nada duplicado
    inicios = {codigo: inicio for codigo, inicio, _ in revisado.chamadas}
    assert inicios[432] > DATA_INICIO  # só relê a janela de revisão
    selic = silver.filter(pl.col("serie_codigo") == 432).sort("data")
    assert selic["valor"][-1] == 10.75  # dado recente revisado
    assert selic["valor"][0] == 10.5  # histórico antigo preservado


def test_sidra_incremental_pede_so_os_ultimos_meses(tmp_path):
    sidra = SidraFalso(MESES)
    pipelines.ingerir_ipca_sidra("lote-1", cliente=sidra, raiz=tmp_path)
    pipelines.atualizar_silver_sidra("lote-1", raiz=tmp_path)
    pipelines.ingerir_ipca_sidra("lote-2", cliente=sidra, raiz=tmp_path)

    assert sidra.periodos_pedidos == ["all", "last%2024"]


def test_fonte_vazia_falha_em_vez_de_passar_em_silencio(tmp_path):
    class Vazio(SGSFalso):
        def buscar_serie(self, codigo, inicio, fim):
            return []

    with pytest.raises(FalhaDeQualidade, match="nenhuma linha"):
        pipelines.ingerir_serie_sgs(432, "lote-1", cliente=Vazio(), hoje=HOJE, raiz=tmp_path)


def test_serie_parada_e_detectada(tmp_path):
    _rodar_sgs(SGSFalso(), "lote-1", tmp_path)
    with pytest.raises(FalhaDeQualidade, match="último dado"):
        pipelines.validar_silver_sgs(hoje=date(2024, 9, 1), raiz=tmp_path)
