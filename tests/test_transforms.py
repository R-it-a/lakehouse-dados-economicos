from datetime import UTC, date, datetime

import polars as pl
import pytest

from lakehouse import transforms
from lakehouse.config import SERIES_POR_CODIGO

SELIC = SERIES_POR_CODIGO[432]
T0 = datetime(2024, 1, 10, tzinfo=UTC)
T1 = datetime(2024, 1, 11, tzinfo=UTC)


def test_inicio_incremental_primeira_carga_busca_tudo():
    assert transforms.inicio_incremental(None, date(2000, 1, 1), 90) == date(2000, 1, 1)


def test_inicio_incremental_rele_janela_de_revisao():
    assert transforms.inicio_incremental(date(2024, 4, 30), date(2000, 1, 1), 90) == date(
        2024, 1, 31
    )


def test_inicio_incremental_nao_volta_antes_do_inicio():
    assert transforms.inicio_incremental(date(2000, 2, 1), date(2000, 1, 1), 90) == date(
        2000, 1, 1
    )


def test_bronze_guarda_resposta_como_texto():
    bronze = transforms.sgs_para_bronze(
        [{"data": "02/01/2024", "valor": "11.75"}], SELIC, "lote-1", T0
    )
    assert bronze.schema == pl.Schema(transforms.ESQUEMA_BRONZE_SGS)
    assert bronze.row(0, named=True)["valor_raw"] == "11.75"


def test_silver_tipa_e_fica_com_a_versao_mais_recente():
    antigo = transforms.sgs_para_bronze(
        [{"data": "02/01/2024", "valor": "11.75"}, {"data": "03/01/2024", "valor": ""}],
        SELIC,
        "lote-1",
        T0,
    )
    revisado = transforms.sgs_para_bronze(
        [{"data": "02/01/2024", "valor": "11.50"}], SELIC, "lote-2", T1
    )

    silver = transforms.sgs_bronze_para_silver(pl.concat([revisado, antigo]))

    assert silver.schema == pl.Schema(transforms.ESQUEMA_SILVER_SGS)
    assert silver.height == 1  # a linha com valor vazio é descartada
    linha = silver.row(0, named=True)
    assert linha["data"] == date(2024, 1, 2)
    assert linha["valor"] == 11.50
    assert linha["atualizado_em"] == T1


def test_silver_falha_com_data_em_formato_inesperado():
    bronze = transforms.sgs_para_bronze([{"data": "2024-01-02", "valor": "1"}], SELIC, "l", T0)
    with pytest.raises((pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError)):
        transforms.sgs_bronze_para_silver(bronze)


def test_sidra_usa_o_cabecalho_para_achar_o_periodo():
    resposta = [
        {"D1C": "Brasil (Código)", "D2C": "Mês (Código)", "V": "Valor"},
        {"D1C": "1", "D2C": "202401", "V": "6900.12"},
        {"D1C": "1", "D2C": "202402", "V": "..."},
    ]
    bronze = transforms.sidra_para_bronze(resposta, 1737, 2266, "lote-1", T0)
    silver = transforms.sidra_bronze_para_silver(bronze)

    assert bronze["periodo_codigo"].to_list() == ["202401", "202402"]
    assert silver["data"].to_list() == [date(2024, 1, 1)]
    assert silver["valor"].to_list() == [6900.12]


def _silver(serie_nome: str, pontos: list[tuple[date, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "serie_codigo": [0] * len(pontos),
            "serie_nome": [serie_nome] * len(pontos),
            "data": [d for d, _ in pontos],
            "valor": [v for _, v in pontos],
            "atualizado_em": [T0] * len(pontos),
        },
        schema=transforms.ESQUEMA_SILVER_SGS,
    )


def test_painel_mensal_calcula_indicadores():
    meses = [date(2023, m, 1) for m in range(1, 13)]
    sgs = pl.concat(
        [
            _silver("ipca_mensal", [(m, 1.0) for m in meses]),
            _silver("igpm_mensal", [(m, 0.5) for m in meses]),
            _silver(
                "selic_meta",
                [(date(2023, 12, 1), 12.25), (date(2023, 12, 29), 11.75)]
                + [(date(2023, m, 15), 13.75) for m in range(1, 12)],
            ),
            _silver("dolar_venda", [(date(2023, 12, 1), 4.80), (date(2023, 12, 29), 5.00)]),
        ]
    )

    painel = transforms.painel_mensal(sgs)
    dezembro = painel.filter(pl.col("mes") == date(2023, 12, 1)).row(0, named=True)

    assert painel.height == 12
    assert dezembro["selic_meta"] == 11.75  # último valor do mês
    assert dezembro["dolar_medio"] == pytest.approx(4.90)
    assert dezembro["dolar_fim"] == 5.00
    assert dezembro["ipca_12m"] == pytest.approx((1.01**12 - 1) * 100)
    assert dezembro["juro_real_ex_post"] == pytest.approx(
        (1.1175 / 1.01**12 - 1) * 100
    )
    # sem 12 meses de histórico, o acumulado fica nulo
    assert painel.filter(pl.col("mes") == date(2023, 11, 1))["ipca_12m"].item() is None


def test_painel_inclui_indice_do_ibge_quando_disponivel():
    sgs = _silver("ipca_mensal", [(date(2024, 1, 1), 0.42)])
    ipca_indice = pl.DataFrame({"data": [date(2024, 1, 1)], "valor": [6900.0]})

    painel = transforms.painel_mensal(sgs, ipca_indice)

    assert painel["ipca_indice"].to_list() == [6900.0]
