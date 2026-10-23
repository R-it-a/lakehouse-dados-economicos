from datetime import date

import httpx
import pytest

from lakehouse.clients import ErroDeFonte, janelas


def test_janelas_divide_em_blocos_de_dez_anos():
    resultado = janelas(date(2000, 1, 1), date(2024, 6, 30))
    assert resultado == [
        (date(2000, 1, 1), date(2009, 12, 31)),
        (date(2010, 1, 1), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2024, 6, 30)),
    ]


def test_janelas_intervalo_invertido_e_vazio():
    assert janelas(date(2024, 1, 2), date(2024, 1, 1)) == []


def test_janelas_comecando_em_29_de_fevereiro():
    resultado = janelas(date(2020, 2, 29), date(2031, 1, 1))
    assert resultado[0] == (date(2020, 2, 29), date(2030, 2, 27))
    assert resultado[1][0] == date(2030, 2, 28)


def test_busca_junta_todas_as_janelas(cliente_mock):
    pedidos = []

    def handler(request: httpx.Request) -> httpx.Response:
        pedidos.append(dict(request.url.params))
        return httpx.Response(200, json=[{"data": request.url.params["dataInicial"], "valor": "1"}])

    registros = cliente_mock(handler).buscar_serie(432, date(2000, 1, 1), date(2024, 1, 1))

    assert len(pedidos) == 3
    assert [r["data"] for r in registros] == ["01/01/2000", "01/01/2010", "01/01/2020"]
    assert pedidos[0]["formato"] == "json"


def test_404_e_tratado_como_janela_sem_dados(cliente_mock):
    registros = cliente_mock(lambda request: httpx.Response(404)).buscar_serie(
        433, date(2024, 1, 1), date(2024, 1, 31)
    )
    assert registros == []


def test_tenta_de_novo_em_erro_temporario(cliente_mock):
    respostas = iter([httpx.Response(503), httpx.Response(200, json=[{"data": "x", "valor": "1"}])])
    registros = cliente_mock(lambda request: next(respostas)).buscar_serie(
        1, date(2024, 1, 1), date(2024, 1, 2)
    )
    assert len(registros) == 1


def test_desiste_depois_do_limite_de_tentativas(cliente_mock):
    with pytest.raises(ErroDeFonte):
        cliente_mock(lambda request: httpx.Response(500)).buscar_serie(
            1, date(2024, 1, 1), date(2024, 1, 2)
        )
