from __future__ import annotations

from datetime import date, timedelta

import httpx
import pytest

from lakehouse.clients import ClienteSGS, ClienteSidra


def registros_diarios(inicio: date, fim: date, valor: float) -> list[dict[str, str]]:
    """Simula a resposta do SGS para uma série diária (só dias úteis)."""
    registros = []
    dia = inicio
    while dia <= fim:
        if dia.weekday() < 5:
            registros.append({"data": dia.strftime("%d/%m/%Y"), "valor": f"{valor:.2f}"})
        dia += timedelta(days=1)
    return registros


def registros_mensais(inicio: date, fim: date, valor: float) -> list[dict[str, str]]:
    """Simula a resposta do SGS para uma série mensal (dia 1 de cada mês)."""
    registros = []
    ano, mes = inicio.year, inicio.month
    while date(ano, mes, 1) <= fim:
        registros.append({"data": f"01/{mes:02d}/{ano}", "valor": f"{valor:.2f}"})
        ano, mes = (ano + 1, 1) if mes == 12 else (ano, mes + 1)
    return registros


class SGSFalso(ClienteSGS):
    """Cliente do SGS que responde com dados sintéticos, sem rede."""

    VALORES = {432: 10.5, 433: 0.4, 189: 0.5, 1: 5.25}

    def __init__(self, valores: dict[int, float] | None = None) -> None:
        self.valores = {**self.VALORES, **(valores or {})}
        self.chamadas: list[tuple[int, date, date]] = []

    def buscar_serie(self, codigo, inicio, fim):
        self.chamadas.append((codigo, inicio, fim))
        valor = self.valores[codigo]
        if codigo in (433, 189):
            # séries mensais: o dado do mês sai no mês seguinte
            return registros_mensais(inicio, fim.replace(day=1) - timedelta(days=1), valor)
        return registros_diarios(inicio, fim, valor)


class SidraFalso(ClienteSidra):
    def __init__(self, meses: list[str], valor_inicial: float = 6000.0) -> None:
        self.meses = meses
        self.valor_inicial = valor_inicial
        self.periodos_pedidos: list[str] = []

    def buscar_tabela(self, tabela, variavel, periodos="all"):
        self.periodos_pedidos.append(periodos)
        cabecalho = {
            "NC": "Nível Territorial (Código)",
            "D1C": "Brasil (Código)",
            "D2C": "Mês (Código)",
            "D2N": "Mês",
            "D3C": "Variável (Código)",
            "V": "Valor",
        }
        linhas = [
            {"NC": "1", "D1C": "1", "D2C": mes, "D2N": mes, "D3C": str(variavel),
             "V": f"{self.valor_inicial + i:.2f}"}
            for i, mes in enumerate(self.meses)
        ]
        return [cabecalho, *linhas]


@pytest.fixture
def cliente_mock():
    """Fábrica de ClienteSGS com um transporte HTTP simulado."""

    def fabrica(handler):
        cliente = httpx.Client(transport=httpx.MockTransport(handler))
        return ClienteSGS(cliente=cliente, espera_segundos=0)

    return fabrica
