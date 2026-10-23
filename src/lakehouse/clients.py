"""Clientes HTTP das fontes públicas: Banco Central (SGS) e IBGE (SIDRA)."""

from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import httpx

STATUS_REPETIVEIS = {429, 500, 502, 503, 504}


class ErroDeFonte(RuntimeError):
    """A fonte respondeu de forma inesperada depois de todas as tentativas."""


def janelas(inicio: date, fim: date, anos: int = 10) -> list[tuple[date, date]]:
    """Divide um intervalo em janelas consecutivas de no máximo `anos` anos.

    A API do SGS limita consultas de séries diárias a 10 anos por requisição.
    """
    if inicio > fim:
        return []
    resultado = []
    atual = inicio
    while atual <= fim:
        try:
            limite = atual.replace(year=atual.year + anos) - timedelta(days=1)
        except ValueError:  # 29 de fevereiro
            limite = atual.replace(year=atual.year + anos, day=28) - timedelta(days=1)
        fim_janela = min(limite, fim)
        resultado.append((atual, fim_janela))
        atual = fim_janela + timedelta(days=1)
    return resultado


class _ClienteBase:
    def __init__(
        self,
        cliente: httpx.Client | None = None,
        max_tentativas: int = 4,
        espera_segundos: float = 2.0,
        timeout: float = 60.0,
    ) -> None:
        self._cliente = cliente or httpx.Client(timeout=timeout, follow_redirects=True)
        self._max_tentativas = max_tentativas
        self._espera = espera_segundos

    def _get(self, url: str, params: dict[str, str] | None = None) -> httpx.Response:
        """GET com novas tentativas e espera exponencial para falhas temporárias."""
        ultimo_erro: Exception | None = None
        for tentativa in range(self._max_tentativas):
            try:
                resposta = self._cliente.get(url, params=params)
            except httpx.TransportError as erro:
                ultimo_erro = erro
            else:
                if resposta.status_code not in STATUS_REPETIVEIS:
                    return resposta
                ultimo_erro = ErroDeFonte(f"HTTP {resposta.status_code} em {url}")
            if tentativa < self._max_tentativas - 1:
                time.sleep(self._espera * 2**tentativa)
        raise ErroDeFonte(f"Falha após {self._max_tentativas} tentativas: {url}") from ultimo_erro


class ClienteSGS(_ClienteBase):
    """Cliente da API de séries temporais do Banco Central."""

    URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

    def buscar_serie(self, codigo: int, inicio: date, fim: date) -> list[dict[str, Any]]:
        """Retorna os registros brutos ({"data": "dd/mm/aaaa", "valor": "1.23"}) do período."""
        registros: list[dict[str, Any]] = []
        for janela_inicio, janela_fim in janelas(inicio, fim):
            resposta = self._get(
                self.URL.format(codigo=codigo),
                params={
                    "formato": "json",
                    "dataInicial": janela_inicio.strftime("%d/%m/%Y"),
                    "dataFinal": janela_fim.strftime("%d/%m/%Y"),
                },
            )
            if resposta.status_code == 404:
                continue  # a API responde 404 quando a janela não tem observações
            resposta.raise_for_status()
            dados = resposta.json()
            if not isinstance(dados, list):
                raise ErroDeFonte(f"Resposta inesperada da série {codigo}: {dados!r}")
            registros.extend(dados)
        return registros


class ClienteSidra(_ClienteBase):
    """Cliente da API do SIDRA (IBGE)."""

    URL = "https://apisidra.ibge.gov.br/values/t/{tabela}/n1/all/v/{variavel}/p/{periodos}"

    def buscar_tabela(
        self, tabela: int, variavel: int, periodos: str = "all"
    ) -> list[dict[str, Any]]:
        """Retorna a resposta bruta do SIDRA: a primeira linha é o cabeçalho com os rótulos."""
        resposta = self._get(
            self.URL.format(tabela=tabela, variavel=variavel, periodos=periodos)
        )
        resposta.raise_for_status()
        dados = resposta.json()
        if not isinstance(dados, list) or len(dados) < 1:
            raise ErroDeFonte(f"Resposta inesperada da tabela {tabela}: {dados!r}")
        return dados
