"""Catálogo das fontes e parâmetros do lakehouse."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DATA_INICIO = date(2000, 1, 1)
"""Primeira data buscada na carga inicial de cada série."""

JANELA_REVISAO_DIAS = 90
"""Quantos dias para trás cada carga incremental relê, para capturar revisões da fonte."""


@dataclass(frozen=True)
class SerieSGS:
    """Série do Sistema Gerenciador de Séries Temporais (SGS) do Banco Central."""

    codigo: int
    nome: str
    descricao: str
    frequencia: str  # "D" (diária) ou "M" (mensal)
    unidade: str
    minimo: float
    maximo: float

    @property
    def atraso_maximo_dias(self) -> int:
        """Quantos dias o dado mais recente pode ter antes de a série ser considerada parada."""
        return 10 if self.frequencia == "D" else 75


SERIES_SGS: tuple[SerieSGS, ...] = (
    SerieSGS(432, "selic_meta", "Meta da taxa Selic definida pelo Copom", "D", "% a.a.", 0, 50),
    SerieSGS(433, "ipca_mensal", "IPCA - variação mensal", "M", "%", -5, 5),
    SerieSGS(189, "igpm_mensal", "IGP-M - variação mensal", "M", "%", -10, 10),
    SerieSGS(1, "dolar_venda", "Dólar americano (venda) - taxa livre", "D", "R$/US$", 0.5, 20),
)

SERIES_POR_CODIGO = {serie.codigo: serie for serie in SERIES_SGS}


@dataclass(frozen=True)
class TabelaSidra:
    """Tabela do SIDRA, o sistema de recuperação automática do IBGE."""

    tabela: int
    variavel: int
    nome: str
    descricao: str


IPCA_INDICE = TabelaSidra(
    tabela=1737,
    variavel=2266,
    nome="ipca_indice",
    descricao="IPCA - número-índice (base dez/1993 = 100)",
)


def raiz_lake() -> Path:
    """Diretório raiz do lakehouse, configurável pela variável LAKEHOUSE_ROOT."""
    return Path(os.environ.get("LAKEHOUSE_ROOT", "data/lake"))
