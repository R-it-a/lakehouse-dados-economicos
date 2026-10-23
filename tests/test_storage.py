from datetime import date

import polars as pl

from lakehouse import storage


def _df(valores: dict[date, float]) -> pl.DataFrame:
    return pl.DataFrame({"data": list(valores), "valor": list(valores.values())})


def test_upsert_e_idempotente(tmp_path):
    caminho = str(tmp_path / "silver" / "t")
    dados = _df({date(2024, 1, 1): 1.0, date(2024, 1, 2): 2.0})

    storage.upsert(dados, caminho, ["data"])
    storage.upsert(dados, caminho, ["data"])

    assert storage.ler(caminho).height == 2


def test_upsert_atualiza_e_insere(tmp_path):
    caminho = str(tmp_path / "silver" / "t")
    storage.upsert(_df({date(2024, 1, 1): 1.0, date(2024, 1, 2): 2.0}), caminho, ["data"])

    storage.upsert(_df({date(2024, 1, 2): 2.5, date(2024, 1, 3): 3.0}), caminho, ["data"])

    resultado = storage.ler(caminho).sort("data")
    assert resultado["valor"].to_list() == [1.0, 2.5, 3.0]


def test_anexar_mantem_historico(tmp_path):
    caminho = str(tmp_path / "bronze" / "t")
    dados = _df({date(2024, 1, 1): 1.0}).with_columns(serie=pl.lit(1))

    storage.anexar(dados, caminho, ["serie"])
    storage.anexar(dados, caminho, ["serie"])

    assert storage.ler(caminho).height == 2


def test_caminho_por_camada(tmp_path):
    assert storage.caminho_tabela("gold", "painel", tmp_path) == str(tmp_path / "gold" / "painel")
