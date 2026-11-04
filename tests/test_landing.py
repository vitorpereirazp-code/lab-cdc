import json
from datetime import UTC, datetime

from lab_cdc.capture import EventoCDC
from lab_cdc.landing import gravar, lsn_para_int

AGORA = datetime(2026, 11, 4, 14, 5, tzinfo=UTC)


def _ev(lsn, op="INSERT", dados=None):
    return EventoCDC(lsn, op, "public.loja", dados or {"id_loja": 1}, AGORA)


def test_lsn_ordena_como_numero_e_nao_como_texto():
    assert lsn_para_int("0/9") < lsn_para_int("0/10")
    assert lsn_para_int("0/FFFFFFFF") < lsn_para_int("1/0")


def test_lote_vazio_nao_grava_nada(tmp_path):
    assert gravar([], tmp_path) is None
    assert list(tmp_path.iterdir()) == []


def test_grava_particionado_e_com_ordem(tmp_path):
    arquivo = gravar([_ev("0/1A"), _ev("0/1B", "DELETE")], tmp_path, agora=AGORA)
    esperado = "loja/dt=2026-11-04/hora=14/eventos-0_1A.json"
    assert arquivo.relative_to(tmp_path).as_posix() == esperado
    linhas = [json.loads(t) for t in arquivo.read_text(encoding="utf-8").splitlines()]
    assert [r["operacao"] for r in linhas] == ["INSERT", "DELETE"]
    assert linhas[0]["lsn_ordem"] == 0x1A


def test_regravar_o_mesmo_lote_nao_duplica(tmp_path):
    lote = [_ev("0/1A")]
    gravar(lote, tmp_path, agora=AGORA)
    gravar(lote, tmp_path, agora=AGORA)
    assert len(list(tmp_path.rglob("*.json"))) == 1
    assert not list(tmp_path.rglob("*.tmp"))
