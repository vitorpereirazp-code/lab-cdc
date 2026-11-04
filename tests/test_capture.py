"""Testes da função pura parse_evento. Não precisam do banco ligado."""

from datetime import UTC, datetime

from lab_cdc.capture import parse_evento

AGORA = datetime(2026, 11, 4, 13, 0, tzinfo=UTC)
TS = "atualizado_em[timestamp without time zone]:'2026-11-04 12:59:45.821386'"


def test_ignora_begin_e_commit():
    assert parse_evento("0/182C0", "BEGIN 754") is None
    assert parse_evento("0/182C3", "COMMIT 754") is None


def test_insert_traz_a_linha_nova_com_tipos():
    linha = (
        "table public.loja: INSERT: id_loja[integer]:1 "
        f"nome[text]:'Auto Center SP' segmento[text]:'PJ' {TS}"
    )
    ev = parse_evento("0/182C8", linha, agora=AGORA)
    assert ev.operacao == "INSERT"
    assert ev.tabela == "public.loja"
    assert ev.lsn == "0/182C8"
    assert ev.dados["id_loja"] == 1  # inteiro, não texto
    assert ev.dados["nome"] == "Auto Center SP"
    assert ev.capturado_em == AGORA
    assert ev.dados_anteriores is None


def test_delete_preserva_linha_completa():
    # graças ao REPLICA IDENTITY FULL, o log traz a linha inteira, não só a chave
    linha = (
        "table public.loja: DELETE: id_loja[integer]:2 "
        f"nome[text]:'Veículos Norte' segmento[text]:'PJ' {TS}"
    )
    ev = parse_evento("0/182C2", linha)
    assert ev.operacao == "DELETE"
    assert ev.dados["id_loja"] == 2
    assert ev.dados["nome"] == "Veículos Norte"
    assert ev.dados["segmento"] == "PJ"


def test_update_separa_valor_antigo_e_novo():
    linha = (
        "table public.loja: UPDATE: old-key: "
        f"id_loja[integer]:1 nome[text]:'Auto Center SP' segmento[text]:'PJ' {TS} "
        "new-tuple: "
        f"id_loja[integer]:1 nome[text]:'Auto Center SP' segmento[text]:'PF' {TS}"
    )
    ev = parse_evento("0/182C5", linha)
    assert ev.operacao == "UPDATE"
    assert ev.dados_anteriores["segmento"] == "PJ"
    assert ev.dados["segmento"] == "PF"


def test_update_sem_old_key_nao_inventa_valor_antigo():
    linha = f"table public.loja: UPDATE: id_loja[integer]:1 segmento[text]:'PF' {TS}"
    ev = parse_evento("0/182C5", linha)
    assert ev.dados["segmento"] == "PF"
    assert ev.dados_anteriores is None


def test_aspas_dentro_do_texto():
    linha = "table public.loja: INSERT: id_loja[integer]:3 nome[text]:'O''Brien & Filhos'"
    assert parse_evento("0/1", linha).dados["nome"] == "O'Brien & Filhos"


def test_texto_com_espaco_e_marcador_falso_nao_confunde_o_parser():
    linha = "table public.loja: INSERT: id_loja[integer]:4 nome[text]:'x new-tuple: y[integer]:9'"
    ev = parse_evento("0/1", linha)
    assert ev.dados == {"id_loja": 4, "nome": "x new-tuple: y[integer]:9"}


def test_null_sem_aspas_e_none_e_texto_null_continua_texto():
    linha = "table public.loja: INSERT: id_loja[integer]:5 segmento[text]:null nome[text]:'null'"
    ev = parse_evento("0/1", linha)
    assert ev.dados["segmento"] is None
    assert ev.dados["nome"] == "null"


def test_booleano_e_decimal():
    linha = "table public.t: INSERT: ativo[boolean]:true nota[double precision]:4.5"
    ev = parse_evento("0/1", linha)
    assert ev.dados == {"ativo": True, "nota": 4.5}


def test_truncate_vira_evento_sem_dados():
    ev = parse_evento("0/1", "table public.loja: TRUNCATE: (no-flags)")
    assert ev.operacao == "TRUNCATE"
    assert ev.dados == {}
