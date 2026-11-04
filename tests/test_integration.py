"""Teste ponta a ponta contra um Postgres de verdade.

Só roda com LAB_CDC_INTEGRACAO=1 e as variáveis PG* apontando para um banco com
wal_level=logical. Cria uma tabela e um slot próprios e apaga os dois no final.
"""

import os

import psycopg
import pytest

from lab_cdc.capture import confirmar, ler_slot

pytestmark = pytest.mark.integration

SLOT = "slot_teste_lab_cdc"


@pytest.fixture
def conn():
    if not os.environ.get("LAB_CDC_INTEGRACAO"):
        pytest.skip("defina LAB_CDC_INTEGRACAO=1 para rodar com um Postgres ligado")
    with psycopg.connect(autocommit=True) as c:
        yield c


def _limpar(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (SLOT,),
        )
        cur.execute("DROP TABLE IF EXISTS loja_teste")


def test_do_log_ao_evento_e_a_ordem_gravar_depois_confirmar(conn):
    _limpar(conn)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE loja_teste (id_loja INT PRIMARY KEY, nome TEXT, segmento TEXT)")
        cur.execute("ALTER TABLE loja_teste REPLICA IDENTITY FULL")
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (SLOT,))
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO loja_teste VALUES (1, 'Auto Center SP', 'PJ')")
            cur.execute("INSERT INTO loja_teste VALUES (2, 'Veículos Norte', 'PJ')")
            cur.execute("UPDATE loja_teste SET segmento = 'PF' WHERE id_loja = 1")
            cur.execute("DELETE FROM loja_teste WHERE id_loja = 2")

        eventos, ate_lsn = ler_slot(conn, SLOT)
        assert [e.operacao for e in eventos] == ["INSERT", "INSERT", "UPDATE", "DELETE"]
        assert eventos[3].dados["nome"] == "Veículos Norte"  # o delete chegou, com a linha inteira
        assert eventos[2].dados_anteriores["segmento"] == "PJ"

        # peek não consome: ler de novo devolve o mesmo
        assert len(ler_slot(conn, SLOT)[0]) == 4

        # só depois de gravar na landing é que se confirma
        confirmar(conn, SLOT, ate_lsn)
        assert ler_slot(conn, SLOT)[0] == []
    finally:
        _limpar(conn)
