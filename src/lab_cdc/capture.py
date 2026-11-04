"""Etapa 3 do laboratório: ler o slot de replicação e produzir eventos estruturados.

Espelha o papel do AWS DMS: lê o log do banco de origem, sem consultar as tabelas.
Por isso o DELETE aparece: a linha já não existe na tabela, mas continua no log.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class EventoCDC:
    """Um evento de mudança. Imutável de propósito: evento capturado não se altera depois."""

    lsn: str  # posição no log: é a ordem verdadeira
    operacao: str  # INSERT | UPDATE | DELETE | TRUNCATE
    tabela: str
    dados: dict  # linha nova (INSERT/UPDATE) ou linha apagada (DELETE)
    capturado_em: datetime
    dados_anteriores: dict | None = None  # só no UPDATE, quando o log traz o valor antigo


_LINHA = re.compile(
    r"^table (?P<tabela>\S+): (?P<op>INSERT|UPDATE|DELETE|TRUNCATE):(?P<resto>.*)$",
    re.DOTALL,
)

# Um token é um rótulo (old-key:/new-tuple:) ou uma coluna no formato nome[tipo]:valor.
# O valor é texto entre aspas simples (com '' para aspas internas) ou um termo sem espaço.
# Como o regex consome o valor entre aspas inteiro, um texto que contenha "new-tuple:"
# dentro de um valor não é confundido com o rótulo.
_TOKEN = re.compile(
    r"(?P<rotulo>old-key:|new-tuple:)"
    r"|(?P<nome>\w+)\[(?P<tipo>[^:]+?)\]:(?P<valor>'(?:[^']|'')*'|\S+)"
)

_INTEIROS = {"integer", "bigint", "smallint"}
_DECIMAIS = {"double precision", "real"}


def _converter(tipo: str, valor: str):
    if valor == "null":  # null sem aspas é NULL; 'null' com aspas é o texto "null"
        return None
    if valor.startswith("'"):
        return valor[1:-1].replace("''", "'")
    if tipo in _INTEIROS:
        return int(valor)
    if tipo == "boolean":
        return valor == "true"
    if tipo in _DECIMAIS:
        return float(valor)
    return valor  # numeric e demais ficam como texto, preservando a precisão


def _parse_colunas(texto: str) -> tuple[dict, dict]:
    nova: dict = {}
    antiga: dict = {}
    destino = nova
    for m in _TOKEN.finditer(texto):
        rotulo = m.group("rotulo")
        if rotulo == "old-key:":
            destino = antiga
        elif rotulo == "new-tuple:":
            destino = nova
        else:
            destino[m.group("nome")] = _converter(m.group("tipo"), m.group("valor"))
    return nova, antiga


def parse_evento(lsn: str, linha: str, agora: datetime | None = None) -> EventoCDC | None:
    """Transforma uma linha do log em evento. Função pura: sem I/O, fácil de testar.

    `agora` é injetável para o teste não depender do relógio.
    """
    m = _LINHA.match(linha)
    if not m:
        return None  # BEGIN e COMMIT não são mudanças
    nova, antiga = _parse_colunas(m.group("resto"))
    return EventoCDC(
        lsn=lsn,
        operacao=m.group("op"),
        tabela=m.group("tabela"),
        dados=nova,
        capturado_em=agora or datetime.now(UTC),
        dados_anteriores=antiga or None,
    )


def ler_slot(conn, slot: str, consumir: bool = False) -> tuple[list[EventoCDC], str | None]:
    """Lê o slot e devolve (eventos, ate_lsn).

    `ate_lsn` é o LSN da última linha lida, o COMMIT da última transação. É ele que se
    passa para `confirmar` depois que os eventos estiverem gravados na landing.

    consumir=False usa peek (não avança a posição). consumir=True usa get, que lê e
    consome: um evento consumido e não gravado se perde para sempre. Prefira peek,
    grave e só então chame `confirmar`.
    """
    fn = "get" if consumir else "peek"  # valor fixo, nunca vem de entrada externa
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT lsn::text, data FROM pg_logical_slot_{fn}_changes(%s, NULL, NULL)",
            (slot,),
        )
        linhas = cur.fetchall()
    eventos = [e for lsn, dado in linhas if (e := parse_evento(lsn, dado))]
    ate_lsn = linhas[-1][0] if linhas else None
    return eventos, ate_lsn


def capturar(conn, slot: str, consumir: bool = False) -> list[EventoCDC]:
    """Atalho que devolve só os eventos."""
    return ler_slot(conn, slot, consumir)[0]


def confirmar(conn, slot: str, ate_lsn: str) -> None:
    """Avança a posição do slot até `ate_lsn`.

    A ordem certa do pipeline: ler_slot (peek) -> gravar na landing -> confirmar.
    Se a gravação falhar, a posição não avançou e o próximo ciclo lê os mesmos eventos.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (slot, ate_lsn))
    conn.commit()
