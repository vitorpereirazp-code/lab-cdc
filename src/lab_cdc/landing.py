"""Etapa 4 do laboratório: gravar os eventos na landing, sem alterar nada.

Landing é append-only: não corrige, não deduplica, não transforma. Existe para que
seja possível reprocessar do zero sem voltar à origem.

Por enquanto a landing é uma pasta local, enviada para um Volume do Databricks com
`databricks fs cp`. Quando o S3 existir, só o destino muda: o formato fica igual.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from lab_cdc.capture import EventoCDC


def lsn_para_int(lsn: str) -> int:
    """Converte o LSN do Postgres ("16/B374D848") num inteiro ordenável.

    Comparar LSN como texto está errado: "0/9" > "0/10" em ordem alfabética.
    """
    alto, baixo = lsn.split("/")
    return (int(alto, 16) << 32) | int(baixo, 16)


def para_linha(evento: EventoCDC) -> str:
    registro = asdict(evento)
    registro["lsn_ordem"] = lsn_para_int(evento.lsn)
    return json.dumps(registro, default=str, ensure_ascii=False)


def gravar(eventos: list[EventoCDC], raiz: Path, agora: datetime | None = None) -> Path | None:
    """Grava um lote como JSON Lines, particionado por data e hora.

    O LSN do primeiro evento entra no nome do arquivo: regravar o mesmo lote
    sobrescreve o arquivo em vez de duplicar. É isso que torna a gravação idempotente.
    """
    if not eventos:
        return None
    agora = agora or datetime.now(UTC)
    tabela = eventos[0].tabela.split(".")[-1]
    pasta = raiz / tabela / f"dt={agora:%Y-%m-%d}" / f"hora={agora:%H}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / f"eventos-{eventos[0].lsn.replace('/', '_')}.json"
    tmp = arquivo.with_suffix(".tmp")
    tmp.write_text("\n".join(para_linha(e) for e in eventos) + "\n", encoding="utf-8")
    tmp.replace(arquivo)  # troca atômica: nunca fica um arquivo pela metade
    return arquivo
