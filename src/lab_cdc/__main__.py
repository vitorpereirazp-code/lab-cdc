"""Uso: python -m lab_cdc  (ou: uv run python -m lab_cdc)

Lê o slot com peek (não consome) e imprime um evento por linha, em JSON.
As credenciais vêm das variáveis PG* do ambiente, nunca do código.
"""

import json
import os
from dataclasses import asdict

import psycopg

from lab_cdc.capture import ler_slot


def main() -> None:
    slot = os.environ.get("LAB_CDC_SLOT", "slot_lab")
    with psycopg.connect(autocommit=True) as conn:
        eventos, _ = ler_slot(conn, slot)
    for evento in eventos:
        print(json.dumps(asdict(evento), default=str, ensure_ascii=False))
    print(f"-- {len(eventos)} evento(s) no slot '{slot}'")


if __name__ == "__main__":
    main()
