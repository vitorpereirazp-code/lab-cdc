"""Uso:
    python -m lab_cdc                      # só olha o slot (peek), não consome nada
    python -m lab_cdc --gravar landing/    # peek -> grava na landing -> confirma o slot

As credenciais vêm das variáveis PG* do ambiente, nunca do código.
"""

import argparse
import os
from pathlib import Path

import psycopg

from lab_cdc.capture import confirmar, ler_slot
from lab_cdc.landing import gravar, para_linha


def main() -> None:
    parser = argparse.ArgumentParser(prog="lab_cdc")
    parser.add_argument("--slot", default=os.environ.get("LAB_CDC_SLOT", "slot_lab"))
    parser.add_argument("--gravar", type=Path, metavar="PASTA",
                        help="grava os eventos nesta pasta e só então avança o slot")
    args = parser.parse_args()

    with psycopg.connect(autocommit=True) as conn:
        eventos, ate_lsn = ler_slot(conn, args.slot)

        if args.gravar is None:
            for evento in eventos:
                print(para_linha(evento))
            print(f"-- {len(eventos)} evento(s) no slot '{args.slot}' (nada foi consumido)")
            return

        arquivo = gravar(eventos, args.gravar)
        # a ordem que importa: gravar primeiro, confirmar depois
        if ate_lsn:
            confirmar(conn, args.slot, ate_lsn)
        print(f"-- {len(eventos)} evento(s) gravados em {arquivo}; slot avançado até {ate_lsn}")


if __name__ == "__main__":
    main()
