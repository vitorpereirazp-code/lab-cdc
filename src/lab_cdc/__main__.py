"""Uso:
    python -m lab_cdc                          # só olha o slot (peek), não consome nada
    python -m lab_cdc --gravar landing         # peek -> grava na pasta local -> confirma o slot
    python -m lab_cdc --s3 lab-cdc-vitor-2026  # peek -> grava no S3 -> confirma o slot

As credenciais vêm do ambiente (variáveis PG* e AWS_PROFILE), nunca do código.
"""

import argparse
import os
from pathlib import Path

import psycopg

from lab_cdc.capture import confirmar, ler_slot
from lab_cdc.landing import gravar, gravar_s3, para_linha


def main() -> None:
    parser = argparse.ArgumentParser(prog="lab_cdc")
    parser.add_argument("--slot", default=os.environ.get("LAB_CDC_SLOT", "slot_lab"))
    parser.add_argument("--gravar", type=Path, metavar="PASTA",
                        help="grava os eventos nesta pasta e só então avança o slot")
    parser.add_argument("--s3", metavar="BUCKET",
                        help="grava no bucket S3 e só então avança o slot")
    args = parser.parse_args()

    with psycopg.connect(autocommit=True) as conn:
        eventos, ate_lsn = ler_slot(conn, args.slot)

        if args.gravar is None and args.s3 is None:
            for evento in eventos:
                print(para_linha(evento))
            print(f"-- {len(eventos)} evento(s) no slot '{args.slot}' (nada foi consumido)")
            return

        if args.s3:
            import boto3
            destino = gravar_s3(eventos, boto3.client("s3"), args.s3)
        else:
            destino = gravar(eventos, args.gravar)
        # a ordem que importa: gravar primeiro, confirmar depois
        if ate_lsn:
            confirmar(conn, args.slot, ate_lsn)
        print(f"-- {len(eventos)} evento(s) gravados em {destino}; slot avançado até {ate_lsn}")


if __name__ == "__main__":
    main()