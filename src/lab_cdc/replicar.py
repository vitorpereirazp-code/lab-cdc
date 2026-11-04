"""Ponte S3 -> Volume do Databricks.

Numa conta paga, o Databricks leria o bucket direto, por uma external location do
Unity Catalog. No Free Edition isso não existe, então este passo copia os arquivos
novos do S3 para o Volume. A regra é a mesma de toda landing: só acrescenta.

Uso (de dentro da pasta do projeto, com AWS_PROFILE definido):
    uv run python -m lab_cdc.replicar --bucket lab-cdc-vitor-2026
"""

from __future__ import annotations

import argparse
import io
import os

PREFIXO_S3 = "landing/"
VOLUME_PADRAO = "/Volumes/lab_cdc/bronze/landing"


def listar_s3(s3, bucket: str, prefixo: str = PREFIXO_S3) -> list[str]:
    """Chaves .json do bucket dentro do prefixo, em ordem. Pagina, porque o S3
    devolve no máximo 1000 objetos por chamada."""
    chaves: list[str] = []
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixo):
        chaves += [o["Key"] for o in pagina.get("Contents", []) if o["Key"].endswith(".json")]
    return sorted(chaves)


def listar_volume(w, raiz: str) -> set[str]:
    """Caminhos relativos (ex.: loja/dt=.../x.json) que já existem no Volume."""
    from databricks.sdk.errors import NotFound

    existentes: set[str] = set()
    pastas = [raiz]
    while pastas:
        pasta = pastas.pop()
        try:
            itens = list(w.files.list_directory_contents(pasta))
        except NotFound:
            continue  # o Volume ainda está vazio
        for item in itens:
            if item.is_directory:
                if not item.name.startswith("_"):  # pula _checkpoints
                    pastas.append(item.path.rstrip("/"))
            else:
                existentes.add(item.path[len(raiz) + 1:])
    return existentes


def pendentes(chaves_s3: list[str], ja_no_volume: set[str], prefixo: str = PREFIXO_S3) -> list[str]:
    """Função pura: quais chaves do S3 ainda não chegaram ao Volume."""
    return [k for k in chaves_s3 if k[len(prefixo):] not in ja_no_volume]


def replicar(s3, w, bucket: str, raiz_volume: str = VOLUME_PADRAO,
             prefixo: str = PREFIXO_S3) -> list[str]:
    """Copia para o Volume só o que falta. Rodar duas vezes não copia nada de novo."""
    copiar = pendentes(listar_s3(s3, bucket, prefixo), listar_volume(w, raiz_volume), prefixo)
    for chave in copiar:
        corpo = s3.get_object(Bucket=bucket, Key=chave)["Body"].read()
        destino = f"{raiz_volume}/{chave[len(prefixo):]}"
        w.files.upload(destino, io.BytesIO(corpo), overwrite=True)
    return copiar


def main() -> None:
    import boto3
    from databricks.sdk import WorkspaceClient

    parser = argparse.ArgumentParser(prog="lab_cdc.replicar")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--volume", default=VOLUME_PADRAO)
    parser.add_argument("--perfil-databricks",
                        default=os.environ.get("DATABRICKS_CONFIG_PROFILE", "lab"))
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.perfil_databricks)
    copiados = replicar(boto3.client("s3"), w, args.bucket, args.volume)
    for chave in copiados:
        print(f"copiado: s3://{args.bucket}/{chave}")
    print(f"-- {len(copiados)} arquivo(s) novo(s) copiado(s) para {args.volume}")


if __name__ == "__main__":
    main()
