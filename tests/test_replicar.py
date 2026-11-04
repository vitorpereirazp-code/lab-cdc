"""Testa a ponte S3 -> Volume com um S3 falso (moto) e um Volume falso em memória."""

import io

import boto3
import pytest
from moto import mock_aws

from lab_cdc.replicar import pendentes, replicar

BUCKET = "lab-cdc-teste"
RAIZ = "/Volumes/lab_cdc/bronze/landing"


class VolumeFalso:
    """Imita o pedaço de WorkspaceClient().files que o replicar usa."""

    def __init__(self):
        self.arquivos: dict[str, bytes] = {}

    def upload(self, caminho, conteudo: io.BytesIO, overwrite=False):
        self.arquivos[caminho] = conteudo.read()

    def list_directory_contents(self, pasta):
        from databricks.sdk.errors import NotFound
        from databricks.sdk.service.files import DirectoryEntry

        filhos = {}
        for caminho in self.arquivos:
            if caminho.startswith(pasta + "/"):
                resto = caminho[len(pasta) + 1:]
                nome = resto.split("/")[0]
                e_pasta = "/" in resto
                filhos[nome] = DirectoryEntry(
                    name=nome, path=f"{pasta}/{nome}" + ("/" if e_pasta else ""),
                    is_directory=e_pasta)
        if not filhos:
            raise NotFound(f"{pasta} não existe")
        return list(filhos.values())


class ClienteFalso:
    def __init__(self):
        self.files = VolumeFalso()


@pytest.fixture
def s3():
    with mock_aws():
        cliente = boto3.client("s3", region_name="us-east-1")
        cliente.create_bucket(Bucket=BUCKET)
        yield cliente


def _por(s3, chave, corpo=b"{}\n"):
    s3.put_object(Bucket=BUCKET, Key=chave, Body=corpo)


def test_pendentes_compara_pelo_caminho_relativo():
    chaves = ["landing/loja/a.json", "landing/loja/b.json"]
    assert pendentes(chaves, {"loja/a.json"}) == ["landing/loja/b.json"]


def test_copia_tudo_na_primeira_vez(s3):
    _por(s3, "landing/loja/dt=2026-11-04/hora=17/eventos-0_1.json", b'{"x":1}\n')
    _por(s3, "landing/loja/dt=2026-11-04/hora=21/eventos-0_2.json")
    w = ClienteFalso()
    copiados = replicar(s3, w, BUCKET, RAIZ)
    assert len(copiados) == 2
    destino = f"{RAIZ}/loja/dt=2026-11-04/hora=17/eventos-0_1.json"
    assert w.files.arquivos[destino] == b'{"x":1}\n'


def test_segunda_execucao_nao_copia_nada(s3):
    _por(s3, "landing/loja/dt=2026-11-04/hora=17/eventos-0_1.json")
    w = ClienteFalso()
    replicar(s3, w, BUCKET, RAIZ)
    assert replicar(s3, w, BUCKET, RAIZ) == []


def test_so_copia_o_arquivo_novo(s3):
    _por(s3, "landing/loja/dt=2026-11-04/hora=17/eventos-0_1.json")
    w = ClienteFalso()
    replicar(s3, w, BUCKET, RAIZ)
    _por(s3, "landing/loja/dt=2026-11-04/hora=21/eventos-0_2.json")
    assert replicar(s3, w, BUCKET, RAIZ) == ["landing/loja/dt=2026-11-04/hora=21/eventos-0_2.json"]


def test_ignora_o_que_nao_e_json_e_fora_da_landing(s3):
    _por(s3, "landing/loja/leia-me.txt")
    _por(s3, "outra-pasta/x.json")
    assert replicar(s3, ClienteFalso(), BUCKET, RAIZ) == []


def test_nao_entra_em_checkpoints_do_volume(s3):
    w = ClienteFalso()
    w.files.arquivos[f"{RAIZ}/_checkpoints/bronze_loja/metadata"] = b"x"
    _por(s3, "landing/loja/a.json")
    assert replicar(s3, w, BUCKET, RAIZ) == ["landing/loja/a.json"]
