"""Testa o MERGE da silver com Spark e Delta locais.

Roda só se pyspark e delta-spark estiverem instalados (são pesados e o CI não precisa
deles). Para rodar: pip install pyspark==3.5.3 delta-spark==3.2.1
"""

import json

import pytest

pytest.importorskip("delta")
pytest.importorskip("pyspark")

from lab_cdc.silver import aplicar_lote, criar_silver  # noqa: E402

SCHEMA = (
    "lsn STRING, lsn_ordem BIGINT, operacao STRING, tabela STRING, "
    "dados STRUCT<id_loja: INT, nome: STRING, segmento: STRING, atualizado_em: STRING>"
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    armazem = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(armazem))
        .config("spark.ui.enabled", "false")
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()


def _lote(spark, eventos):
    linhas = [json.dumps(e) for e in eventos]
    return spark.read.schema(SCHEMA).json(spark.sparkContext.parallelize(linhas))


def _ev(ordem, op, id_loja, segmento="PJ", nome="Loja"):
    dados = {"id_loja": id_loja, "nome": nome, "segmento": segmento, "atualizado_em": "x"}
    return {"lsn": f"0/{ordem:X}", "lsn_ordem": ordem, "operacao": op,
            "tabela": "public.loja", "dados": dados}


def _estado(spark, tabela):
    return {r.id_loja: r.segmento for r in spark.table(tabela).collect()}


LOTE = [
    _ev(1, "INSERT", 1), _ev(2, "INSERT", 2),
    _ev(3, "UPDATE", 1, segmento="PF"), _ev(4, "DELETE", 2),
]


def test_o_delete_chega_e_o_update_vale(spark):
    spark.sql("DROP TABLE IF EXISTS silver_loja_a")
    criar_silver(spark, "silver_loja_a")
    aplicar_lote(spark, _lote(spark, LOTE), "silver_loja_a")
    assert _estado(spark, "silver_loja_a") == {1: "PF"}


def test_reprocessar_o_mesmo_lote_nao_muda_nada(spark):
    spark.sql("DROP TABLE IF EXISTS silver_loja_b")
    criar_silver(spark, "silver_loja_b")
    aplicar_lote(spark, _lote(spark, LOTE), "silver_loja_b")
    aplicar_lote(spark, _lote(spark, LOTE), "silver_loja_b")
    assert _estado(spark, "silver_loja_b") == {1: "PF"}


def test_evento_antigo_nao_sobrescreve_o_novo(spark):
    spark.sql("DROP TABLE IF EXISTS silver_loja_c")
    criar_silver(spark, "silver_loja_c")
    aplicar_lote(spark, _lote(spark, [_ev(5, "UPDATE", 1, segmento="PF")]), "silver_loja_c")
    aplicar_lote(spark, _lote(spark, [_ev(3, "UPDATE", 1, segmento="PJ")]), "silver_loja_c")
    assert _estado(spark, "silver_loja_c") == {1: "PF"}


def test_lotes_em_sequencia(spark):
    spark.sql("DROP TABLE IF EXISTS silver_loja_d")
    criar_silver(spark, "silver_loja_d")
    aplicar_lote(spark, _lote(spark, LOTE[:2]), "silver_loja_d")
    assert _estado(spark, "silver_loja_d") == {1: "PJ", 2: "PJ"}
    aplicar_lote(spark, _lote(spark, LOTE[2:]), "silver_loja_d")
    assert _estado(spark, "silver_loja_d") == {1: "PF"}
