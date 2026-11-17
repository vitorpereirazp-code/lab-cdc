"""Confere a estrutura da DAG do Airflow. Pulado se o Airflow não estiver instalado
(ele é pesado e só existe dentro do container)."""

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk")  # Airflow 3; ausente no CI e fora do container

PASTA_DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(PASTA_DAGS))
    assert bag.import_errors == {}, bag.import_errors
    return bag.get_dag("lab_cdc_ingestao")


def test_capturar_vem_antes_de_replicar(dag):
    assert dag.get_task("replicar").upstream_task_ids == {"capturar"}


def test_protecoes_da_dag(dag):
    assert dag.catchup is False      # não reexecuta ciclos passados ao ligar
    assert dag.max_active_runs == 1  # nunca duas leituras do slot ao mesmo tempo
    assert dag.get_task("capturar").retries == 2
