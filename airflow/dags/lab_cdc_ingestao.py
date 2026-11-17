"""DAG do laboratório: Postgres -> S3 -> Volume do Databricks, a cada 15 minutos.

Faz automaticamente o que antes eram dois comandos manuais:
    uv run python -m lab_cdc --s3 <bucket>       -> tarefa capturar
    uv run python -m lab_cdc.replicar ...         -> tarefa replicar
Quando o arquivo chega ao Volume, o job do Databricks dispara sozinho (bronze -> silver).
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="lab_cdc_ingestao",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 11, 1),
    # catchup=False: ao ligar o Airflow, NÃO executa todos os ciclos de 15 min desde 1º/11.
    # Para CDC isso é certo: o slot já guarda tudo que aconteceu, um ciclo pega o atraso inteiro.
    catchup=False,
    # Uma execução por vez: duas lendo o mesmo slot poderiam gravar o mesmo evento duas vezes.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["lab-cdc"],
)
def lab_cdc_ingestao():
    @task
    def capturar() -> int:
        """Lê o slot, grava no S3 e só então confirma o slot. Devolve quantos eventos leu."""
        import os

        import boto3
        import psycopg

        from lab_cdc.capture import confirmar, ler_slot
        from lab_cdc.landing import gravar_s3

        slot = os.environ["LAB_CDC_SLOT"]
        with psycopg.connect(autocommit=True) as conn:
            eventos, ate_lsn = ler_slot(conn, slot)
            destino = gravar_s3(eventos, boto3.client("s3"), os.environ["LAB_CDC_BUCKET"])
            if ate_lsn:
                confirmar(conn, slot, ate_lsn)
        print(f"{len(eventos)} evento(s) -> {destino or 'nada a gravar'}")
        return len(eventos)

    @task
    def replicar(eventos_capturados: int) -> int:
        """Copia para o Volume os arquivos do S3 que ainda não estão lá.

        Roda mesmo quando a captura não trouxe nada: um arquivo de um ciclo anterior
        pode ter ficado para trás se a réplica daquele ciclo falhou.
        """
        import os

        import boto3
        from databricks.sdk import WorkspaceClient

        from lab_cdc.replicar import replicar as copiar_novos

        # sem perfil: o SDK lê DATABRICKS_HOST e DATABRICKS_TOKEN do ambiente
        copiados = copiar_novos(boto3.client("s3"), WorkspaceClient(), os.environ["LAB_CDC_BUCKET"])
        print(f"captura trouxe {eventos_capturados}; {len(copiados)} arquivo(s) copiado(s)")
        return len(copiados)

    replicar(capturar())


lab_cdc_ingestao()
