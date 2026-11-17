# Airflow do laboratório: a imagem oficial mais as bibliotecas que o lab_cdc usa.
# O código do lab_cdc NÃO entra na imagem: ele é montado como volume (ver docker-compose),
# então uma mudança em src/ vale na próxima execução da DAG, sem reconstruir nada.
FROM apache/airflow:3.3.2-python3.12

# Cria a pasta do banco interno do Airflow com dono "airflow", o usuário da imagem.
# Um volume novo herda o dono de uma pasta que já existe na imagem. Sem esta linha,
# o Docker cria a pasta como root e o Airflow falha com "unable to open database file".
RUN mkdir -p /opt/airflow/estado

# As constraints oficiais travam as versões compatíveis com este Airflow,
# para uma biblioteca nossa não quebrar uma dependência dele.
RUN pip install --no-cache-dir "psycopg[binary]" boto3 databricks-sdk \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
