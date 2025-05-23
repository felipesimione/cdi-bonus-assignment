# src/main.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
# Importar as funções de setup E o context manager de DB
from setup_data import generate_raw_cdc_data, insert_users, DEFAULT_NUM_LINES, DEFAULT_NUM_USERS, DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME
from db import get_db_connection # Importa o context manager
from transformation.wallet_history import calculate_wallet_history

# --- Configurações (lidas de variáveis de ambiente) ---
# Não precisamos mais ler DATABASE_URL, USER, PASSWORD diretamente aqui,
# pois get_db_connection faz isso.
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
NUM_LINES = int(os.getenv("NUM_LINES", DEFAULT_NUM_LINES))
NUM_USERS = int(os.getenv("NUM_USERS", DEFAULT_NUM_USERS))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME))

# --- Lógica de Setup Inicial ---
# 1. Gerar dados raw se o arquivo não existir
raw_data_dir = os.path.dirname(RAW_DATA_PATH)
raw_data_filename = os.path.basename(RAW_DATA_PATH)

if not os.path.exists(RAW_DATA_PATH):
    print(f"[MAIN] Arquivo de dados raw não encontrado ({RAW_DATA_PATH}). Iniciando geração...")
    generate_raw_cdc_data(NUM_LINES, NUM_USERS, raw_data_dir, raw_data_filename)
    print("[MAIN] Geração de dados raw concluída.")
else:
    print(f"[MAIN] Arquivo de dados raw encontrado ({RAW_DATA_PATH}). Pulando geração.")

# 2. Inserir usuários no banco de dados
print("[MAIN] Iniciando inserção de usuários no banco de dados...")
# Chama a função insert_users (que agora usa o context manager internamente)
insert_users(NUM_USERS)
print("[MAIN] Inserção de usuários concluída.")

# --- Iniciar a Spark Session ---
print("[MAIN] Iniciando Spark Session...")
spark = SparkSession.builder \
    .appName("CDIBonusCalculation") \
    .master(SPARK_MASTER_URL) \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

print("[MAIN] Spark Session iniciada.")

# --- Lógica Principal da Aplicação PySpark ---

# 3. Ler o arquivo Raw CDC
print(f"[MAIN] Lendo arquivo raw CDC de: {RAW_DATA_PATH}")
raw_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount_change", FloatType(), True)
])

try:
    df_raw_cdc = spark.read.csv(
        RAW_DATA_PATH,
        header=True,
        schema=raw_schema
    )
    print("[MAIN] Arquivo raw CDC lido com sucesso.")

    df_raw_cdc.show(5)
    df_raw_cdc.printSchema()

    print("[MAIN] Chamando a função de cálculo de histórico da carteira...")
    df_wallet_history = calculate_wallet_history(df_raw_cdc)

    print("\n--- Exemplo da Tabela de Histórico da Carteira (df_wallet_history) ---")
    df_wallet_history.show(10, truncate=False) # Mostra 10 linhas, sem truncar o conteúdo
    df_wallet_history.printSchema() # Mostra o esquema do DataFrame resultante

except Exception as e:
    print(f"[MAIN] Erro ao ler o arquivo raw CDC: {e}")
    spark.stop()
    exit(1)

# --- Continue aqui com a lógica para interagir com o banco de dados ---

# Exemplo: Como ler dados de uma tabela (ex: daily_interest_rates)
# Você usaria o context manager AQUI para obter a conexão
# Note que operações Spark (como spark.read.jdbc) gerenciam suas próprias conexões
# O context manager é mais útil para operações Python diretas no DB (como a inserção de usuários)
# ou se você precisar de uma conexão para metadados ou operações que não são de leitura/escrita de DataFrame.

# Exemplo de leitura de dados com Spark JDBC (não usa o context manager diretamente,
# mas o driver JDBC do Spark gerencia a conexão usando os parâmetros fornecidos)
# db_properties = {
#     "user": os.getenv("DATABASE_USER"),
#     "password": os.getenv("DATABASE_PASSWORD"),
#     "driver": "org.postgresql.Driver"
# }
# df_daily_rates = spark.read.jdbc(url=os.getenv("DATABASE_URL"), table="daily_interest_rates", properties=db_properties)
# df_daily_rates.show()


# Exemplo: Como escrever dados em uma tabela (ex: daily_bonus_payouts)
# df_bonus_payouts.write.jdbc(url=os.getenv("DATABASE_URL"), table="daily_bonus_payouts", mode="append", properties=db_properties)


# Se você precisar executar uma query SQL simples ou uma operação que não seja de DataFrame:
# try:
#     with get_db_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute("SELECT COUNT(*) FROM users;")
#             user_count = cur.fetchone()[0]
#             print(f"[MAIN] Total de usuários no DB: {user_count}")
# except Exception as e:
#     print(f"[MAIN] Erro ao contar usuários: {e}")


# --- Finalizar a Spark Session ---
print("[MAIN] Processamento concluído. Parando Spark Session.")
spark.stop()