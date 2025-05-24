import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

from db import get_spark_jdbc_properties
from setup_data import generate_raw_cdc_data, insert_users, DEFAULT_NUM_LINES, DEFAULT_NUM_USERS, DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME
from transformation.wallet_history import calculate_wallet_history
from src.transformation.calculate_cdi_bonus import calculate_cdi_bonus_for_period
from src.transformation.generate_daily_rates import insert_daily_rates_into_db

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
NUM_LINES = int(os.getenv("NUM_LINES", DEFAULT_NUM_LINES))
NUM_USERS = int(os.getenv("NUM_USERS", DEFAULT_NUM_USERS))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME))

def create_spark_session():
    """
    Cria e retorna uma sessão Spark configurada.
    """
    print("Criando sessão Spark...")
    spark = SparkSession.builder \
        .appName("CDIBonusCalculation") \
        .master(SPARK_MASTER_URL) \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    print("Sessão Spark criada com sucesso.")
    return spark

def step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS):
    if not os.path.exists(RAW_DATA_PATH):
        print(f"[MAIN] Arquivo de dados raw não encontrado ({RAW_DATA_PATH}). Iniciando geração...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, os.path.dirname(RAW_DATA_PATH), os.path.basename(RAW_DATA_PATH))
        print("[MAIN] Geração de dados raw concluída.")
    else:
        print(f"[MAIN] Arquivo de dados raw encontrado ({RAW_DATA_PATH}). Pulando geração.")

    raw_data_dir = os.path.dirname(RAW_DATA_PATH)
    raw_data_filename = os.path.basename(RAW_DATA_PATH)

    if not os.path.exists(RAW_DATA_PATH):
        print(f"[MAIN] Arquivo de dados raw não encontrado ({RAW_DATA_PATH}). Iniciando geração...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, raw_data_dir, raw_data_filename)
        print("[MAIN] Geração de dados raw concluída.")
    else:
        print(f"[MAIN] Arquivo de dados raw encontrado ({RAW_DATA_PATH}). Pulando geração.")


def step_insert_users(NUM_USERS):
    print("[MAIN] Iniciando inserção de usuários no banco de dados...")
    insert_users(NUM_USERS)
    print("[MAIN] Inserção de usuários concluída.")


def step_run_wallet_history_calculation(spark, RAW_DATA_PATH):
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

        DB_TABLE_WALLET_HISTORY = "wallet_history"
        jdbc_url, jdbc_properties = get_spark_jdbc_properties()

        # --- ADICIONE ESTAS LINHAS PARA DEPURAR O DATAFRAME ANTES DA ESCRITA ---
        print("[MAIN] Schema do df_wallet_history antes da escrita JDBC:")
        df_wallet_history.printSchema()
        print("[MAIN] Amostra do df_wallet_history antes da escrita JDBC:")
        df_wallet_history.show(5, truncate=False) # truncate=False para ver o timestamp completo
        # --- FIM DAS LINHAS DE DEPURACAO ---

        df_wallet_history.write.jdbc(
            url=jdbc_url,
            table=DB_TABLE_WALLET_HISTORY,
            mode="overwrite", 
            properties=jdbc_properties
        )

    except Exception as e:
        print(f"[MAIN] Erro ao ler o arquivo raw CDC: {e}")
        spark.stop()
        exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    
    step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS)
    step_insert_users(NUM_USERS)
    step_run_wallet_history_calculation(spark, RAW_DATA_PATH)
    insert_daily_rates_into_db()
    calculate_cdi_bonus_for_period(spark)
    spark.stop()
