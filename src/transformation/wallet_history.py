# src/transformation/wallet_history.py

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import sum, col, to_timestamp, lit, coalesce, monotonically_increasing_id
from datetime import datetime
import logging

# Configurar o logger para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def calculate_wallet_history(df_raw_cdc: DataFrame) -> DataFrame:
    """
    Calculates the wallet balance history table from raw CDC data.

    Args:
        df_raw_cdc (DataFrame): Spark DataFrame containing the raw CDC data
                                (user_id, timestamp, amount_change).
                                The 'timestamp' column is expected as StringType
                                in the format "yyyy-MM-dd'T'HH:mm:ss.SSSSSS".

    Returns:
        DataFrame: Spark DataFrame with the wallet balance history,
                   including the 'balance' column.
    """
    logger.info("Iniciando a construção da tabela de histórico da carteira...")

    # Definir o formato do timestamp esperado (o mesmo que você usa no seu CSV)
    TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

    # 1. Garantir que a coluna 'timestamp' seja do tipo TimestampType e tratar NULLs
    #    Se 'timestamp' for NULL, vamos preencher com o timestamp atual para que a ordenação funcione.
    #    Em um cenário real, você validaria a origem dos dados ou teria uma lógica mais robusta para NULLs.
    df_raw_cdc_processed = df_raw_cdc.withColumn(
        "timestamp",
        coalesce(to_timestamp(col("timestamp"), TIMESTAMP_FORMAT), lit(datetime.now())) # Converte para timestamp e preenche NULLs
    )
    logger.info("Coluna 'timestamp' processada e NULLs tratados.")

    df_raw_cdc_with_id = df_raw_cdc_processed.withColumn("row_id", monotonically_increasing_id())
    logger.info("ID de linha único adicionado para desempate.")

    # 2. Definir a especificação da janela para o cálculo cumulativo
    #    Particionamos por user_id e ordenamos por timestamp para garantir que o saldo seja calculado
    #    corretamente para cada usuário em ordem cronológica.
    window_spec = Window.partitionBy("user_id").orderBy("timestamp", "row_id")
    logger.info("Especificação da janela definida (partitionBy user_id, orderBy timestamp).")

    # 3. Calcular o saldo cumulativo
    #    A função sum(col("amount_change")).over(window_spec) calcula a soma acumulada
    #    de 'amount_change' dentro de cada partição (user_id), ordenada por 'timestamp'.
    df_wallet_history = df_raw_cdc_with_id.withColumn( # <--- USAR O DATAFRAME COM O NOVO ID
        "balance",
        sum(col("amount_change")).over(window_spec)
    ).drop("row_id") # Opcional: remover o row_id se não for mais necessário no resultado final

    logger.info("Saldo cumulativo calculado com sucesso.")

    logger.info("Tabela de histórico da carteira construída.")
    return df_wallet_history
