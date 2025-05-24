# src/calculate_cdi_bonus.py (atualizado)

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, unix_timestamp, from_unixtime, window, max as spark_max, row_number, date_sub, to_timestamp, expr
from pyspark.sql.window import Window
from datetime import date, timedelta
from src.db import get_spark_jdbc_properties, get_db_connection, get_min_max_dates_from_wallet_history

def calculate_cdi_bonus_for_day(spark: SparkSession, calculation_date: date, jdbc_url: str, jdbc_properties: dict):
    """
    Calcula e registra o bônus CDI para um único dia.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        calculation_date (date): A data para a qual o bônus será calculado.
        jdbc_url (str): A URL de conexão JDBC para o banco de dados.
        jdbc_properties (dict): Dicionário de propriedades JDBC (usuário, senha, driver).
    """
    print(f"\n--- Processando bônus para a data: {calculation_date} ---")

    # --- 1. Ler Taxas de Juros da Tabela daily_interest_rates ---
    print(f"Lendo taxas de juros da tabela daily_interest_rates para {calculation_date}...")
    daily_interest_rates_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "daily_interest_rates") \
        .options(**jdbc_properties) \
        .load() \
        .filter(col("rate_date") == calculation_date)

    if daily_interest_rates_df.count() == 0:
        print(f"Erro: Nenhuma taxa de juros encontrada para a data {calculation_date}. Pulando este dia.")
        return # Retorna sem processar este dia

    current_daily_rate = daily_interest_rates_df.select("daily_rate").collect()[0][0]
    print(f"Taxa de juros para {calculation_date}: {current_daily_rate:.8f}")

    # --- 2. Ler a Tabela wallet_history e Derivar Saldo e Último Movimento ---
    print(f"Lendo dados da tabela wallet_history para derivar saldos e últimos movimentos...")

    # Definir o timestamp de início do dia de cálculo (00:00:00)
    start_of_calculation_day_ts = to_timestamp(lit(calculation_date.strftime("%Y-%m-%d") + " 00:00:00"), "yyyy-MM-dd HH:mm:ss")

    # Ler todos os dados da wallet_history que são relevantes para o cálculo
    # Precisamos de todos os movimentos até o início do dia de cálculo
    wallet_history_raw_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "wallet_history") \
        .options(**jdbc_properties) \
        .load() \
        .filter(col("timestamp") < start_of_calculation_day_ts) # Apenas movimentos ANTES do início do dia de cálculo

    # Se não houver dados no histórico antes do dia de cálculo, não há usuários elegíveis
    if wallet_history_raw_df.count() == 0:
        print(f"Nenhum histórico de carteira encontrado antes de {calculation_date}. Nenhum bônus será calculado para este dia.")
        return # Retorna sem processar este dia

    # Usar uma Window Function para encontrar o último saldo e timestamp de movimento para cada usuário
    # antes do início do dia de cálculo.
    window_spec = Window.partitionBy("user_id").orderBy(col("timestamp").desc())

    wallet_snapshot_df = wallet_history_raw_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select(
            col("user_id"),
            col("balance").alias("balance_at_start_of_day"), # Saldo no início do dia
            col("timestamp").alias("last_movement_timestamp") # Último movimento antes do início do dia
        )

    # --- 3. Unir Dados e Aplicar Regras de Elegibilidade ---

    # Adicionar a taxa de juros atual ao DataFrame de usuários
    users_with_rate_df = wallet_snapshot_df.withColumn("current_daily_rate", lit(current_daily_rate))

    # Regras de Elegibilidade:
    # 1. Saldo acima de $100
    # 2. Saldo não movido por pelo menos 24 horas antes do início do dia de cálculo
    #    Para a regra de 24 horas:
    #    - Comparamos o 'last_movement_timestamp' com o timestamp de início do dia de cálculo menos 24 horas.
    #    - Se 'last_movement_timestamp' for menor ou igual a (start_of_calculation_day_ts - 24h), então não foi movido nas últimas 24h.

    # Timestamp 24 horas antes do início do dia de cálculo
    threshold_timestamp = start_of_calculation_day_ts - expr("INTERVAL 1 DAY")

    eligible_users_df = users_with_rate_df.filter(
        (col("balance_at_start_of_day") > 100) &
        (unix_timestamp(col("last_movement_timestamp")) <= unix_timestamp(threshold_timestamp))
    )

    print(f"Usuários Elegíveis para o Bônus em {calculation_date}: {eligible_users_df.count()}")

    # --- 4. Calcular o Bônus CDI ---
    calculated_bonus_df = eligible_users_df.withColumn(
        "calculated_amount",
        col("balance_at_start_of_day") * col("current_daily_rate")
    ).withColumn(
        "payout_date",
        lit(calculation_date) # A data do payout é a data do cálculo
    ).select(
        col("payout_date"),
        col("user_id"),
        col("calculated_amount").cast("numeric(18, 2)") # Garante o tipo correto para o DB
    )

    print(f"Bônus CDI Calculado para Payout em {calculation_date}: {calculated_bonus_df.count()} registros.")

    # --- 5. Inserir na Tabela daily_bonus_payouts ---
    print(f"Escrevendo resultados na tabela daily_bonus_payouts para {calculation_date}...")
    try:
        # Deletar registros existentes para o dia atual antes de inserir
        with get_db_connection() as conn:
            cur = conn.cursor()
            delete_sql = "DELETE FROM daily_bonus_payouts WHERE payout_date = %s;"
            cur.execute(delete_sql, (calculation_date,))
            conn.commit()
            print(f"Registros existentes para {calculation_date} deletados da daily_bonus_payouts.")

        calculated_bonus_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "daily_bonus_payouts") \
            .options(**jdbc_properties) \
            .mode("append") \
            .save()
        print(f"Dados inseridos com sucesso na daily_bonus_payouts para {calculation_date}.")
    except Exception as e:
        print(f"Erro ao escrever dados na daily_bonus_payouts para {calculation_date}: {e}")
        # Em um backfill, você pode querer parar a execução aqui ou ter uma estratégia de retry mais robusta.


def calculate_cdi_bonus_for_period(spark: SparkSession, start_date_override: date = None, end_date_override: date = None):
    """
    Orquestra o cálculo do bônus CDI para um período, dia a dia.

    Args:
        spark (SparkSession): A sessão Spark ativa, já configurada.
        start_date_override (date, optional): Data de início do período de cálculo.
                                              Se None, será determinada automaticamente.
        end_date_override (date, optional): Data de fim do período de cálculo.
                                            Se None, será determinada automaticamente.
    """
    # --- Configurações do Banco de Dados (para Spark JDBC) ---
    jdbc_url, jdbc_properties = get_spark_jdbc_properties()

    try:
        # --- Determinar o Período de Cálculo Histórico ---
        min_overall_date = None
        max_overall_date = None

        if start_date_override and end_date_override:
            min_overall_date = start_date_override
            max_overall_date = end_date_override
            if min_overall_date > max_overall_date:
                raise ValueError("A data de início não pode ser posterior à data de fim.")
            print(f"Período de cálculo definido por argumentos: {min_overall_date} a {max_overall_date}")
        else:
            # Se nenhuma data for fornecida, determine o período a partir da wallet_history
            min_wallet_date, max_wallet_date = get_min_max_dates_from_wallet_history()

            if not min_wallet_date or not max_wallet_date:
                print("Não foi possível determinar o período a partir de wallet_history. Abortando cálculo.")
                return

            # O cálculo de bônus é sempre para o dia COMPLETO anterior.
            # Então, a data mínima para cálculo é o dia seguinte ao primeiro movimento.
            # A data máxima para cálculo é o dia anterior ao dia atual.
            min_overall_date = min_wallet_date + timedelta(days=1) # Bônus só pode ser calculado para o dia seguinte ao primeiro movimento
            max_overall_date = date.today() - timedelta(days=1) # Calcular até o dia anterior ao atual

            # Se a data máxima da wallet_history for anterior à data de hoje - 1, use-a como limite superior
            # Isso evita tentar calcular bônus para dias onde não há dados de wallet_history relevantes.
            if max_wallet_date < max_overall_date:
                max_overall_date = max_wallet_date

            print(f"Período de cálculo determinado automaticamente: {min_overall_date} a {max_overall_date}")

        # --- Loop para calcular o bônus para cada dia no período ---
        current_calculation_date = min_overall_date
        while current_calculation_date <= max_overall_date:
            calculate_cdi_bonus_for_day(spark, current_calculation_date, jdbc_url, jdbc_properties)
            current_calculation_date += timedelta(days=1)

        print("\nCálculo histórico do bônus CDI concluído para todo o período.")

    except Exception as e:
        print(f"Erro geral no cálculo do bônus CDI: {e}")
        raise # Re-lança a exceção para que o main.py possa lidar com ela

# O bloco __main__ é removido ou simplificado, pois a função será chamada de main.py
# if __name__ == "__main__":
#    # Este bloco agora é apenas para testes diretos do módulo, se necessário
#    # Em um ambiente de produção, a chamada virá de main.py
#    pass