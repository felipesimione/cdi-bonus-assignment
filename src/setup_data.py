# src/setup_data.py

import csv
import random
import datetime
import os
import psycopg2
import time
from db import get_db_connection

# --- Configurações para Geração de Dados ---
# Estas podem ser lidas de variáveis de ambiente no script principal
DEFAULT_NUM_LINES = 100000
DEFAULT_NUM_USERS = 1000
# Caminho de saída relativo ao diretório de execução (será /app dentro do contêiner)
DEFAULT_OUTPUT_DIR = "data/raw_cdc_files"
DEFAULT_OUTPUT_FILENAME = "wallet_cdc_raw.csv"
MIN_AMOUNT_CHANGE = -1000.0
MAX_AMOUNT_CHANGE = 5000.0
TIME_RANGE_DAYS = 365

# --- Função para gerar um timestamp aleatório dentro de um intervalo ---
def random_timestamp(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)

    random_time = datetime.timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999)
    )
    return random_date + random_time

# --- Função para gerar os dados raw CDC ---
def generate_raw_cdc_data(num_lines, num_users, output_dir, output_filename):
    print(f"Generating {num_lines} raw CDC data for {num_users} users...")

    os.makedirs(output_dir, exist_ok=True)
    output_filepath = os.path.join(output_dir, output_filename)

    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=TIME_RANGE_DAYS)

    all_records = []

    lines_per_user = num_lines // num_users
    remaining_lines = num_lines % num_users

    user_lines_counts = [lines_per_user] * num_users
    for i in range(remaining_lines):
        user_lines_counts[i] += 1

    user_ids = list(range(1, num_users + 1))
    random.shuffle(user_ids) 

    for i, user_id in enumerate(user_ids):
        num_events_for_user = user_lines_counts[i]
        user_records = []

        current_timestamp = random_timestamp(start_date, start_date + datetime.timedelta(days=TIME_RANGE_DAYS // 10)) 

        for _ in range(num_events_for_user):
            
            time_delta = datetime.timedelta(
                minutes=random.randint(1, 60),
                hours=random.randint(0, 23)
            )
            current_timestamp += time_delta

            if current_timestamp > end_date:
                current_timestamp = end_date - datetime.timedelta(seconds=random.randint(1, 60)) 

            amount_change = random.uniform(MIN_AMOUNT_CHANGE, MAX_AMOUNT_CHANGE)

            timestamp_str = current_timestamp.isoformat()

            user_records.append({
                'user_id': user_id,
                'timestamp': timestamp_str,
                'amount_change': round(amount_change, 2)
            })

        all_records.extend(user_records)

    print("Sorting all records by timestamp...")
    all_records.sort(key=lambda x: x['timestamp'])

    print(f"Writing data to the file {output_filepath}...")
    with open(output_filepath, 'w', newline='') as csvfile:
        fieldnames = ['user_id', 'timestamp', 'amount_change']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(all_records)

    print(f"File generation completed. Saved in: {output_filepath}")
    print(f"Total number of lines generated: {len(all_records)}")

# --- Função para inserir usuários ---
# Não precisa mais receber db_url, user, password, pois get_db_connection lê das env vars
def insert_users(num_users):
    """
    Insere usuários na tabela 'users' usando o context manager de conexão.
    """
    print(f"[SETUP] Iniciando inserção de {num_users} usuários na tabela 'users'...")

    try:
        # Usa o context manager para obter e gerenciar a conexão
        with get_db_connection() as conn:
            # Usa um context manager para o cursor também (boa prática)
            with conn.cursor() as cur:
                insert_sql = "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING;"
                user_ids_to_insert = [(i,) for i in range(1, num_users + 1)]

                cur.executemany(insert_sql, user_ids_to_insert)

            # Commit a transação após a inserção (dentro do bloco 'with conn:')
            conn.commit()
            print(f"[SETUP] Usuários inseridos ou já existentes.")

    except Exception as e:
        # O context manager get_db_connection já fecha a conexão em caso de erro
        # Mas podemos adicionar log adicional aqui se necessário
        print(f"[SETUP] Erro durante a inserção de usuários: {e}")
        # A exceção já foi levantada pelo context manager ou pelo código acima
        raise # Re-levanta a exceção para que o chamador (main.py) saiba que falhou

    print("[SETUP] Inserção de usuários concluída.")

