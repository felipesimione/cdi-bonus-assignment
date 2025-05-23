# src/db.py

import os
import psycopg2
import time
from contextlib import contextmanager
from urllib.parse import urlparse # Para parsear a URL do banco de dados

# --- Configurações de Retry ---
MAX_DB_RETRIES = 20 # Número máximo de tentativas de conexão
DB_RETRY_DELAY = 3 # Segundos para esperar entre as tentativas

# --- Context Manager para Conexão com o Banco de Dados ---
@contextmanager
def get_db_connection():
    """
    Context manager para obter uma conexão com o banco de dados PostgreSQL.
    Lê as credenciais de variáveis de ambiente e implementa lógica de retry.
    Garante que a conexão seja fechada ao sair do bloco 'with'.
    """
    db_url = os.getenv("DATABASE_URL")
    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")

    if not db_url or not db_user or not db_password:
        raise ValueError("Variáveis de ambiente DATABASE_URL, DATABASE_USER ou DATABASE_PASSWORD não definidas.")

    conn = None
    db_params = {}

    # Parse a URL do banco de dados
    # Ex: postgresql://user:password@host:port/dbname
    try:
        parsed_url = urlparse(db_url)
        db_params = {
            'database': parsed_url.path[1:], # Remove a barra inicial
            'user': db_user, # Usar user/password das env vars, não da URL (mais seguro)
            'password': db_password,
            'host': parsed_url.hostname,
            'port': parsed_url.port if parsed_url.port else 5432 # Default para 5432
        }
    except Exception as e:
        raise ValueError(f"Erro ao parsear DATABASE_URL '{db_url}': {e}")


    print(f"[DB] Tentando conectar ao banco de dados em {db_params.get('host')}:{db_params.get('port')}/{db_params.get('database')}...")

    # Lógica de retry para esperar o banco de dados ficar pronto
    for i in range(MAX_DB_RETRIES):
        try:
            conn = psycopg2.connect(**db_params)
            print("[DB] Conexão com o banco de dados estabelecida.")
            break # Sai do loop de retry se conectar com sucesso
        except psycopg2.OperationalError as e:
            print(f"[DB] Erro de conexão com o banco de dados (Tentativa {i+1}/{MAX_DB_RETRIES}): {e}")
            if i < MAX_DB_RETRIES - 1:
                print(f"[DB] Aguardando {DB_RETRY_DELAY} segundos antes de tentar novamente...")
                time.sleep(DB_RETRY_DELAY)
            else:
                print("[DB] Número máximo de tentativas de conexão excedido.")
                raise # Levanta a exceção se falhar após todas as tentativas
        except Exception as e:
             print(f"[DB] Erro inesperado ao conectar ao banco de dados (Tentativa {i+1}/{MAX_DB_RETRIES}): {e}")
             if i < MAX_DB_RETRIES - 1:
                print(f"[DB] Aguardando {DB_RETRY_DELAY} segundos antes de tentar novamente...")
                time.sleep(DB_RETRY_DELAY)
             else:
                print("[DB] Número máximo de tentativas de conexão excedido.")
                raise


    if conn is None:
         # Isso só deve acontecer se o loop de retry terminar sem sucesso
         raise ConnectionError("Não foi possível estabelecer conexão com o banco de dados após várias tentativas.")

    try:
        # O código dentro do bloco 'with' será executado aqui
        yield conn
    finally:
        # Este bloco é sempre executado ao sair do bloco 'with'
        if conn:
            conn.close()
            print("[DB] Conexão com o banco de dados fechada.")

