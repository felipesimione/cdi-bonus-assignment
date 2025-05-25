# Stage 1: Base Spark image with Python dependencies
# Usamos uma imagem Bitnami Spark que já vem com Spark, Java e Python
FROM bitnami/spark:3.5.0 AS spark_base

# Instalar dependências Python e cliente PostgreSQL
# Precisamos de root temporariamente para instalar pacotes do sistema (cliente postgresql)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Voltar para o usuário padrão da imagem Bitnami (geralmente 1001)
# É uma boa prática não rodar como root
USER 1001

# Stage 2: Build da Aplicação
FROM spark_base AS app_builder

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de dependências e instale-as
# Certifique-se que requirements.txt inclui pyspark e psycopg2-binary
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY postgresql-42.6.0.jar /opt/bitnami/spark/jars/

# Copie o código da sua aplicação para o contêiner
# Isso copiará src/main.py, src/setup_data.py, src/db.py, etc.
COPY src/ ./src/
COPY tests/ ./tests/

# NÃO COPIE A PASTA data/ AQUI!
# Ela será montada como um volume em tempo de execução via docker-compose.yml

# Comando para executar sua aplicação quando o contêiner iniciar
# Usamos spark-submit para rodar a aplicação PySpark
# O caminho /app/src/main.py é onde o script foi copiado
CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/src/main.py"]

# Expor portas se necessário (ex: para UI da aplicação Spark)
# EXPOSE 4040