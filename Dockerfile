# Use uma imagem base Python
FROM python:3.13-slim

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de dependências e instale-as
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código da sua aplicação para o contêiner
COPY src/ ./src/
# COPY data/ ./data/ # Se os arquivos raw estiverem na pasta data/

# Comando para executar sua aplicação quando o contêiner iniciar
# Certifique-se de que main.py é o script que inicia o processo
CMD ["python", "src/main.py"]