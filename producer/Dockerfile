FROM python:3.9-slim

WORKDIR /app

# Copia os arquivos necessários
COPY requirements.txt .
COPY producer.py .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando para executar o producer
CMD ["python", "-u", "producer.py"]