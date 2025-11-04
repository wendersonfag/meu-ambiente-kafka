# Como Testar Conexão Bore no Databricks

## URL Atual do Bore
Baseado no seu `docker-compose.yml`, a URL atual é: **bore.pub:53049**

**IMPORTANTE:** Esta URL muda toda vez que você reinicia o script `start-kafka-env.ps1`

## Teste de Conectividade no Databricks

### 1. Teste com netcat (nc)

No notebook do Databricks, execute:

```python
%sh
nc -zv bore.pub 53049
```

**Saída esperada:**
```
Connection to bore.pub 53049 port [tcp/*] succeeded!
```

### 2. Teste com telnet

```python
%sh
timeout 5 telnet bore.pub 53049
```

**Saída esperada:**
```
Connected to bore.pub.
Escape character is '^]'.
```

### 3. Teste com curl

```python
%sh
curl -v telnet://bore.pub:53049
```

### 4. Teste com Python (socket)

```python
import socket

def test_kafka_connection(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()

        if result == 0:
            print(f"✓ Conexão bem-sucedida com {host}:{port}")
            return True
        else:
            print(f"✗ Falha ao conectar em {host}:{port}")
            return False
    except Exception as e:
        print(f"✗ Erro: {e}")
        return False

# Teste a conexão
test_kafka_connection("bore.pub", 53049)
```

## Teste Completo de Leitura do Kafka

### 1. Verificar tópicos disponíveis

```python
from pyspark.sql import SparkSession

# Configuração
kafka_bootstrap_servers = "bore.pub:53049"

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("Kafka Test") \
    .getOrCreate()

# Ler tópicos disponíveis
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "gps") \
  .option("startingOffsets", "earliest") \
  .load()

# Mostrar schema
df.printSchema()

# Mostrar primeiros dados
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show(10, False)
```

### 2. Streaming do Kafka

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# Configuração
kafka_bootstrap_servers = "bore.pub:53049"
topic = "gps"

# Criar sessão Spark
spark = SparkSession.builder \
    .appName("Kafka Streaming Test") \
    .getOrCreate()

# Schema dos dados GPS
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

# Ler stream do Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()

# Parse JSON
parsed_df = df \
  .selectExpr("CAST(value AS STRING) as json") \
  .select(from_json(col("json"), schema).alias("data")) \
  .select("data.*")

# Mostrar dados
query = parsed_df \
  .writeStream \
  .format("console") \
  .outputMode("append") \
  .start()

# Aguardar por 60 segundos
query.awaitTermination(60)
query.stop()
```

## Como Obter a URL Atual do Bore

### Opção 1: Ler do arquivo bore_url.txt

No seu computador local (Windows), após executar `start-kafka-env.ps1`:

```powershell
Get-Content bore_url.txt
```

### Opção 2: Ler do docker-compose.yml

```powershell
Select-String -Path docker-compose.yml -Pattern "EXTERNAL://"
```

### Opção 3: Ver no output do script

Quando você executa `start-kafka-env.ps1`, a URL é exibida assim:

```
[3/6] Iniciando túnel Bore...
  ✓ Bore iniciado com sucesso!
  URL Externa: bore.pub:53049

  Configuração para Databricks:
  kafka.bootstrap.servers = "bore.pub:53049"
```

## Troubleshooting

### Erro: "Connection refused"

```bash
nc: connect to bore.pub port 53049 (tcp) failed: Connection refused
```

**Possíveis causas:**
1. Script `start-kafka-env.ps1` não está rodando
2. URL do Bore mudou (reiniciou o script)
3. Bore tunnel caiu

**Solução:**
- Reinicie o script: `.\start-kafka-env.ps1`
- Copie a nova URL exibida
- Atualize o teste no Databricks

### Erro: "Connection timeout"

```bash
nc: connect to bore.pub port 53049 (tcp) failed: Connection timed out
```

**Possíveis causas:**
1. Firewall bloqueando
2. Rede corporativa bloqueando túneis
3. Databricks não consegue acessar internet externa

**Solução:**
- Verifique regras de firewall
- Teste de outra rede
- Contate administrador de rede

### Erro: "Consumer group rebalancing"

Se o Kafka conectar mas ficar em loop de rebalancing:

```python
# Adicione configurações de timeout
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .option("kafka.session.timeout.ms", "30000") \
  .option("kafka.request.timeout.ms", "40000") \
  .option("kafka.heartbeat.interval.ms", "3000") \
  .load()
```

## Exemplo Completo - Notebook Databricks

```python
# Célula 1: Testar conectividade
%sh
nc -zv bore.pub 53049

# Célula 2: Configurar Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

kafka_servers = "bore.pub:53049"
topic = "gps"

spark = SparkSession.builder \
    .appName("Kafka Bore Test") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka-checkpoint") \
    .getOrCreate()

print(f"✓ Spark configurado para conectar em: {kafka_servers}")

# Célula 3: Ler batch de dados
df_batch = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()

print(f"Total de mensagens: {df_batch.count()}")
df_batch.selectExpr("CAST(value AS STRING)").show(5, False)

# Célula 4: Streaming
df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_servers) \
  .option("subscribe", topic) \
  .load()

query = df_stream \
  .selectExpr("CAST(value AS STRING) as message") \
  .writeStream \
  .format("console") \
  .outputMode("append") \
  .start()

# Executar por 30 segundos
import time
time.sleep(30)
query.stop()
```

## Dicas Importantes

1. **URL muda sempre**: Toda vez que você reinicia o `start-kafka-env.ps1`, uma nova porta é gerada
2. **Mantenha o script rodando**: Se você fechar o terminal, o túnel Bore para
3. **Copie a URL certa**: Sempre use a URL exibida no final do script
4. **Teste primeiro com nc**: Antes de tentar ler do Kafka, teste a conectividade básica
5. **Use o arquivo bore_url.txt**: Este arquivo sempre tem a URL mais recente
