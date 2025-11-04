# Código Correto para Databricks - Kafka com Bore

## Passo 1: Testar Conectividade

```python
%sh
nc -zv bore.pub 53049
```

**Saída esperada:**
```
Connection to bore.pub 53049 port [tcp/*] succeeded!
```

## Passo 2: Configuração Inicial

```python
# Configurações
kafka_bootstrap_servers = "bore.pub:53049"
topic = "gps"

print(f"Kafka Server: {kafka_bootstrap_servers}")
print(f"Tópico: {topic}")
```

## Passo 3: Leitura em Batch (Teste Simples)

```python
# Ler dados em modo batch
df_batch = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .option("endingOffsets", "latest") \
  .load()

# Mostrar schema
df_batch.printSchema()

# Contar mensagens
total = df_batch.count()
print(f"Total de mensagens no tópico '{topic}': {total}")

# Mostrar primeiras mensagens (convertendo value de binário para string)
df_batch.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
).show(10, truncate=False)
```

## Passo 4: Leitura com Parsing de JSON

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Schema dos dados GPS (ajuste conforme seu producer)
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

# Ler e parsear JSON
df_parsed = df_batch \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Mostrar dados parseados
display(df_parsed)
```

## Passo 5: Streaming (Leitura Contínua)

```python
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# Schema dos dados
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

# Ler stream do Kafka
df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "latest") \
  .load()

# Parse JSON
df_parsed_stream = df_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Mostrar em tempo real
display(df_parsed_stream)
```

**IMPORTANTE:** Para parar o streaming, clique em "Stop Execution" no Databricks ou cancele a célula.

## Passo 6: Streaming com Agregação

```python
from pyspark.sql.functions import col, from_json, count, avg, window

# Schema
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

# Ler stream
df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "latest") \
  .load()

# Parse e agregar
df_aggregated = df_stream \
    .selectExpr("CAST(value AS STRING) as json_string", "timestamp as kafka_timestamp") \
    .select(from_json(col("json_string"), schema).alias("data"), "kafka_timestamp") \
    .select("data.*", "kafka_timestamp") \
    .groupBy("device_id") \
    .agg(
        count("*").alias("total_messages"),
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude")
    )

# Display
display(df_aggregated)
```

## Passo 7: Salvar Streaming em Delta Table

```python
from pyspark.sql.functions import col, from_json, current_timestamp

# Schema
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

# Ler stream
df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "latest") \
  .load()

# Parse JSON
df_parsed = df_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", current_timestamp())

# Salvar em Delta
checkpoint_path = "/tmp/kafka_checkpoint_gps"
output_path = "/tmp/gps_data"

query = df_parsed \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(output_path)

print(f"Streaming iniciado. ID da query: {query.id}")
print(f"Status: {query.status}")

# Para parar depois:
# query.stop()
```

## Passo 8: Ler da Delta Table

```python
# Ler os dados salvos
df_delta = spark.read.format("delta").load("/tmp/gps_data")

# Mostrar dados
display(df_delta.orderBy(col("processed_at").desc()))

# Estatísticas
print(f"Total de registros: {df_delta.count()}")
df_delta.groupBy("device_id").count().show()
```

## Erros Comuns e Soluções

### Erro 1: `TypeError: int() argument must be a string...`

**Causa:** Tentando usar `.display()` em query de streaming antes de iniciar

**Solução:** Use `.display()` SOMENTE após `.load()` ou após o dataframe estar completo

```python
# ❌ ERRADO
query = df.writeStream.format("console").start()
display(query)  # Erro!

# ✓ CORRETO
df_stream = spark.readStream.format("kafka")...load()
display(df_stream)  # OK!
```

### Erro 2: `Connection refused`

**Causa:** Bore não está rodando ou URL mudou

**Solução:**
1. No Windows, execute `.\start-kafka-env.ps1`
2. Copie a nova URL exibida
3. Atualize a variável `kafka_bootstrap_servers`

### Erro 3: `Topic not found`

**Causa:** Tópico não existe no Kafka

**Solução:**
```python
# Listar tópicos disponíveis
df_topics = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "gps") \
  .load()

df_topics.select("topic").distinct().show()
```

### Erro 4: `Stream timeout`

**Causa:** Kafka não está enviando dados

**Solução:** Verifique se o producer está rodando:
```bash
# No Windows
docker logs python-producer
```

## Template Completo - Notebook Databricks

```python
# ============================================
# CÉLULA 1: Teste de Conectividade
# ============================================
%sh
nc -zv bore.pub 53049


# ============================================
# CÉLULA 2: Configuração
# ============================================
from pyspark.sql.functions import col, from_json, count, avg, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

# Configurações
kafka_bootstrap_servers = "bore.pub:53049"  # ATUALIZE COM SUA URL
topic = "gps"

# Schema dos dados
schema = StructType() \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("device_id", StringType())

print(f"✓ Configurado: {kafka_bootstrap_servers}")


# ============================================
# CÉLULA 3: Teste Batch (Ver Dados Existentes)
# ============================================
df_batch = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()

total = df_batch.count()
print(f"Total de mensagens: {total}")

df_batch.selectExpr("CAST(value AS STRING) as value").show(5, truncate=False)


# ============================================
# CÉLULA 4: Streaming em Tempo Real
# ============================================
df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "latest") \
  .load()

df_parsed = df_stream \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Mostrar dados em tempo real
display(df_parsed)


# ============================================
# CÉLULA 5: Salvar em Delta (Opcional)
# ============================================
checkpoint_path = "/tmp/kafka_checkpoint_gps"
output_path = "/tmp/gps_data_delta"

query = df_parsed \
    .withColumn("processed_at", current_timestamp()) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(output_path)

print(f"Query ID: {query.id}")


# ============================================
# CÉLULA 6: Ler Delta Table (Opcional)
# ============================================
df_saved = spark.read.format("delta").load("/tmp/gps_data_delta")
display(df_saved.orderBy(col("processed_at").desc()))


# ============================================
# CÉLULA 7: Parar Streaming (Quando Necessário)
# ============================================
# Lista todas as queries ativas
for q in spark.streams.active:
    print(f"Query ID: {q.id}, Status: {q.status}")
    q.stop()
    print(f"Query {q.id} parada")
```

## Dicas Importantes

1. **Sempre teste conectividade primeiro** com `nc -zv`
2. **Use `.show()` para batch** e `.display()` para streaming
3. **Atualize a URL** toda vez que reiniciar o script Bore
4. **Pare os streams** antes de fechar o notebook
5. **Verifique os logs** se houver erro: `docker logs kafka`

## Próximos Passos

1. Execute as células na ordem
2. Ajuste o schema conforme seus dados
3. Teste primeiro em batch, depois streaming
4. Salve em Delta para análises posteriores
