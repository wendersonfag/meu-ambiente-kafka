# Databricks notebook source
# MAGIC %md
# MAGIC # Consumer Bronze - Kafka Simulator to Medallion Architecture
# MAGIC
# MAGIC Este notebook consome os arquivos JSON do `databricks-generator.py` e grava na camada Bronze.
# MAGIC
# MAGIC **Arquitetura:**
# MAGIC - **Entrada**: 3 tópicos (kafka-orders, kafka-events, kafka-gps) em `/FileStore/kafka-sim/`
# MAGIC - **Saída**: Tabela Delta `bronze.kafka_multiplex` (Hive Metastore)
# MAGIC - **Checkpoint**: `/tmp/kafka_multiplex_checkpoint`
# MAGIC - **Trigger**: 5 segundos
# MAGIC
# MAGIC **Estrutura da Bronze:**
# MAGIC ```
# MAGIC Coluna          | Tipo      | Descrição
# MAGIC --------------- | --------- | ---------------------------------
# MAGIC key             | LONG      | Ordem de entrada (sequence_id)
# MAGIC value           | STRING    | JSON completo do evento
# MAGIC topic           | STRING    | Nome do tópico (kafka-orders, etc)
# MAGIC partition       | INT       | Hash da key (0-2)
# MAGIC offset          | LONG      | Sequencial por partition
# MAGIC timestamp       | TIMESTAMP | Timestamp do dado original
# MAGIC ingestion_timestamp | TIMESTAMP | Quando foi inserido na Bronze
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, to_json,
    row_number, abs, hash, concat_ws, min as spark_min, max as spark_max
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql.window import Window
import json

print("✅ Bibliotecas carregadas com sucesso!")
print(f"🔥 Spark Version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurações da Bronze

# COMMAND ----------

# Diretórios DBFS dos tópicos simulados
BASE_PATH = "/FileStore/kafka-sim"
TOPIC_ORDER = f"{BASE_PATH}/kafka-orders"
TOPIC_EVENTOS = f"{BASE_PATH}/kafka-events"
TOPIC_GPS = f"{BASE_PATH}/kafka-gps"

# Configurações da camada Bronze (Hive Metastore - sem Unity Catalog)
BRONZE_DATABASE = "bronze"
BRONZE_TABLE = "kafka_multiplex"
BRONZE_TABLE_FULL = f"{BRONZE_DATABASE}.{BRONZE_TABLE}"

# Checkpoint location para rastreamento (Hive-compatible)
CHECKPOINT_PATH = "/tmp/kafka_multiplex_checkpoint"

# Configurações de streaming
TRIGGER_INTERVAL = "5 seconds"

print(f"📂 Tópicos configurados:")
print(f"   - kafka-orders:  {TOPIC_ORDER}")
print(f"   - kafka-events:  {TOPIC_EVENTOS}")
print(f"   - kafka-gps:     {TOPIC_GPS}")
print(f"\n📊 Bronze (Hive):")
print(f"   - Tabela: {BRONZE_TABLE_FULL}")
print(f"   - Checkpoint: {CHECKPOINT_PATH}")
print(f"   - Trigger: {TRIGGER_INTERVAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Database (Hive Metastore)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}")

print(f"✅ Database '{BRONZE_DATABASE}' criado/verificado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Definição dos Schemas

# COMMAND ----------

# Schema para o tópico 'kafka-orders'
order_data_schema = StructType([
    StructField("_sequence_id", LongType(), True),
    StructField("payment_id", StringType(), True),
    StructField("order_key", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_last4", StringType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("dt_current_timestamp", StringType(), True)
])

order_schema = StructType([
    StructField("data", order_data_schema, True)
])

# Schema para o tópico 'kafka-events'
event_details_schema = StructType([
    StructField("event_name", StringType(), True),
    StructField("timestamp", StringType(), True)
])

event_data_schema = StructType([
    StructField("_sequence_id", LongType(), True),
    StructField("event_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("event", event_details_schema, True),
    StructField("dt_current_timestamp", StringType(), True)
])

event_schema = StructType([
    StructField("data", event_data_schema, True)
])

# Schema para o tópico 'kafka-gps'
gps_data_schema = StructType([
    StructField("_sequence_id", LongType(), True),
    StructField("gps_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed_kph", IntegerType(), True),
    StructField("accuracy_m", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("dt_current_timestamp", StringType(), True)
])

gps_schema = StructType([
    StructField("data", gps_data_schema, True)
])

print("✅ Schemas definidos!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Função para Ler Stream com Auto Loader

# COMMAND ----------

def read_topic_stream(topic_path, schema, topic_name):
    """
    Lê arquivos JSON de um tópico usando Auto Loader.

    Args:
        topic_path: Caminho do diretório
        schema: Schema dos dados
        topic_name: Nome do tópico (para identificação)

    Returns:
        DataFrame stream
    """
    df_stream = spark \
        .readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema_{topic_name}") \
        .option("cloudFiles.inferColumnTypes", "false") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .option("recursiveFileLookup", "true") \
        .schema(schema) \
        .load(topic_path)

    # Adiciona coluna de nome do tópico
    df_with_topic = df_stream.withColumn("_topic_name", lit(topic_name))

    return df_with_topic

print("✅ Função de leitura definida!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Função para Transformar em Formato Bronze

# COMMAND ----------

def prepare_bronze_data(df_stream, topic_name):
    """
    Prepara os dados para a camada Bronze com metadados simulados do Kafka.

    Args:
        df_stream: DataFrame stream
        topic_name: Nome do tópico

    Returns:
        DataFrame com colunas: key, value, topic, partition, offset, timestamp, ingestion_timestamp
    """
    # Extrai sequence_id antes de fazer o to_json
    df_with_key = df_stream.select(
        col("data._sequence_id").alias("_key"),
        col("data.*").alias("_data"),
        to_json(col("data")).alias("_raw_json"),
        col("_topic_name")
    )

    # Calcula partição baseado no hash da key
    df_with_partition = df_with_key.select(
        col("_key").alias("key"),
        col("_raw_json").alias("value"),
        col("_topic_name").alias("topic"),
        (abs(hash(col("_key"))) % 3).alias("partition"),  # 3 partições (0, 1, 2)
    )

    # Calcula offset sequencial por partition
    window_spec = Window.partitionBy("partition").orderBy("key")
    df_with_offset = df_with_partition.withColumn(
        "offset",
        row_number().over(window_spec) - 1  # Começa do 0
    )

    # Adiciona timestamps
    df_bronze = df_with_offset.select(
        col("key").cast("LONG"),
        col("value").cast("STRING"),
        col("topic").cast("STRING"),
        col("partition").cast("INT"),
        col("offset").cast("LONG"),
        col("key").cast("TIMESTAMP").alias("timestamp"),  # Simulado: usa a key
        current_timestamp().alias("ingestion_timestamp")
    )

    return df_bronze

print("✅ Função de transformação definida!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificar Arquivos Disponíveis

# COMMAND ----------

print("🔍 Verificando arquivos disponíveis para consumo...\n")

for topic_name, topic_path in [
    ("kafka-orders", TOPIC_ORDER),
    ("kafka-events", TOPIC_EVENTOS),
    ("kafka-gps", TOPIC_GPS)
]:
    try:
        files = dbutils.fs.ls(topic_path)
        print(f"✅ {topic_name}: {len(files)} arquivos encontrados")
    except Exception as e:
        print(f"⚠️  {topic_name}: Nenhum arquivo encontrado")
        print(f"   Execute o notebook 'databricks-generator.py' primeiro!")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Limpar Checkpoint (Opcional)

# COMMAND ----------

# Descomente para limpar o checkpoint e reprocessar
# dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
# print("🗑️  Checkpoint limpo! Os arquivos serão reprocessados do início.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Ler Streams dos 3 Tópicos

# COMMAND ----------

print("📖 Lendo streams dos tópicos...\n")

df_orders = read_topic_stream(TOPIC_ORDER, order_schema, "kafka-orders")
df_events = read_topic_stream(TOPIC_EVENTOS, event_schema, "kafka-events")
df_gps = read_topic_stream(TOPIC_GPS, gps_schema, "kafka-gps")

print("✅ Streams dos tópicos carregados!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Preparar Dados para Bronze

# COMMAND ----------

print("🔄 Preparando dados para a camada Bronze...\n")

df_bronze_orders = prepare_bronze_data(df_orders, "kafka-orders")
df_bronze_events = prepare_bronze_data(df_events, "kafka-events")
df_bronze_gps = prepare_bronze_data(df_gps, "kafka-gps")

print("📋 Schema final da Bronze:")
df_bronze_orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Unificar Streams (Multiplexar)

# COMMAND ----------

# Combina os 3 streams em um único stream
df_bronze_multiplex = df_bronze_orders.unionByName(
    df_bronze_events
).unionByName(
    df_bronze_gps
)

print("✅ Streams multiplexados com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Gravar na Camada Bronze

# COMMAND ----------

print("="*70)
print("🚀 INICIANDO GRAVAÇÃO NA CAMADA BRONZE")
print("="*70)
print(f"\n📊 Tabela: {BRONZE_TABLE_FULL}")
print(f"📍 Checkpoint: {CHECKPOINT_PATH}")
print(f"⏱️  Trigger: {TRIGGER_INTERVAL}")
print(f"\nEsperado: Dados de 3 tópicos em um único stream")
print("\n" + "="*70)

# Grava o stream na tabela Bronze
query = df_bronze_multiplex \
    .writeStream \
    .format("delta") \
    .mode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("mergeSchema", "true") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .toTable(BRONZE_TABLE)

print(f"\n✅ Stream iniciado!")
print(f"   Query ID: {query.id}")
print(f"   Status: {query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Monitorar o Stream

# COMMAND ----------

import time

print("📊 Monitorando stream...\n")

# Aguarda um pouco para dados chegarem
time.sleep(15)

# Consulta a Bronze para verificar dados
try:
    df_check = spark.sql(f"SELECT COUNT(*) as total_rows FROM {BRONZE_TABLE_FULL}")
    total = df_check.collect()[0][0]
    print(f"✅ Total de registros na Bronze: {total}")

    # Mostra amostra dos dados
    spark.sql(f"SELECT * FROM {BRONZE_TABLE_FULL} LIMIT 5").display()

except Exception as e:
    print(f"⚠️  Aguardando primeiros dados... {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Queries Úteis para Análise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contar registros por tópico
# MAGIC SELECT topic, COUNT(*) as count, MIN(key) as min_key, MAX(key) as max_key
# MAGIC FROM bronze.kafka_multiplex
# MAGIC GROUP BY topic
# MAGIC ORDER BY topic;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por partição
# MAGIC SELECT topic, partition, COUNT(*) as count
# MAGIC FROM bronze.kafka_multiplex
# MAGIC GROUP BY topic, partition
# MAGIC ORDER BY topic, partition;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Offset máximo por topic
# MAGIC SELECT topic, partition, MAX(offset) as max_offset
# MAGIC FROM bronze.kafka_multiplex
# MAGIC GROUP BY topic, partition
# MAGIC ORDER BY topic, partition;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Últimos 10 registros
# MAGIC SELECT
# MAGIC   key,
# MAGIC   topic,
# MAGIC   partition,
# MAGIC   offset,
# MAGIC   ingestion_timestamp,
# MAGIC   substring(value, 1, 100) as value_preview
# MAGIC FROM bronze.kafka_multiplex
# MAGIC ORDER BY key DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Status dos Streams Ativos

# COMMAND ----------

print("🔄 Streams ativos:\n")

for stream in spark.streams.active:
    print(f"📊 {stream.name}:")
    print(f"   ID: {stream.id}")
    print(f"   Status: {stream.status}")
    print(f"   Progress: {stream.lastProgress}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Parar Streams (quando necessário)

# COMMAND ----------

# Descomente para parar todos os streams
# print("🛑 Parando todos os streams...")
# for stream in spark.streams.active:
#     stream.stop()
#     print(f"   ✅ Stream '{stream.id}' parado")
# print("\n✅ Todos os streams foram parados!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Resumo
# MAGIC
# MAGIC ✅ **Stream em tempo real**
# MAGIC - 3 tópicos (kafka-orders, kafka-events, kafka-gps) sendo ingeridos
# MAGIC - Metadados simulados do Kafka (key, partition, offset, timestamp)
# MAGIC - Salvos em Delta Lake na tabela `bronze.kafka_multiplex` (Hive Metastore)
# MAGIC
# MAGIC 📋 **Estrutura da Bronze**
# MAGIC - `key`: Ordem de entrada (sequence_id)
# MAGIC - `value`: JSON completo como STRING
# MAGIC - `topic`: Nome do tópico
# MAGIC - `partition`: Hash da key (0-2)
# MAGIC - `offset`: Sequencial por partition
# MAGIC - `timestamp`: Timestamp do dado
# MAGIC - `ingestion_timestamp`: Quando foi inserido
# MAGIC
# MAGIC 🔄 **Trigger**: 5 segundos
# MAGIC 📍 **Checkpoint**: `/tmp/kafka_multiplex_checkpoint`
# MAGIC
# MAGIC ---
