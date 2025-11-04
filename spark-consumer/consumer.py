#!/usr/bin/env python3
"""
Spark Streaming Consumer - Consome dados de 3 tópicos Kafka
Tópicos: ordem, eventos, gps
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configurações
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_ORDER = 'ordem'
TOPIC_EVENTOS = 'eventos'
TOPIC_GPS = 'gps'

def wait_for_kafka(broker, max_retries=30):
    """Aguarda o Kafka estar pronto"""
    print(f"Aguardando Kafka em {broker}...")
    for i in range(max_retries):
        try:
            # Tenta criar uma sessão simples para testar conexão
            from socket import socket, AF_INET, SOCK_STREAM
            host, port = broker.split(':')
            sock = socket(AF_INET, SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            if result == 0:
                print("Kafka está pronto!")
                time.sleep(5)  # Aguarda mais um pouco para garantir
                return True
        except Exception as e:
            print(f"Tentativa {i+1}/{max_retries}: Kafka não está pronto ({e})")
        time.sleep(2)
    raise Exception("Kafka não ficou disponível a tempo")

def create_spark_session():
    """Cria a SparkSession com configurações para Kafka"""
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- Schemas dos Dados ---

# Schema para o tópico 'ordem'
order_data_schema = StructType([
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

# Schema para o tópico 'eventos'
event_details_schema = StructType([
    StructField("event_name", StringType(), True),
    StructField("timestamp", StringType(), True)
])

event_data_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("event", event_details_schema, True),
    StructField("dt_current_timestamp", StringType(), True)
])

event_schema = StructType([
    StructField("data", event_data_schema, True)
])

# Schema para o tópico 'gps'
gps_data_schema = StructType([
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

def consume_topic(spark, topic_name, schema, query_name):
    """
    Consome dados de um tópico Kafka específico
    
    Args:
        spark: SparkSession
        topic_name: Nome do tópico Kafka
        schema: Schema dos dados JSON
        query_name: Nome da query para identificação
    """
    print(f"\n{'='*60}")
    print(f"Iniciando consumo do tópico: {topic_name}")
    print(f"{'='*60}\n")
    
    # Lê o stream do Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Converte o valor de bytes para string e depois para JSON
    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
        .select(
            col("key"),
            from_json(col("value"), schema).alias("json_data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("key", "json_data.data.*", "kafka_timestamp")
    
    # Mostra o schema
    print(f"\n📋 Schema do tópico '{topic_name}':")
    parsed_df.printSchema()
    
    # Escreve no console
    query = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName(query_name) \
        .trigger(processingTime="5 seconds") \
        .start()
    
    return query

def main():
    """Função principal"""
    print("="*60)
    print("🚀 Iniciando Spark Streaming Consumer")
    print("="*60)
    
    # Aguarda o Kafka
    wait_for_kafka(KAFKA_BROKER)
    
    # Cria a Spark Session
    spark = create_spark_session()
    print(f"✅ SparkSession criada: {spark.version}")
    
    try:
        # Inicia o consumo dos três tópicos
        query_ordem = consume_topic(spark, TOPIC_ORDER, order_schema, "query_ordem")
        query_eventos = consume_topic(spark, TOPIC_EVENTOS, event_schema, "query_eventos")
        query_gps = consume_topic(spark, TOPIC_GPS, gps_schema, "query_gps")
        
        print("\n" + "="*60)
        print("✅ Todos os consumers estão rodando!")
        print("📊 Acesse a Spark UI em: http://localhost:4040")
        print("⏹️  Pressione CTRL+C para parar")
        print("="*60 + "\n")
        
        # Aguarda todas as queries
        query_ordem.awaitTermination()
        query_eventos.awaitTermination()
        query_gps.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⏹️  Parando consumer...")
    except Exception as e:
        print(f"\n❌ Erro: {e}")
    finally:
        spark.stop()
        print("✅ Spark Session encerrada")

if __name__ == "__main__":
    main()