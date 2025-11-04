"""
Conceitos centrais:
- Origem de entrada e gravação na origem

##Nesta aula iremos:

1. Comparar API Batch X Streaming
2. Construir um DataFrame streaming
3. Mostrar na tela o resultado de uma consulta streaming
4. Gravar Resultado

**Aviso:**                
Notebook com caracter apenas de estudos.


****Rodar Power Shell*****
docker exec -it spark-dev spark-submit --master local[*] /app/LAB_AULA_17_Lendo_origem_kakfa.py
"""

# imports libs
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from typing import List
from prettytable import PrettyTable


# Variaveis
kafka_config = {
    'kafka.bootstrap.servers': 'kafka:9092', 
    'startingOffsets': 'earliest'
}

topic_order = "ordem"
topic_pattern = "*"


def create_spark_session():
    """Cria a SparkSession com configurações para Kafka"""
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark



def consume_topic_batch(spark: SparkSession, topic: str, config: dict) -> fn.DataFrame:
    """
    Consome um tópico Kafka usando Batch API (para comparação)
    
    Args:
        spark: SparkSession
        topic: Nome do tópico Kafka
        kafka_config: Configurações do Kafka
    
    Returns:
        DataFrame em modo batch
    """

    df = (
        spark.read
        .format("kafka")
        .options(**config)
        .option("subscribe", topic)
        .load()
    )
    return df
    
def consume_topic_streaming(spark: SparkSession, topic: str, config: dict) -> fn.DataFrame:
    """
    Consome um tópico Kafka usando Streaming API

    Args:
        spark: SparkSession
        topic: Nome do tópico Kafka
        kafka_config: Configurações do Kafka
    
    Returns:
        DataFrame em modo streaming
    """

    df = (
        spark.readStream
        .format("kafka")
        .options(**config)
        .option("subscribe", topic)
        .load()
    )

    return df

   

def main():

    spark = create_spark_session()
    print(f"✅ SparkSession criada: {spark.version}")

    df_batch = consume_topic_batch(spark, topic_order, kafka_config)

    print("Print do dataframe de bacth")
    df_batch.show()


    df_streaming = consume_topic_streaming(spark, topic_order, kafka_config)

    print("\n🔹 Schema do DataFrame Streaming:")
    df_streaming.printSchema()
    
    print("\n🔹 Iniciando streaming query (modo STREAMING)...")
    print("   ⏱️  Processando dados a cada 5 segundos...")
    print("   ⏹️  Pressione CTRL+C para parar\n")

    query = (
        df_streaming
        .selectExpr(
            "CAST(key AS STRING) as order_key",
            "CAST(value AS STRING) as json_data",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", True)
        .trigger(processingTime="5 seconds")  # Processa a cada 5 segundos
        .start()
    )

     # Aguarda a streaming query (ou CTRL+C)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Parando streaming query...")
        query.stop()
        print("✅ Query parada com sucesso!")
    
    print("\n✅ Processamento concluído!")
    spark.stop()



if __name__ == "__main__":
    main()



