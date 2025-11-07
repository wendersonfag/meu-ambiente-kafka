# Databricks Kafka Simulator - Medallion Architecture

Este simulador permite testar e estudar Spark Structured Streaming no Databricks **sem precisar de conexão Kafka** ou tunelamento via Bore.

Implementa a **Arquitetura Medallion** (Bronze-Silver-Gold) com ingestion em tempo real na camada Bronze.

## Visão Geral

O simulador consiste em 2 notebooks Python que replicam o comportamento do ambiente Kafka local:

1. **databricks-generator.py** - Gera arquivos JSON simulando os 3 tópicos Kafka com sequence_id para ordem
2. **databricks-consumer.py** - Consome esses arquivos com Auto Loader e grava na camada **Bronze** em Delta Lake

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────────┐
│                     DATABRICKS - MEDALLION ARCH                      │
│                                                                      │
│  ┌──────────────────┐         ┌──────────────┐      ┌─────────────┐ │
│  │   Generator      │         │   Consumer   │      │   Bronze    │ │
│  │   Notebook       │         │   Notebook   │      │   (Delta)   │ │
│  │                  │         │              │      │             │ │
│  │  Gera JSONs      │────────>│ Auto Loader  │─────>│ Tabela      │ │
│  │  + sequence_id   │  DBFS   │ (readStream) │      │ kafka_      │ │
│  │  a cada 5s       │         │              │      │ multiplex   │ │
│  │                  │         │ Transforma:  │      │             │ │
│  │  • kafka-orders  │         │ • key        │      │ key         │ │
│  │  • kafka-events  │         │ • value      │      │ value       │ │
│  │  • kafka-gps     │         │ • topic      │      │ topic       │ │
│  └──────────────────┘         │ • partition  │      │ partition   │ │
│                               │ • offset     │      │ offset      │ │
│  📂 /FileStore/kafka-sim/      │ • timestamp  │      │ timestamp   │ │
│     ├── kafka-orders/          │ • ingestion  │      │ ingestion_  │ │
│     ├── kafka-events/          └──────────────┘      │ timestamp   │ │
│     └── kafka-gps/                                   │             │ │
│                                                      │ Trigger: 5s │ │
│                                   📍 Checkpoint:     │ Formato:    │ │
│                            /tmp/kafka_multiplex     │ Delta       │ │
│                            _checkpoint              └─────────────┘ │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Estrutura dos Dados

Os dados gerados são **idênticos** ao ambiente Kafka local, mantendo a mesma estrutura JSON:

### Tópico: kafka-orders

```json
{
  "data": {
    "_sequence_id": 1,
    "payment_id": "uuid",
    "order_key": "uuid",
    "amount": 123.45,
    "currency": "BRL",
    "method": "Card",
    "status": "succeeded",
    "card_brand": "Visa",
    "card_last4": "1234",
    "net_amount": 111.10,
    "country": "BR",
    "ip_address": "192.168.1.1",
    "timestamp": "2025-01-15 10:30:45.123",
    "dt_current_timestamp": "2025-01-15 10:30:45.123"
  }
}
```

### Tópico: kafka-events

```json
{
  "data": {
    "event_id": "uuid",
    "payment_id": "uuid",  // Relacionado com ordem
    "event": {
      "event_name": "authorized",
      "timestamp": "2025-01-15 10:30:45.123"
    },
    "dt_current_timestamp": "2025-01-15 10:30:45.123"
  }
}
```

### Tópico: kafka-gps

```json
{
  "data": {
    "gps_id": "uuid",
    "order_id": "uuid",  // Relacionado com ordem (order_key)
    "lat": -23.5505,
    "lon": -46.6333,
    "speed_kph": 45,
    "accuracy_m": 15.5,
    "timestamp": "2025-01-15 10:30:45.123",
    "dt_current_timestamp": "2025-01-15 10:30:45.123"
  }
}
```

## Estrutura da Camada Bronze (Hive Metastore)

Os dados são transformados e salvos em Delta Lake na tabela `bronze.kafka_multiplex`:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| **key** | LONG | Ordem sequencial de entrada (_sequence_id) |
| **value** | STRING | JSON completo do evento (convertido para string) |
| **topic** | STRING | Nome do tópico (kafka-orders, kafka-events, kafka-gps) |
| **partition** | INT | Partição simulada (0-2, baseada em hash da key) |
| **offset** | LONG | Offset sequencial dentro da partição (começa em 0) |
| **timestamp** | TIMESTAMP | Timestamp do dado original |
| **ingestion_timestamp** | TIMESTAMP | Quando o dado foi inserido na Bronze (current_timestamp) |

**Exemplo de dados na Bronze:**

```
key=1, topic="kafka-orders", partition=0, offset=0, value={"_sequence_id": 1, "payment_id": "...", ...}
key=2, topic="kafka-events", partition=1, offset=0, value={"_sequence_id": 2, "event_id": "...", ...}
key=3, topic="kafka-gps", partition=2, offset=0, value={"_sequence_id": 3, "gps_id": "...", ...}
...
```

**Localização no Hive Metastore:**
- Database: `bronze`
- Tabela: `kafka_multiplex`
- Checkpoint: `/tmp/kafka_multiplex_checkpoint`

## Guia de Uso Passo a Passo

### 1. Preparar o Ambiente Databricks

#### Opção A: Databricks Community Edition (Gratuito)

1. Acesse: https://community.cloud.databricks.com/
2. Crie uma conta gratuita (se ainda não tiver)
3. Faça login

#### Opção B: Databricks Workspace (Pago/Trial)

1. Acesse seu workspace
2. Certifique-se de ter um cluster disponível

### 2. Importar os Notebooks

#### Via Upload de Arquivos:

1. No Databricks, vá para **Workspace** (barra lateral)
2. Clique com botão direito na pasta desejada → **Import**
3. Escolha **File** e faça upload de:
   - `databricks-generator.py`
   - `databricks-consumer.py`

#### Via Git (Recomendado):

1. No Databricks, vá para **Repos**
2. Clique em **Add Repo**
3. Cole a URL do seu repositório: `https://github.com/wendersonfag/meu-ambiente-kafka`
4. Os notebooks estarão em: `databricks-notebooks/`

### 3. Criar/Iniciar um Cluster

1. No Databricks, vá para **Compute** (barra lateral)
2. Se não tiver cluster:
   - Clique em **Create Cluster**
   - Escolha configurações mínimas:
     - Runtime: **14.3 LTS** ou superior
     - Node type: Menor disponível (ex: `i3.xlarge` ou similar)
     - Autoscaling: Desabilitado (1 worker)
3. Aguarde o cluster iniciar (ícone verde)

### 4. Executar o Generator

1. Abra o notebook `databricks-generator.py`
2. Anexe ao cluster criado (dropdown superior direito)
3. Execute **Run All** ou execute célula por célula:
   - Células 1-7: Configuração e funções (execute todas)
   - Célula 8: **Iniciar Geração de Dados** (deixe rodando)

**O que acontece:**
- Gera 1 lote de dados (kafka-orders + kafka-events + kafka-gps) com sequence_id a cada 5 segundos
- Salva como arquivos JSON em `/FileStore/kafka-sim/`
- Cada arquivo JSON contém um _sequence_id para rastrear a ordem de entrada
- Exibe log de progresso no output da célula

**Dica:** Deixe gerando pelo menos 20-30 arquivos antes de iniciar o consumer (1-2 minutos).

### 5. Executar o Consumer

1. Abra o notebook `databricks-consumer.py` **em outra aba**
2. Anexe ao mesmo cluster
3. Execute **Run All** ou execute célula por célula:
   - Células 1-7: Configuração, schemas e criação do database/schema Bronze
   - Célula 9: Ler streams dos 3 tópicos
   - Célula 12: **Gravar na Camada Bronze** (inicia o stream contínuo)

**O que acontece:**
- Auto Loader detecta arquivos JSON automaticamente em `/FileStore/kafka-sim/`
- Multiplexar os 3 streams em um único stream unificado
- Transforma dados adicionando metadados do Kafka (key, partition, offset, topic, ingestion_timestamp)
- Grava em tempo real na tabela Delta `bronze.kafka_multiplex` (Hive)
- Mantém checkpoint em `/tmp/kafka_multiplex_checkpoint`
- Processa novos arquivos conforme são criados a cada 5 segundos

### 6. Consultar os Dados na Bronze

Após o consumer iniciar, você pode consultar a camada Bronze:

```sql
SELECT topic, COUNT(*) as total, MIN(key) as min_key, MAX(key) as max_key
FROM bronze.kafka_multiplex
GROUP BY topic
```

- **Visualize os dados** usando as queries SQL fornecidas no notebook
- **Monitore progresso** na célula 13 que mostra contadores em tempo real
- **Analise por tópico, partição e offset** usando as queries na célula 14

### 7. Análises na Camada Bronze (Hive)

Após os dados serem ingeridos, você pode executar queries SQL:

1. **Contar registros por tópico:**
   ```sql
   SELECT topic, COUNT(*) as count FROM bronze.kafka_multiplex GROUP BY topic
   ```

2. **Distribuição por partição:**
   ```sql
   SELECT topic, partition, COUNT(*) as count FROM bronze.kafka_multiplex GROUP BY topic, partition
   ```

3. **Verificar offsets:**
   ```sql
   SELECT topic, partition, MAX(offset) as max_offset FROM bronze.kafka_multiplex GROUP BY topic, partition
   ```

4. **Extrair dados do JSON (value):**
   ```sql
   SELECT key, topic, from_json(value, 'STRUCT<data:STRING>').data as data FROM bronze.kafka_multiplex LIMIT 10
   ```

## Comparação: Kafka Local vs Databricks Simulator

| Aspecto | Kafka Local | Databricks Simulator |
|---------|-------------|----------------------|
| **Configuração** | Docker + Bore tunnel | Apenas notebooks |
| **Latência** | Rede externa | DBFS (interno) |
| **Streaming Real** | ✅ Kafka nativo | ⚠️ Simulado (Auto Loader) |
| **Schemas** | ✅ Idênticos | ✅ Idênticos |
| **Dados** | ✅ Idênticos | ✅ Idênticos |
| **Camada Bronze** | ❌ Não incluída | ✅ Medallion Architecture |
| **Melhor para** | Produção/Testes reais | Estudos/Medallion/Prototipagem |

## Comandos Úteis

### Verificar Arquivos Gerados

```python
# No Databricks
dbutils.fs.ls("/FileStore/kafka-sim/kafka-orders")
```

### Limpar Dados Antigos

```python
# Limpar arquivos JSON
dbutils.fs.rm("/FileStore/kafka-sim", recurse=True)

# Limpar checkpoints (para reprocessar)
dbutils.fs.rm("/tmp/kafka-sim-checkpoints", recurse=True)
```

### Parar Todos os Streams

```python
# Executar no consumer notebook
for stream in spark.streams.active:
    stream.stop()
```

### Ver Streams Ativos

```python
for stream in spark.streams.active:
    print(f"{stream.name}: {stream.status}")
```

## Configurações Avançadas

### Ajustar Intervalo de Geração

No `databricks-generator.py`, célula "Configurações":

```python
INTERVAL_SECONDS = 2  # Gera a cada 2 segundos (mais rápido)
MAX_FILES = 50        # Limita a 50 arquivos por tópico
```

### Ajustar Trigger do Stream

No `databricks-consumer.py`, célula "Configurações":

```python
TRIGGER_INTERVAL = "10 seconds"  # Processa a cada 10 segundos
```

### Salvar em Delta Lake

No `databricks-consumer.py`, descomente a célula 13.1:

```python
query_save_ordem = df_ordem \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka-sim-checkpoints/save_kafka_orders") \
    .table("kafka_sim_orders")
```

Depois consulte:

```sql
SELECT * FROM kafka_sim_orders LIMIT 10
```

## Exemplos de Análises

### 1. Pedidos por Método de Pagamento

```python
display(
    df_ordem
    .groupBy("method")
    .count()
    .orderBy("count", ascending=False)
)
```

### 2. Valor Médio por Status

```python
from pyspark.sql.functions import avg, round

display(
    df_ordem
    .groupBy("status")
    .agg(
        round(avg("amount"), 2).alias("avg_amount"),
        count("*").alias("count")
    )
)
```

### 3. Eventos em Sequência

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.partitionBy("payment_id").orderBy("event_timestamp")

display(
    df_eventos
    .withColumn("seq", row_number().over(window))
    .select("payment_id", "event_name", "seq", "event_timestamp")
)
```

### 4. Mapa de Calor GPS

```python
# No display, escolha visualização "Map" e configure:
# - Latitude: lat
# - Longitude: lon
# - Keys: speed_kph

display(
    df_gps.select("lat", "lon", "speed_kph")
)
```

## Troubleshooting

### Problema: "No files found in path"

**Causa:** O generator ainda não criou arquivos.

**Solução:**
1. Verifique se o generator está rodando (célula 8)
2. Aguarde pelo menos 30 segundos
3. Execute: `dbutils.fs.ls("/FileStore/kafka-sim/kafka-orders")`

### Problema: Stream não atualiza

**Causa:** Checkpoint pode estar travado ou trigger muito longo.

**Solução:**
```python
# Limpar checkpoint
dbutils.fs.rm("/tmp/kafka-sim-checkpoints", recurse=True)

# Reiniciar o consumer
```

### Problema: "Schema mismatch"

**Causa:** Dados antigos com estrutura diferente.

**Solução:**
```python
# Limpar dados antigos
dbutils.fs.rm("/FileStore/kafka-sim", recurse=True)

# Limpar schema checkpoint
dbutils.fs.rm("/tmp/kafka-sim-checkpoints", recurse=True)

# Reiniciar generator e consumer
```

### Problema: Cluster desconectou

**Causa:** Inatividade (Community Edition desconecta após 2h).

**Solução:**
1. Reinicie o cluster em **Compute**
2. Re-execute os notebooks do início
3. Checkpoints preservam o progresso

### Problema: "Faker module not found"

**Causa:** Biblioteca não instalada no cluster.

**Solução:**
- A célula `%pip install faker` no generator instala automaticamente
- Se persistir, reinicie o kernel: `dbutils.library.restartPython()`

## Próximos Passos

Após dominar este simulador, você pode:

1. **Criar transformações customizadas** nos streams
2. **Salvar dados processados** em Delta Lake
3. **Criar dashboards** com agregações em tempo real
4. **Testar watermarks** para late data handling
5. **Implementar window operations** para análises temporais
6. **Migrar para Kafka real** quando estiver pronto (o código é muito similar)

## Recursos Adicionais

- [Databricks Auto Loader Docs](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Streaming](https://docs.databricks.com/delta/delta-streaming.html)

## Diferenças vs. Kafka Real

Para referência, estas são as principais diferenças ao migrar para Kafka:

### No Gerador:
```python
# Simulador (DBFS)
dbutils.fs.put(filename, json_data)

# Kafka Real
producer.send(topic, value=data, key=key)
```

### No Consumer:
```python
# Simulador (Auto Loader)
.readStream.format("cloudFiles").option("cloudFiles.format", "json")

# Kafka Real
.readStream.format("kafka").option("kafka.bootstrap.servers", "bore.pub:xxxx")
```

O **schema e lógica de processamento** são idênticos! 🎉

---

## Suporte

Problemas ou dúvidas:
- Consulte a seção **Troubleshooting** acima
- Revise os comentários nos notebooks
- Verifique logs de erro nas células

**Bons estudos!** 🚀
