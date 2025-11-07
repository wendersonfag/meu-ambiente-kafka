# 🥉 Consumer Bronze - Guia de Uso

## 📌 Visão Geral

O **Consumer Bronze** é um notebook Databricks que:
- ✅ Lê dados **em tempo real** dos 3 tópicos Kafka (`ordem`, `eventos`, `gps`)
- ✅ Captura dados brutos com **metadados Kafka** (key, value, partition, offset, timestamp)
- ✅ Cria **3 tabelas Bronze** no Delta Lake
- ✅ Processa com **trigger de 5 segundos**
- ✅ Estrutura idêntica à imagem de referência

## 📂 Arquivo

```
databricks-notebooks/databricks-bronze-consumer.py
```

## 🚀 Como Usar

### 1. Pré-requisitos

Certifique-se de que:

```powershell
# Terminal 1: Inicie o ambiente Kafka
.\start-kafka-env.ps1

# Aguarde até ver:
# ✅ Kafka rodando
# ✅ Producer gerando dados
# ✅ Bore URL disponível
```

### 2. Configurar Kafka Broker (se usando Bore)

Se estiver usando **Bore tunnel** para acessar de Databricks remotamente:

```powershell
# O script start-kafka-env.ps1 já cria bore_url.txt
# Verifique o conteúdo:
Get-Content bore_url.txt
# Saída exemplo: bore.pub:53049
```

### 3. Executar no Databricks

1. Abra o Databricks workspace
2. Vá para: **Workspace > Repos > seu-projeto**
3. Abra o notebook: `databricks-bronze-consumer`
4. **Execute todas as células** clicando no botão ▶️ **Run All**

## 📊 Estrutura das Tabelas Bronze

Cada tabela Bronze tem a seguinte estrutura:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `key` | STRING | Chave da mensagem Kafka |
| `value` | STRING | Payload JSON da mensagem |
| `partition` | INTEGER | Partição Kafka |
| `offset` | LONG | Offset da mensagem |
| `timestamp` | TIMESTAMP | Timestamp da mensagem Kafka |
| `timestampType` | INTEGER | Tipo de timestamp Kafka |
| `topic` | STRING | Nome do tópico |
| `bronze_ingestion_timestamp` | TIMESTAMP | Timestamp de ingestão no Bronze |

### Tabelas Criadas

```sql
-- Tabela 1: Pedidos/Pagamentos
SELECT * FROM kafka_bronze_ordem

-- Tabela 2: Eventos de Pagamento
SELECT * FROM kafka_bronze_eventos

-- Tabela 3: Dados de GPS
SELECT * FROM kafka_bronze_gps
```

## 🔄 Como Funciona

### 1. Leitura Kafka
```python
spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BROKER)
  .option("subscribe", "ordem|eventos|gps")
  .load()
```

### 2. Captura de Metadados
```python
df.select(
    "key",           # Chave da mensagem
    "value",         # Valor/payload
    "partition",     # Partição Kafka
    "offset",        # Offset
    "timestamp",     # Timestamp Kafka
    "timestampType"  # Tipo de timestamp
)
```

### 3. Escrita em Delta
```python
df.writeStream
  .format("delta")
  .mode("append")
  .trigger(processingTime="5 seconds")
  .table("kafka_bronze_ordem")
```

## 💾 Configurações

No notebook, você pode ajustar:

```python
# Broker Kafka
KAFKA_BROKER = "bore.pub:53049"  # ou "localhost:29092" para local

# Intervalo de processamento (trigger)
TRIGGER_INTERVAL = "5 seconds"  # ou "10 seconds", "1 minute"

# Checkpoint (para rastrear offsets processados)
CHECKPOINT_BASE = "/tmp/kafka-bronze-checkpoints"
```

## 🔍 Monitorar os Streams

### Visualizar Dados Bronze

O notebook exibe automaticamente:
- ✅ Últimas 100 linhas de cada tabela
- ✅ Contagem de linhas por tabela
- ✅ Status dos streams ativos

### Verificar Streams Ativos

```python
# Célula 11 do notebook mostra:
spark.streams.active  # Lista todos os streams rodando
```

### Query SQL para Explorar

```sql
-- Ver dados mais recentes
SELECT * FROM kafka_bronze_ordem
ORDER BY bronze_ingestion_timestamp DESC
LIMIT 10

-- Contar por partição
SELECT partition, COUNT(*) as count
FROM kafka_bronze_ordem
GROUP BY partition

-- Verificar offsets
SELECT partition, MAX(offset) as max_offset
FROM kafka_bronze_ordem
GROUP BY partition ORDER BY partition
```

## 🛑 Parar os Streams

Para parar **sem perder dados**:

1. **Opção 1**: Descomente a célula 12 e execute:
```python
for stream in spark.streams.active:
    stream.stop()
```

2. **Opção 2**: Clique em **Cancel** no Databricks

Os dados já salvos permanecerão nas tabelas Bronze.

## 🗑️ Resetar e Reprocessar

Para **limpar checkpoints** e reprocessar tudo do início:

1. Descomente a célula 13:
```python
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
```

2. Execute novamente o notebook

⚠️ **Aviso**: Isso **não apaga** a tabela Bronze, apenas reprocessa desde o início dos tópicos Kafka.

## 📈 Próximos Passos

### Criar Camada Silver

Depois que os dados estão na Bronze, você pode criar a **Silver** com dados limpos:

```python
# Exemplo: Parsear JSON de ordem
df_silver_ordem = spark.sql("""
    SELECT
        topic,
        partition,
        offset,
        timestamp,
        from_json(value, 'payment_id STRING, order_key STRING, amount DOUBLE, ...') as data
    FROM kafka_bronze_ordem
    WHERE value IS NOT NULL
""")
```

### Criar Camada Gold

Depois da Silver, criar **Gold** com agregações:

```python
# Exemplo: Resumo por status
df_gold = spark.sql("""
    SELECT
        from_json(value, schema).data.status,
        COUNT(*) as count,
        SUM(from_json(value, schema).data.amount) as total_amount
    FROM kafka_bronze_ordem
    GROUP BY status
""")
```

## 🔧 Troubleshooting

### Problema: "Não consegue conectar ao Kafka"
- ✅ Verifique se `start-kafka-env.ps1` foi executado
- ✅ Verifique o `bore_url.txt` tem uma URL válida
- ✅ Teste: `nc -zv bore.pub xxxxx` (no seu terminal)

### Problema: "Tabela não encontrada"
- ✅ As tabelas são criadas automaticamente na primeira execução
- ✅ Aguarde a célula 5 completar
- ✅ Verifique: `SHOW TABLES LIKE 'kafka_bronze%'`

### Problema: "Sem dados nas tabelas"
- ✅ Verifique se o producer está rodando: `docker-compose logs python-producer`
- ✅ Aguarde alguns segundos (trigger é 5s)
- ✅ Verifique tópicos: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`

### Problema: "Checkpoint location não acessível"
- ✅ Se usar DBFS, ajuste em Databricks para `/dbfs/tmp/kafka-bronze-checkpoints`
- ✅ Ou use Unity Catalog: `/Volumes/main/default/checkpoint`

## 📚 Estrutura Completa do Projeto

```
meu-ambiente-kafka/
├── docker-compose.yml
├── producer/
│   └── producer.py
├── spark-consumer/
│   └── consumer.py (local)
├── databricks-notebooks/
│   ├── databricks-generator.py (simula dados)
│   ├── databricks-consumer.py (Auto Loader)
│   └── databricks-bronze-consumer.py ⭐ (novo - Kafka em tempo real)
├── start-kafka-env.ps1
└── CLAUDE.md

```

## 💡 Dicas Importantes

1. **Trigger de 5 segundos**: Significa que a cada 5 segundos, novos dados são processados
2. **Modo Append**: Dados novos são **adicionados**, nunca sobrescrevem
3. **Checkpoints**: Garantem que cada mensagem é processada exatamente uma vez
4. **Metadados Kafka**: Preservados para auditoria e rastreamento

## 🎯 Exemplo Completo de Uso

```bash
# Terminal 1: Inicie Kafka
.\start-kafka-env.ps1

# Aguarde mensagens:
# ✅ Producer gerando dados
# ✅ Bore URL criada

# Terminal 2 (Databricks): Execute o notebook
# 1. Abra databricks-bronze-consumer.py
# 2. Clique "Run All"
# 3. Aguarde ~30 segundos

# Terminal 2: Verifique os dados
SELECT COUNT(*) FROM kafka_bronze_ordem  -- Crescendo a cada 5s

# Terminal 1: Parar quando finalizado
Ctrl+C
docker-compose down
```

## 📞 Suporte

Para dúvidas, verifique:
- ✅ `CLAUDE.md` - Documentação geral
- ✅ `TESTE-DATABRICKS.md` - Testes específicos
- ✅ Spark UI: http://localhost:4040 (quando rodando localmente)
- ✅ Databricks: Clusters > Your Cluster > Apps

---

**Última Atualização**: 2025-11-07
**Versão**: 1.0
**Spark**: 3.5.0+
**Databricks Runtime**: 13.3+ (com Kafka suportado)
