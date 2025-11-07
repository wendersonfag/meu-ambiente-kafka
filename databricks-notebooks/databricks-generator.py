# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de Dados Simulados - Kafka Simulator
# MAGIC
# MAGIC Este notebook simula o producer Kafka gerando arquivos JSON incrementalmente.
# MAGIC
# MAGIC **Como usar:**
# MAGIC 1. Execute todas as células até "Configurações"
# MAGIC 2. Ajuste os parâmetros se necessário (intervalo, quantidade de arquivos)
# MAGIC 3. Execute a célula "Iniciar Geração de Dados"
# MAGIC 4. Para parar: Cancele a execução da célula (botão Cancel)
# MAGIC
# MAGIC **Tópicos gerados:**
# MAGIC - `/FileStore/kafka-sim/kafka-orders/` - Dados de pedidos/pagamentos
# MAGIC - `/FileStore/kafka-sim/kafka-events/` - Eventos de pagamento
# MAGIC - `/FileStore/kafka-sim/kafka-gps/` - Dados de GPS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalação de Dependências

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# Restart Python para carregar as novas bibliotecas
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports e Configurações

# COMMAND ----------

import json
import time
import uuid
import random
from datetime import datetime
from faker import Faker

# Inicializa o Faker para gerar dados em pt-BR
fake = Faker('pt_BR')

print("✅ Bibliotecas carregadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configurações

# COMMAND ----------

# Diretórios DBFS para simular os tópicos Kafka
BASE_PATH = "/FileStore/kafka-sim"
TOPIC_ORDER = f"{BASE_PATH}/kafka-orders"
TOPIC_EVENTOS = f"{BASE_PATH}/kafka-events"
TOPIC_GPS = f"{BASE_PATH}/kafka-gps"

# Configurações de geração
INTERVAL_SECONDS = 5  # Intervalo entre cada geração (simula frequência do producer)
MAX_FILES = 100       # Número máximo de arquivos a gerar (0 = infinito)

# Coordenadas de SP (baseado no producer original)
LAT_MIN, LAT_MAX = -23.6, -23.5
LON_MIN, LON_MAX = -46.7, -46.6

print(f"📂 Diretórios configurados:")
print(f"   - Ordens:  {TOPIC_ORDER}")
print(f"   - Eventos: {TOPIC_EVENTOS}")
print(f"   - GPS:     {TOPIC_GPS}")
print(f"\n⏱️  Intervalo: {INTERVAL_SECONDS} segundos")
print(f"📊 Máximo de arquivos: {MAX_FILES if MAX_FILES > 0 else 'Ilimitado'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funções para Gerar Dados Falsos

# COMMAND ----------

def get_current_timestamp():
    """Retorna o timestamp atual formatado."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def create_fake_order(sequence_id):
    """Gera uma mensagem de 'order' simplificada com sequence_id."""
    amount = round(random.uniform(10.0, 500.0), 2)

    return {
        "data": {
            "_sequence_id": sequence_id,  # Ordem de entrada na Bronze
            "payment_id": str(uuid.uuid4()),
            "order_key": str(uuid.uuid4()),
            "amount": amount,
            "currency": random.choice(["BRL", "USD", "EUR"]),
            "method": random.choice(["Card", "Pix", "Boleto"]),
            "status": random.choice(["succeeded", "pending", "failed"]),
            "card_brand": random.choice(["Visa", "MasterCard", "Elo"]),
            "card_last4": fake.credit_card_number()[-4:],
            "net_amount": round(amount * 0.9, 2),
            "country": "BR",
            "ip_address": fake.ipv4(),
            "timestamp": get_current_timestamp(),
            "dt_current_timestamp": get_current_timestamp()
        }
    }

def create_fake_event(payment_id, sequence_id):
    """Gera uma mensagem de 'eventos' ligada a um payment_id com sequence_id."""
    event_name = random.choice(["created", "authorized", "captured", "succeeded", "refunded"])
    return {
        "data": {
            "_sequence_id": sequence_id,  # Ordem de entrada na Bronze
            "event_id": str(uuid.uuid4()),
            "payment_id": payment_id,
            "event": {
                "event_name": event_name,
                "timestamp": get_current_timestamp()
            },
            "dt_current_timestamp": get_current_timestamp()
        }
    }

def create_fake_gps(order_id, sequence_id):
    """Gera uma mensagem de 'gps' ligada a um order_id com sequence_id."""
    return {
        "data": {
            "_sequence_id": sequence_id,  # Ordem de entrada na Bronze
            "gps_id": str(uuid.uuid4()),
            "order_id": order_id,
            "lat": round(random.uniform(LAT_MIN, LAT_MAX), 6),
            "lon": round(random.uniform(LON_MIN, LON_MAX), 6),
            "speed_kph": random.randint(10, 60),
            "accuracy_m": round(random.uniform(5.0, 50.0), 1),
            "timestamp": get_current_timestamp(),
            "dt_current_timestamp": get_current_timestamp()
        }
    }

print("✅ Funções de geração de dados definidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Função para Salvar no DBFS

# COMMAND ----------

def save_to_dbfs(data, topic_path, file_id):
    """
    Salva dados JSON no DBFS simulando mensagens Kafka.

    Args:
        data: Dicionário com os dados
        topic_path: Caminho do tópico (diretório)
        file_id: ID único para o arquivo
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{topic_path}/msg_{timestamp}_{file_id}.json"

    # Converte para JSON
    json_data = json.dumps(data, ensure_ascii=False)

    # Salva no DBFS usando dbutils
    dbutils.fs.put(filename, json_data, overwrite=True)

    return filename

print("✅ Função de salvamento definida!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Limpar Dados Antigos (Opcional)

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Execute esta célula apenas se quiser limpar dados antigos antes de começar**

# COMMAND ----------

# Descomente as linhas abaixo para limpar os diretórios
# dbutils.fs.rm(TOPIC_ORDER, recurse=True)
# dbutils.fs.rm(TOPIC_EVENTOS, recurse=True)
# dbutils.fs.rm(TOPIC_GPS, recurse=True)
# print("🗑️  Dados antigos removidos!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criar Diretórios

# COMMAND ----------

# Garante que os diretórios existem
try:
    dbutils.fs.mkdirs(TOPIC_ORDER)
    dbutils.fs.mkdirs(TOPIC_EVENTOS)
    dbutils.fs.mkdirs(TOPIC_GPS)
    print("✅ Diretórios criados/verificados com sucesso!")
except Exception as e:
    print(f"⚠️  Aviso: {e}")
    print("   (Isso é normal se os diretórios já existirem)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Iniciar Geração de Dados
# MAGIC
# MAGIC **🚀 Execute esta célula para iniciar a geração de dados!**
# MAGIC
# MAGIC Para parar: clique no botão "Cancel" da célula em execução.

# COMMAND ----------

print("="*70)
print("🚀 INICIANDO GERAÇÃO DE DADOS SIMULADOS")
print("="*70)
print(f"\n📂 Salvando em: {BASE_PATH}")
print(f"⏱️  Intervalo: {INTERVAL_SECONDS} segundos entre cada lote")
print(f"📊 Limite: {MAX_FILES if MAX_FILES > 0 else 'Ilimitado'} arquivos por tópico")
print("\n🛑 Para parar: Clique em 'Cancel' ou pressione 'Interrupt Kernel'\n")
print("="*70)

file_count = 0

try:
    while True:
        # Verifica se atingiu o limite
        if MAX_FILES > 0 and file_count >= MAX_FILES:
            print(f"\n✅ Limite de {MAX_FILES} arquivos atingido. Parando geração.")
            break

        file_count += 1

        # 1. Gera um pedido (order)
        order_data = create_fake_order(file_count)

        # Pega os IDs para ligar as mensagens
        payment_id = order_data['data']['payment_id']
        order_key = order_data['data']['order_key']

        # 2. Gera um evento baseado no pedido
        event_data = create_fake_event(payment_id, file_count)

        # 3. Gera um ponto de GPS baseado no pedido
        gps_data = create_fake_gps(order_key, file_count)

        # Salva os arquivos no DBFS
        order_file = save_to_dbfs(order_data, TOPIC_ORDER, file_count)
        event_file = save_to_dbfs(event_data, TOPIC_EVENTOS, file_count)
        gps_file = save_to_dbfs(gps_data, TOPIC_GPS, file_count)

        # Log de progresso
        print(f"\n[{file_count:04d}] {datetime.now().strftime('%H:%M:%S')}")
        print(f"  ✅ Ordem:   payment_id={payment_id[:8]}... | amount={order_data['data']['amount']:.2f} {order_data['data']['currency']}")
        print(f"  ✅ Evento:  event={event_data['data']['event']['event_name']}")
        print(f"  ✅ GPS:     lat={gps_data['data']['lat']:.4f}, lon={gps_data['data']['lon']:.4f}")

        # Aguarda antes do próximo lote
        time.sleep(INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("\n\n🛑 Geração interrompida pelo usuário.")
except Exception as e:
    print(f"\n\n❌ Erro durante a geração: {e}")
finally:
    print("\n" + "="*70)
    print(f"📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"Total de arquivos gerados: {file_count} por tópico")
    print(f"Total geral: {file_count * 3} arquivos")
    print("\n✅ Geração finalizada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verificar Arquivos Gerados

# COMMAND ----------

print("📂 Verificando arquivos gerados...\n")

for topic_name, topic_path in [("KAFKA-ORDERS", TOPIC_ORDER), ("KAFKA-EVENTS", TOPIC_EVENTOS), ("KAFKA-GPS", TOPIC_GPS)]:
    try:
        files = dbutils.fs.ls(topic_path)
        print(f"📁 {topic_name}: {len(files)} arquivos")

        # Mostra os 3 primeiros arquivos como exemplo
        if len(files) > 0:
            print(f"   Exemplos:")
            for f in files[:3]:
                print(f"   - {f.name}")
    except Exception as e:
        print(f"⚠️  {topic_name}: Nenhum arquivo encontrado ou erro: {e}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Visualizar Conteúdo de um Arquivo (Exemplo)

# COMMAND ----------

# Lê e exibe o conteúdo do primeiro arquivo de ordem como exemplo
try:
    files = dbutils.fs.ls(TOPIC_ORDER)
    if len(files) > 0:
        sample_file = files[0].path
        content = dbutils.fs.head(sample_file)

        print(f"📄 Exemplo de conteúdo (primeiro arquivo de KAFKA-ORDERS):")
        print(f"   Arquivo: {files[0].name}\n")

        # Parse e formata o JSON para melhor visualização
        parsed = json.loads(content)
        print(json.dumps(parsed, indent=2, ensure_ascii=False))
    else:
        print("⚠️  Nenhum arquivo encontrado ainda. Execute a célula de geração primeiro.")
except Exception as e:
    print(f"❌ Erro ao ler arquivo: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Próximos Passos
# MAGIC
# MAGIC 1. ✅ **Dados gerados** - Arquivos JSON criados nos diretórios DBFS
# MAGIC 2. 🚀 **Próximo notebook** - Abra o `databricks-consumer.py` para consumir esses dados usando Auto Loader
# MAGIC 3. 📖 **Documentação** - Consulte `DATABRICKS-SIMULATOR.md` para mais detalhes
# MAGIC
# MAGIC ---
