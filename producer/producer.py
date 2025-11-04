# Salve como: producer/producer.py
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# --- Configuração ---
# ATENÇÃO: Mudar de 'localhost' para o nome do serviço no docker-compose, ex: 'kafka'
KAFKA_BROKER = 'kafka:9092' 
TOPIC_ORDER = 'ordem' # Corrigido para 'ordem' como no seu pedido
TOPIC_EVENTOS = 'eventos'
TOPIC_GPS = 'gps'

# Coordenadas de SP (baseado no seu exemplo de GPS)
LAT_MIN, LAT_MAX = -23.6, -23.5
LON_MIN, LON_MAX = -46.7, -46.6

# Inicializa o Faker para gerar dados em pt-BR
fake = Faker('pt_BR')

# --- Funções para Gerar Dados Falsos ---

def get_current_timestamp():
    """Retorna o timestamp atual formatado."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def create_fake_order():
    """Gera uma mensagem de 'order' simplificada."""
    amount = round(random.uniform(10.0, 500.0), 2)
    
    return {
        "data": {
            "payment_id": str(uuid.uuid4()),
            "order_key": str(uuid.uuid4()),
            "amount": amount,
            "currency": random.choice(["BRL", "USD", "EUR"]),
            "method": random.choice(["Card", "Pix", "Boleto"]),
            "status": random.choice(["succeeded", "pending", "failed"]),
            "card_brand": random.choice(["Visa", "MasterCard", "Elo"]),
            "card_last4": fake.credit_card_number()[-4:],
            "net_amount": round(amount * 0.9, 2), # Simulação
            "country": "BR",
            "ip_address": fake.ipv4(),
            "timestamp": get_current_timestamp(),
            "dt_current_timestamp": get_current_timestamp()
        }
    }

def create_fake_event(payment_id):
    """Gera uma mensagem de 'eventos' ligada a um payment_id."""
    event_name = random.choice(["created", "authorized", "captured", "succeeded", "refunded"])
    return {
        "data": {
            "event_id": str(uuid.uuid4()),
            "payment_id": payment_id,
            "event": {
                "event_name": event_name,
                "timestamp": get_current_timestamp()
            },
            "dt_current_timestamp": get_current_timestamp()
        }
    }

def create_fake_gps(order_id):
    """Gera uma mensagem de 'gps' ligada a um order_id."""
    return {
        "data": {
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

# --- Produtor Principal ---

def wait_for_kafka(broker):
    """Espera o Kafka ficar disponível antes de conectar."""
    print(f"Tentando conectar ao Kafka em {broker}...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                client_id='python-producer'
            )
            print("Conexão com Kafka estabelecida!")
            return producer
        except Exception as e:
            print(f"Kafka ainda não está pronto ({e}). Tentando novamente em 5s...")
            time.sleep(5)


def main():
    producer = wait_for_kafka(KAFKA_BROKER)
    
    print("Produtor conectado com sucesso! Pressione CTRL+C para parar.")

    try:
        while True:
            # 1. Gera um pedido (order)
            order_data = create_fake_order()
            
            # Pega os IDs para ligar as mensagens
            payment_id = order_data['data']['payment_id']
            order_key = order_data['data']['order_key']
            
            # 2. Gera um evento baseado no pedido
            event_data = create_fake_event(payment_id)
            
            # 3. Gera um ponto de GPS baseado no pedido
            gps_data = create_fake_gps(order_key)

            # --- Envia as mensagens para os tópicos ---
            
            producer.send(TOPIC_ORDER, value=order_data, key=order_key.encode('utf-8'))
            print(f"\nEnviado para '{TOPIC_ORDER}': \tkey={order_key[:8]}...")

            producer.send(TOPIC_EVENTOS, value=event_data, key=payment_id.encode('utf-8'))
            print(f"Enviado para '{TOPIC_EVENTOS}': \tkey={payment_id[:8]}...")
            
            producer.send(TOPIC_GPS, value=gps_data, key=order_key.encode('utf-8'))
            print(f"Enviado para '{TOPIC_GPS}': \tkey={order_key[:8]}...")
            
            producer.flush()
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nParando produtor...")
    finally:
        if producer:
            print("Fechando conexão com o Kafka.")
            producer.close()

if __name__ == "__main__":
    main()