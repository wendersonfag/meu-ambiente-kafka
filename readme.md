# 🚀 Workflow de Desenvolvimento - Spark Manual

## 📋 Visão Geral

Este guia mostra como usar o ambiente onde:
- **Kafka + Zookeeper + Producer** ficam rodando continuamente
- **Spark** você executa manualmente quando quiser testar seu código

## 🏗️ Configuração Inicial

### 1. Subir apenas a infraestrutura (Kafka + Producer)

```bash
# Inicia Kafka, Zookeeper, Producer e container Spark (mas não executa o código)
docker-compose up -d --build

# Verificar se está tudo rodando
docker-compose ps
```

Você verá:
```
NAME              STATUS          PORTS
kafka             Up (healthy)    0.0.0.0:9092->9092/tcp
python-producer   Up              
spark-dev         Up              0.0.0.0:4040->4040/tcp
zookeeper         Up              0.0.0.0:2181->2181/tcp
```

## 🔥 Executando o Spark Manualmente

### ⚠️ IMPORTANTE: Escolha o Terminal Correto no Windows

**Windows**: Use **PowerShell** ou **CMD** (NÃO use Git Bash)
- Git Bash converte caminhos incorretamente e causará erro

### Opção 1: Executar o consumer.py (comando único)

```powershell
# Windows PowerShell (RECOMENDADO)
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# Windows CMD
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# Git Bash (se realmente precisar usar)
MSYS_NO_PATHCONV=1 docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# Linux/Mac
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py
```

### Opção 2: Entrar no container e executar (mais flexível)

```bash
# Entra no container
docker exec -it spark-dev bash

# Dentro do container, execute:
spark-submit --master local[*] /app/consumer.py

# Ou com mais opções:
spark-submit \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
  /app/consumer.py

# Para sair do container
exit
```

### Opção 3: Executar com Python direto (para testes rápidos)

```bash
docker exec -it spark-dev python /app/consumer.py
```

## 📝 Workflow de Desenvolvimento Diário

### 1️⃣ Manhã - Iniciar o ambiente

```bash
# Sobe toda a infraestrutura
docker-compose up -d --build

# Verifica logs do producer (opcional)
docker-compose logs -f python-producer
```

### 2️⃣ Durante o dia - Desenvolver e testar

```powershell
# IMPORTANTE: Use PowerShell ou CMD no Windows (não Git Bash)

# 1. Edite o arquivo spark-consumer/consumer.py no seu editor favorito (VS Code, PyCharm, etc.)

# 2. Teste suas alterações (PowerShell/CMD)
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# 3. Se der erro, CTRL+C e ajuste o código

# 4. Execute novamente (não precisa rebuild!)
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py
```

### 3️⃣ Fim do dia - Parar tudo

```bash
# Para todos os containers
docker-compose down

# Ou se quiser manter os dados do Kafka:
docker-compose stop
```

## 🎯 Comandos Úteis

### Ver logs em tempo real

```bash
# Todos os serviços
docker-compose logs -f

# Apenas o producer
docker-compose logs -f python-producer

# Apenas o Kafka
docker-compose logs -f kafka
```

### Ver tópicos do Kafka

```bash
# Lista os tópicos
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Descreve um tópico
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic ordem
```

### Consumir mensagens do Kafka manualmente

```bash
# Ver mensagens do tópico 'ordem'
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ordem \
  --from-beginning \
  --max-messages 5

# Ver mensagens do tópico 'eventos'
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic eventos \
  --from-beginning \
  --max-messages 5
```

### Limpar checkpoint do Spark (se necessário)

```bash
docker exec -it spark-dev rm -rf /tmp/checkpoint
```

### Acessar Spark UI

Quando o Spark estiver rodando, acesse:
```
http://localhost:4040
```

## 🔧 Testando Diferentes Configurações do Spark

### Com mais memória

```bash
docker exec -it spark-dev spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  /app/consumer.py
```

### Com mais cores

```bash
docker exec -it spark-dev spark-submit \
  --master local[4] \
  /app/consumer.py
```

### Com configurações customizadas

```bash
docker exec -it spark-dev spark-submit \
  --master local[*] \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /app/consumer.py
```

## 🐛 Troubleshooting

### Spark não conecta ao Kafka
```bash
# Verifica se o Kafka está rodando
docker-compose ps kafka

# Verifica logs do Kafka
docker-compose logs kafka
```

### Código não atualiza
```bash
# As mudanças são automáticas porque usamos volumes!
# Apenas execute novamente:
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py
```

### Quer reconstruir o container Spark
```bash
# Se mudou o Dockerfile ou requirements.txt:
docker-compose up -d --build spark-dev
```

## 💡 Dicas Pro

### ⚠️ Problema com Git Bash no Windows

O Git Bash converte caminhos Linux automaticamente, causando erros como:
```
python3: can't open file '/app/C:/Program Files/Git/app/consumer.py'
```

**Soluções:**
1. **Use PowerShell ou CMD** (recomendado)
2. No Git Bash, adicione `MSYS_NO_PATHCONV=1` antes do comando
3. No Git Bash, use `//app` ao invés de `/app`

### Criar aliases no seu terminal

**Windows PowerShell** - Adicione no seu `$PROFILE`:
```powershell
# Para criar/editar o profile:
# notepad $PROFILE

function Run-SparkJob { docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py }
function Spark-Shell { docker exec -it spark-dev bash }
function Kafka-Logs { docker-compose logs -f python-producer }

Set-Alias spark-run Run-SparkJob
Set-Alias spark-shell Spark-Shell
Set-Alias kafka-logs Kafka-Logs
```

**Linux/Mac** - Adicione no `.bashrc` ou `.zshrc`:
```bash
alias spark-run="docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py"
alias spark-shell="docker exec -it spark-dev bash"
alias kafka-logs="docker-compose logs -f python-producer"
```

Depois você pode simplesmente digitar:
```bash
spark-run    # Executa o job
spark-shell  # Entra no container
kafka-logs   # Ver logs do producer
```

## 📊 Exemplo de Sessão Completa

```powershell
# NOTA: Use PowerShell ou CMD no Windows

# 1. Sobe o ambiente
docker-compose up -d --build

# 2. Verifica se está tudo ok
docker-compose ps

# 3. Ver mensagens sendo produzidas
docker-compose logs -f python-producer

# 4. Em outro terminal PowerShell/CMD, executa o Spark
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# 5. Acessa Spark UI no navegador
# http://localhost:4040

# 6. Para parar o Spark: CTRL+C

# 7. Edita o código no VS Code
# code spark-consumer/consumer.py

# 8. Executa novamente
docker exec -it spark-dev spark-submit --master local[*] /app/consumer.py

# 9. Ao final do dia
docker-compose down
```

## 🎓 Comparação: Automático vs Manual

| Aspecto | Modo Automático | Modo Manual (Atual) |
|---------|----------------|---------------------|
| **Iniciar Spark** | Automático ao subir | Você controla quando executar |
| **Testar mudanças** | Precisa rebuild | Instantâneo (apenas execute novamente) |
| **Desenvolvimento** | Mais lento | Mais rápido e flexível |
| **Produção** | ✅ Ideal | ❌ Não recomendado |
| **Aprendizado** | Menos controle | ✅ Mais controle e entendimento |

---

**✨ Vantagens deste workflow:**
- ⚡ Testa mudanças instantaneamente
- 🎯 Controle total sobre quando executar
- 🔧 Fácil debugar e experimentar
- 📚 Melhor para aprendizado