# Ambiente Kafka com Bore para Databricks

Este ambiente automatiza a configuração do Kafka com exposição externa via Bore para uso no Databricks.

## IMPORTANTE - Primeira Execução

Antes de rodar o script pela primeira vez, certifique-se de que existe o arquivo `docker-compose.template.yml`. Se não existir, ele será criado automaticamente a partir do `docker-compose.yml`.

## Pré-requisitos

### 1. Docker Desktop
- Instale Docker Desktop: https://www.docker.com/products/docker-desktop

### 2. Bore (Túnel)
Escolha uma das opções:

**Opção A: Via Cargo (Rust)**
```powershell
# Instalar Rust
winget install Rustlang.Rustup

# Instalar Bore
cargo install bore-cli
```

**Opção B: Download direto**
- Baixe o executável: https://github.com/ekzhang/bore/releases
- Coloque na pasta do projeto ou no PATH do sistema

## Como Usar

### Opção 1: Teste Local (Sem Bore)

Para testar o ambiente localmente sem o Bore:

```powershell
cd "c:\Workspace\Engenharia de Dados Academy\Formacao-spark-databricks\meu-ambiente-kafka"
.\teste-manual.ps1
```

### Opção 2: Iniciar com Bore (Para Databricks)

Para iniciar com exposição externa via Bore:

```powershell
cd "c:\Workspace\Engenharia de Dados Academy\Formacao-spark-databricks\meu-ambiente-kafka"
.\start-kafka-env.ps1
```

O script vai:
1. Limpar processos antigos
2. Verificar/instalar Bore
3. Iniciar túnel Bore e capturar URL pública
4. Atualizar docker-compose.yml automaticamente
5. Subir todos os containers
6. Exibir resumo com URLs de conexão

### Parar o Ambiente

Pressione `Ctrl+C` no terminal onde o script está rodando, ou execute:

```powershell
docker-compose down
```

## Configuração no Databricks

Após iniciar o ambiente, use a URL mostrada no resumo:

```python
# Exemplo de configuração no Databricks
kafka_bootstrap_servers = "bore.pub:12345"  # Use a URL exibida pelo script

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "seu-topico") \
  .load()
```

## Estrutura do Ambiente

### Serviços Disponíveis

| Serviço | Porta Local | Descrição |
|---------|-------------|-----------|
| Zookeeper | 2181 | Coordenação do Kafka |
| Kafka (interno) | 9092 | Comunicação entre containers |
| Kafka (local) | 29092 | Acesso localhost |
| Kafka (externo) | 19092 → Bore | Acesso via túnel público |
| Spark UI | 4040 | Interface do Spark |

### Listeners do Kafka

- **PLAINTEXT** (kafka:9092): Comunicação interna entre containers
- **PLAINTEXT_HOST** (localhost:29092): Testes locais no Windows
- **EXTERNAL** (bore.pub:xxxxx): Acesso externo via Databricks

## Arquivos Gerados

O script gera automaticamente:

- `bore_url.txt`: URL pública do Bore (ignorada pelo Git)
- `docker-compose.template.yml`: Backup do docker-compose original (ignorada pelo Git)

## Troubleshooting

### Erro: "Bore não encontrado"
```powershell
# Instale via Cargo
cargo install bore-cli

# Ou baixe o executável e adicione ao PATH
```

### Erro: "Docker não está rodando"
```powershell
# Inicie o Docker Desktop manualmente
```

### Erro: Kafka não sobe (Exited 1)
Se o Kafka falhar ao iniciar, provavelmente o `docker-compose.yml` tem uma URL antiga do Bore:

```powershell
# Solução 1: Rode o script start-kafka-env.ps1 que corrige automaticamente

# Solução 2: Restaure o template manualmente
cd "c:\Workspace\Engenharia de Dados Academy\Formacao-spark-databricks\meu-ambiente-kafka"
Copy-Item docker-compose.template.yml docker-compose.yml -Force
docker-compose down
docker-compose up -d
```

### Porta em uso
```powershell
# Verifique se há processos usando as portas
netstat -ano | findstr "9092"
netstat -ano | findstr "19092"

# Pare os processos ou mude as portas no docker-compose.yml
```

### Kafka não conecta no Databricks
1. Verifique se o Bore está rodando: deve aparecer no output do script
2. Confirme a URL no `bore_url.txt`
3. Teste a conectividade de fora da rede local
4. Verifique firewall e antivírus
5. Certifique-se de usar a URL mais recente exibida pelo script

## Dicas

- **Sempre use o script**: Ele garante que a URL do Bore está atualizada no docker-compose
- **URL muda a cada execução**: O Bore gera uma porta diferente toda vez
- **Mantenha o script rodando**: Ele monitora os logs e mantém o túnel ativo
- **Para desenvolvimento local**: Use `localhost:29092` em vez da URL do Bore

## Próximos Passos

1. Execute o script
2. Copie a URL do Bore exibida
3. Use a URL no seu notebook do Databricks
4. Teste a conexão lendo/escrevendo no tópico Kafka

## Suporte

Para problemas com:
- **Bore**: https://github.com/ekzhang/bore
- **Kafka**: https://kafka.apache.org/documentation/
- **Docker**: https://docs.docker.com/
