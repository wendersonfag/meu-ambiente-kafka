# Script PowerShell para iniciar ambiente Kafka com Bore
# Autor: Automatizado para Databricks
# Data: 2025-11-02

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Iniciando Ambiente Kafka com Bore para Databricks" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Configuracoes
$KAFKA_PORT = 19092
$BORE_CONFIG_FILE = "bore_url.txt"
$DOCKER_COMPOSE_FILE = "docker-compose.yml"
$DOCKER_COMPOSE_TEMPLATE = "docker-compose.template.yml"

# Funcao para limpar processos antigos
function Stop-OldProcesses {
    Write-Host "[1/6] Limpando processos antigos..." -ForegroundColor Yellow

    # Para o Docker Compose se estiver rodando
    if (Test-Path $DOCKER_COMPOSE_FILE) {
        docker-compose down 2>$null
    }

    # Para processos bore antigos
    Get-Process | Where-Object {$_.ProcessName -like "*bore*"} | Stop-Process -Force -ErrorAction SilentlyContinue

    # Remove arquivos temporarios
    if (Test-Path $DOCKER_COMPOSE_FILE) {
        Remove-Item $DOCKER_COMPOSE_FILE -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $BORE_CONFIG_FILE) {
        Remove-Item $BORE_CONFIG_FILE -Force -ErrorAction SilentlyContinue
    }

    Write-Host "  OK Processos antigos finalizados e arquivos temporarios limpos" -ForegroundColor Green
    Write-Host ""
}

# Funcao para verificar e instalar o Bore
function Install-Bore {
    Write-Host "[2/6] Verificando instalacao do Bore..." -ForegroundColor Yellow

    if (Get-Command bore -ErrorAction SilentlyContinue) {
        Write-Host "  OK Bore ja esta instalado" -ForegroundColor Green
    } else {
        Write-Host "  Bore nao encontrado. Instalando via Cargo..." -ForegroundColor Yellow

        # Verifica se o Cargo esta instalado
        if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
            Write-Host "  ERRO Cargo (Rust) nao esta instalado!" -ForegroundColor Red
            Write-Host "  Por favor, instale Rust de: https://rustup.rs/" -ForegroundColor Red
            Write-Host "  Ou baixe o executavel do Bore de: https://github.com/ekzhang/bore/releases" -ForegroundColor Red
            exit 1
        }

        cargo install bore-cli
        Write-Host "  OK Bore instalado com sucesso" -ForegroundColor Green
    }
    Write-Host ""
}

# Funcao para iniciar o Bore e capturar a URL
function Start-Bore {
    Write-Host "[3/6] Iniciando tunel Bore..." -ForegroundColor Yellow

    # Remove arquivo de configuracao antigo
    if (Test-Path $BORE_CONFIG_FILE) {
        Remove-Item $BORE_CONFIG_FILE -Force
    }

    # Inicia o Bore em background e captura o output
    $job = Start-Job -ScriptBlock {
        param($port)
        bore local $port --to bore.pub 2>&1
    } -ArgumentList $KAFKA_PORT

    # Aguarda a URL ser gerada (timeout de 30 segundos)
    $timeout = 30
    $elapsed = 0
    $boreUrl = $null

    while ($elapsed -lt $timeout) {
        Start-Sleep -Seconds 1
        $elapsed++

        $output = Receive-Job -Job $job 2>&1 | Out-String

        if ($output -match "bore\.pub:(\d+)") {
            $borePort = $matches[1]
            $boreUrl = "bore.pub:$borePort"
            break
        }
    }

    if (-not $boreUrl) {
        Write-Host "  ERRO Timeout ao iniciar Bore!" -ForegroundColor Red
        Stop-Job -Job $job
        Remove-Job -Job $job
        exit 1
    }

    # Salva a URL em arquivo
    $boreUrl | Out-File -FilePath $BORE_CONFIG_FILE -Encoding UTF8

    Write-Host "  OK Bore iniciado com sucesso!" -ForegroundColor Green
    Write-Host "  URL Externa: $boreUrl" -ForegroundColor Cyan
    Write-Host ""

    return $boreUrl
}

# Funcao para criar/restaurar template do docker-compose
function Ensure-DockerComposeTemplate {
    if (-not (Test-Path $DOCKER_COMPOSE_TEMPLATE)) {
        Write-Host "  Criando template do docker-compose..." -ForegroundColor Yellow
        Copy-Item $DOCKER_COMPOSE_FILE $DOCKER_COMPOSE_TEMPLATE
        Write-Host "  OK Template criado" -ForegroundColor Green
    }
}

# Funcao para atualizar o docker-compose com a URL do Bore
function Update-DockerCompose {
    param([string]$BoreUrl)

    Write-Host "[4/6] Atualizando docker-compose.yml..." -ForegroundColor Yellow

    Ensure-DockerComposeTemplate

    # IMPORTANTE: Sempre recria do template para garantir configuracao correta
    Copy-Item $DOCKER_COMPOSE_TEMPLATE $DOCKER_COMPOSE_FILE -Force
    Write-Host "  >> Copiado do template" -ForegroundColor Gray

    # Le o novo arquivo
    $content = Get-Content $DOCKER_COMPOSE_FILE -Raw

    # Substitui a URL do Bore usando regex (aceita qualquer formato de URL externa)
    $content = $content -replace "EXTERNAL://bore\.pub:\d+", "EXTERNAL://$BoreUrl"

    # Salva o arquivo atualizado
    $content | Out-File -FilePath $DOCKER_COMPOSE_FILE -Encoding UTF8 -NoNewline

    Write-Host "  OK Docker-compose atualizado com: $BoreUrl" -ForegroundColor Green
    Write-Host ""
}

# Funcao para iniciar o Docker Compose
function Start-DockerCompose {
    Write-Host "[5/6] Iniciando containers Docker..." -ForegroundColor Yellow

    docker-compose up -d

    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK Containers iniciados com sucesso!" -ForegroundColor Green
    } else {
        Write-Host "  ERRO ao iniciar containers!" -ForegroundColor Red
        exit 1
    }
    Write-Host ""
}

# Funcao para exibir informacoes finais
function Show-Summary {
    param([string]$BoreUrl)

    Write-Host "[6/6] Resumo da Configuracao" -ForegroundColor Yellow
    Write-Host "==================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Status: " -NoNewline -ForegroundColor White
    Write-Host "AMBIENTE PRONTO!" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Conexoes Disponiveis:" -ForegroundColor White
    Write-Host "  -- Local:      localhost:29092" -ForegroundColor Gray
    Write-Host "  -- Interna:    kafka:9092" -ForegroundColor Gray
    Write-Host "  -- Databricks: $BoreUrl" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Configuracao para Databricks:" -ForegroundColor White
    Write-Host "  kafka.bootstrap.servers = ""$BoreUrl""" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Servicos Rodando:" -ForegroundColor White
    Write-Host "  -- Zookeeper" -ForegroundColor Gray
    Write-Host "  -- Kafka Broker" -ForegroundColor Gray
    Write-Host "  -- Python Producer" -ForegroundColor Gray
    Write-Host "  -- Spark Dev" -ForegroundColor Gray
    Write-Host "  -- Bore Tunnel" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  Spark UI: http://localhost:4040" -ForegroundColor Gray
    Write-Host ""
    Write-Host "==================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Pressione Ctrl+C para parar o ambiente" -ForegroundColor Yellow
    Write-Host ""
}

# Funcao principal
function Main {
    try {
        Stop-OldProcesses
        Install-Bore
        $boreUrl = Start-Bore
        Update-DockerCompose -BoreUrl $boreUrl
        Start-DockerCompose
        Show-Summary -BoreUrl $boreUrl

        # Mantem o script rodando
        Write-Host "Monitorando logs (Ctrl+C para sair)..." -ForegroundColor Gray
        docker-compose logs -f

    } catch {
        Write-Host ""
        Write-Host "ERRO: $_" -ForegroundColor Red
        exit 1
    }
}

# Executa o script
Main
