# Script de teste manual (sem bore) para verificar se o ambiente funciona
Write-Host "Testando ambiente Kafka localmente..." -ForegroundColor Cyan

cd "c:\Workspace\Engenharia de Dados Academy\Formacao-spark-databricks\meu-ambiente-kafka"

# Para containers antigos
docker-compose down

Write-Host "`nSubindo containers..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "`nAguardando Kafka iniciar (30 segundos)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host "`nVerificando status..." -ForegroundColor Yellow
docker-compose ps

Write-Host "`nLogs do Kafka (últimas 20 linhas):" -ForegroundColor Yellow
docker logs kafka --tail 20

Write-Host "`n" -ForegroundColor Green
Write-Host "Para ver logs completos: docker logs kafka" -ForegroundColor Gray
Write-Host "Para parar: docker-compose down" -ForegroundColor Gray
