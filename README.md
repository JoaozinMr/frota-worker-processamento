# Frota – Worker de Processamento

Serviço em background (.NET 8) que consome o tópico `frota.telemetria` do Redpanda/Kafka, aplica regras de negócio espaciais e persiste o estado (Hot/Cold path). Parte do sistema distribuído de monitoramento de frota (ver [Contexto_Geral_Projeto_Frota.md](Contexto_Geral_Projeto_Frota.md) e [Plano_LLM_Worker_Processamento.md](Plano_LLM_Worker_Processamento.md)).

## Funcionalidades

| Requisito | Implementação |
|---|---|
| Consumer Group + commit manual | [TelemetryConsumerService](src/FrotaWorker/Services/TelemetryConsumerService.cs): `EnableAutoCommit=false`; offsets confirmados só após processar **e** fazer flush do batch no PostgreSQL |
| Velocidade / Geofence | [RuleEvaluator](src/FrotaWorker/Services/RuleEvaluator.cs) (limite km/h + Haversine) |
| Sensor morto (bônus) | [DeadSensorService](src/FrotaWorker/Services/DeadSensorService.cs): job periódico que varre `veiculo:*:posicao` no Redis |
| Hot path | [RedisStateStore](src/FrotaWorker/Services/RedisStateStore.cs): `veiculo:{id}:posicao` (hash, só sobrescreve se o evento for mais novo) |
| Cold path | [PostgresBatchWriter](src/FrotaWorker/Services/PostgresBatchWriter.cs): batch de 500 registros ou 2s; tabela `telemetria` (hypertable se TimescaleDB existir) |
| Alertas síncronos + Polly | [Alertas.cs](src/FrotaWorker/Services/Alertas.cs) + [Program.cs](src/FrotaWorker/Program.cs): timeout 3s, retry exponencial 1s/2s/4s |
| Idempotência | Dedup Redis `alerta:{id}:{tipo}` (TTL 5 min, gravada após POST OK); `ON CONFLICT DO NOTHING` no Postgres; Redis ignora eventos antigos |
| DLQ | Payload inválido → `frota.telemetria.dlq` imediatamente; erros não-transitórios após `MaxProcessingAttempts`. Falhas de dependência (API/Redis/DB fora) são reprocessadas sem confirmar o offset |
| Observabilidade | Correlation ID lido dos headers Kafka (`X-Correlation-ID`) → Serilog JSON; `GET /health`; [Dockerfile](Dockerfile) |

## Contrato da mensagem (JSON)

```json
{ "id_veiculo": "V-001", "latitude": -23.55, "longitude": -46.63,
  "velocidade": 72.5, "status_motor": "LIGADO", "timestamp": "2026-10-10T12:00:00Z" }
```
A chave Kafka deve ser o `id_veiculo`. Alerta enviado (`POST {Alertas:BaseUrl}{Alertas:Path}`):
`id_veiculo, tipo_infracao (EXCESSO_VELOCIDADE | FORA_GEOFENCE | SENSOR_MORTO), descricao, latitude, longitude, velocidade, timestamp, correlation_id`. Padrão: `POST http://localhost:5100/api/alerts`. Contrato completo em [Handoff_Worker_para_API_Alertas.md](Handoff_Worker_para_API_Alertas.md).

## Configuração

Em `appsettings.json` ou variáveis de ambiente (`Kafka__BootstrapServers`, `Redis__ConnectionString`, `Postgres__ConnectionString`, `Alertas__BaseUrl`, `Rules__SpeedLimitKmh`, ...).

## Executar

```bash
dotnet test
dotnet run --project src/FrotaWorker
docker build -t frota-worker .
```