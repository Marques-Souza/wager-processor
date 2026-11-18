# Wager Processor

Serviço NestJS em Bun para processar operações de apostas com PostgreSQL e SQS.
As garantias financeiras ficam no banco: idempotência persistida, transações
atômicas, lock por wallet, ledger imutável e outbox transacional.

## Requisitos

- Bun 1.x
- Docker Desktop com Docker Compose

## Rodar localmente

No PowerShell, na raiz do repositório:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
bun install
docker compose up -d
bun run db:migration:up
bun run start:dev
```

A API escuta em `http://localhost:3000`. O Compose sobe PostgreSQL e LocalStack;
a aplicação roda no host. O LocalStack provisiona as filas FIFO de entrada,
eventos e dead-letter durante a inicialização. O PostgreSQL usa a porta host
`5433` por padrão para não conflitar com uma instalação local em `5432`.
Se `.env` já existir, revise `DB_PORT` e `POSTGRES_PORT` sem sobrescrever suas
configurações locais.

## Testes

```powershell
bun test
bun run test:integration
```

Os testes de integração conectam ao PostgreSQL e ao LocalStack reais. Não os
execute contra serviços com dados que você não queira alterar: a migration cria
o schema da aplicação e os testes criam registros com identificadores aleatórios.
Eles cobrem concorrência entre processos, atomicidade, idempotência, retry e
redrive para DLQ, reinício do processo HTTP, redelivery após commit, shutdown do
consumidor, ledger e publishers concorrentes.

## Rotas principais

- `POST /wallets` — cria wallet e, se o saldo inicial for positivo, registra
  `OPENING` e `CREDIT` no ledger atomicamente.
- `POST /wagering/transactions` — processa uma transação; exige
  `Idempotency-Key`.
- `GET /wallets/:walletId` e `GET /wallets/:walletId/ledger`
- `GET /wagering/transactions/:transactionId`
- `GET /providers/:providerId/wagering/transactions/:externalTransactionId`
- `POST /wallets/:walletId/reconciliation`
- `GET /health/live` e `GET /health/ready`
- `GET /metrics` — métricas Prometheus básicas por instância

A fila de entrada recebe envelopes `WagerTransactionRequested` com a transação
e `idempotencyKey` dentro de `data`.
Eventos de saída são publicados em `wager-processor-events.fifo`.

Decisões, trade-offs e limitações conhecidas estão em [ARCHITECTURE.md](./ARCHITECTURE.md).
