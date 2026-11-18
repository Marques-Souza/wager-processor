# Arquitetura e decisões

## Persistência e consistência

- PostgreSQL é a fonte de verdade. MikroORM gerencia conexão, transações e
  migrations versionadas; o núcleo usa SQL parametrizado pelo driver do
  MikroORM para explicitar `FOR UPDATE`, `SKIP LOCKED` e constraints.
  Trade-off: menos mapeamento ORM, mas SQL e schema ficam explícitos e auditáveis.
- `Money` usa Decimal.js no domínio e `{ amount: string, currency: string }`
  nos contratos. A persistência usa `numeric(20,2)` e `char(3)`, lidos como
  strings; nunca converte valores monetários para `number`.
- A wallet é bloqueada com `SELECT ... FOR UPDATE` dentro da transação que
  grava saldo, status da transação, ledger e outbox. O lock é por `walletId`,
  permitindo concorrência entre wallets e consistência entre instâncias.
- Um advisory lock transacional, derivado da idempotency key, serializa apenas
  retries simultâneos da mesma operação antes da inserção. Não substitui a
  unicidade persistida nem o lock financeiro por wallet; evita deadlocks ao
  disputar simultaneamente os índices de idempotency key e transação externa.
- O schema impõe unicidade de `(player_id, currency)`, idempotency key e
  `(provider_id, external_transaction_id)`, saldo não negativo, aritmética dos
  lançamentos, referências e imutabilidade do ledger via trigger.
- O `payloadHash` é SHA-256 de JSON canônico dos campos de negócio. Chaves de
  objetos são ordenadas recursivamente; valor monetário é normalizado para
  duas casas. A idempotency key não entra no hash. Reutilizar a chave com
  conteúdo diferente é conflito (`409`).
- O resultado persistido inclui status e saldo observado. Um replay devolve
  esse resultado original, não o saldo atual da wallet.

## Transações e eventos

- A gravação financeira e as mensagens de outbox são atômicas. Um publisher
  independente seleciona eventos com `FOR UPDATE SKIP LOCKED`, publica na fila
  FIFO de saída e marca `published_at`. Falha ao publicar agenda retry
  exponencial (1, 2, 4... até 60 segundos). Uma falha entre envio e commit pode
  causar reenvio: a entrega é at-least-once e consumidores devem deduplicar por
  `eventId`.
- O consumidor da fila de entrada usa `inbox_messages` na mesma transação SQL
  do processamento. Só apaga a mensagem do SQS depois do commit. Erro
  transitório deixa a mensagem para retry com backoff; erro permanente não
  confirmado é redirecionado à DLQ pelo limite de cinco entregas.
- A fila de entrada é `wager-transactions.fifo`; saída e DLQ têm filas
  separadas para evitar que eventos publicados sejam consumidos como novos
  comandos.

## Regras de negócio adotadas

- `BET` debita; saldo insuficiente produz `REJECTED/INSUFFICIENT_FUNDS`.
  `WIN` credita, podendo referenciar uma BET da mesma rodada. `LOSS` é
  registrado sem lançamento. `REFUND` reverte uma BET; `ROLLBACK` reverte BET,
  WIN ou REFUND. Referências devem compartilhar provider, player, wallet,
  moeda e rodada. Reversões são de valor integral e só uma por tipo/referência.
- Reversão que causaria saldo negativo é `REJECTED/REVERSAL_WOULD_OVERDRAW`.
  Outros códigos estáveis incluem `REFERENCE_NOT_FOUND`, `REFERENCE_MISMATCH`
  `REFERENCE_NOT_PROCESSED` e `REFERENCE_ALREADY_REVERSED`.
- Ausência de referência persiste `PENDING_REFERENCE`. Worker reconsulta com
  backoff exponencial até 60 segundos; após 24 horas rejeita com
  `REFERENCE_NOT_FOUND` (referência ausente) ou `REFERENCE_NOT_PROCESSED`
  (referência presente, mas não processada) e gera evento.
- `PROCESSED`, `REJECTED` e `FAILED` são estados terminais. `OPENING` é interno
  e não aceito pela API nem pela fila.

## HTTP e autenticação

- Request inválido: `422`; wallet/transação inexistente: `404`.
- Conflito de idempotência ou unicidade: `409`.
- Resultado processado: `200`; referência pendente: `202`; rejeição de negócio:
  `422`; indisponibilidade transitória: `503`.
- `AuthGuard` é intencionalmente no-op, aplicado às rotas de negócio. Uma
  implementação real deve validar tokens OIDC emitidos por um IdP como
  Keycloak; health permanece aberto.

## Reconciliação e observabilidade

`POST /wallets/:walletId/reconciliation` compara o saldo materializado com a
soma do ledger. Divergência gera log estruturado e aparece na resposta; o
serviço não corrige o saldo automaticamente. `/health/live` verifica o processo
e `/health/ready` consulta PostgreSQL e SQS. `/metrics` expõe contadores por
status, duplicatas, retries, DLQ, conflitos de lock, latência e lag da outbox
no formato Prometheus; os valores são mantidos por processo e reiniciam com ele.

## Limitações conhecidas

- Os logs HTTP de submissão, consumidor SQS, outbox e reconciliação são JSON e
  carregam correlationId e identificadores disponíveis. Rotas de consulta e
  alguns erros gerais de workers ainda não têm logs operacionais completos.
- O guard de autenticação não valida identidade; autenticação real foi deixada
  como extensão documentada.
- Os testes de integração estão implementados para PostgreSQL e LocalStack,
  incluindo concorrência financeira entre processos, wallets distintas,
  atomicidade financeira/inbox/outbox, idempotência, regras BET/WIN/LOSS/
  REFUND/ROLLBACK, ledger, referência fora de ordem, consumidor SQS e publisher
  concorrente. Os 20 testes da suíte completa passaram. A cobertura inclui
  constraints verificadas diretamente no PostgreSQL, migration aplicada e
  revertida em database isolado, reinício real do processo HTTP, redelivery de
  mensagem SQS deixada sem ack após commit, retry transitório, redrive após
  limite de tentativas, retry da outbox e graceful shutdown durante long poll
  e processamento em lote. O ponto commit/ack é exercitado deixando a mensagem
  invisível e redeliverable, sem matar o processo exatamente entre as duas
  instruções.
- O publisher tem cobertura de retry e concorrência com `SKIP LOCKED`, mas não
  há teste automatizado que encerre o processo após o envio ao SQS e antes do
  commit da outbox. Nesse intervalo pode haver publicação duplicada; os
  consumidores devem continuar idempotentes.
- O teste de shutdown prova a conclusão da mensagem em processamento e o
  retorno imediato à fila das demais mensagens já recebidas no lote; não
  simula falha do host/contêiner durante o shutdown.
- O PostgreSQL do Compose usa a porta host `5433` por padrão para evitar conflito
  com instalações locais que já ocupem `5432`.
- Não há teste de carga. A janela de falha entre envio SQS de saída e commit
  do publisher é tratada como entrega duplicada, conforme o modelo at-least-once;
  os publishers concorrentes e reprocessamento da outbox são cobertos, mas não
  há teste que mate o processo exatamente nessa janela.
