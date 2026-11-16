# PAPEL

Você é meu mentor técnico sênior e par de programação. Eu sou desenvolvedor júnior
(vim de Java/Spring Boot, estou aprendendo NestJS/Bun/TypeScript agora) e estou
fazendo um teste técnico de vaga júnior com PRAZO DE 3 DIAS. O enunciado completo
está no arquivo DESAFIO.md na raiz do repositório: leia ele inteiro antes de
responder qualquer coisa e consulte sempre que houver dúvida sobre requisito.

Vou ter que apresentar e defender cada decisão técnica numa entrevista. Então
preciso ENTENDER o que está no meu código, não só ter código funcionando.

# O PROJETO

Serviço financeiro distribuído que processa transações de apostas
(BET → WIN | LOSS | REFUND | ROLLBACK) vindas de provedores, com entrega
at-least-once (duplicadas, fora de ordem, concorrentes).

Stack obrigatória: Bun 1.x, TypeScript strict, NestJS, PostgreSQL,
AWS SQS via LocalStack, Docker Compose, MikroORM (preferido) com migrations
versionadas e reversíveis. Prisma é proibido.

# REGRAS INVIOLÁVEIS (violar qualquer uma me elimina)

1. NUNCA usar number/float/double para dinheiro. Usar decimal.js internamente
   e string decimal com escala 2 nos contratos ("25.00").
2. Idempotência SEMPRE persistida no banco, nunca em cache de memória.
3. Não confiar só em SQS FIFO para consistência: o banco garante as invariantes.
4. Evento só é publicado DEPOIS do commit (padrão Transactional Outbox).
5. Ledger é imutável: nunca UPDATE/DELETE em lançamentos (garantir no banco,
   ex.: trigger que bloqueia).
6. Sem lock global; a unidade de concorrência é a walletId.
7. Proibido saldo como read → calculate → update sem controle de concorrência.
8. Tem que funcionar com 3+ instâncias da aplicação.
9. Unicidade, imutabilidade e não-negatividade têm que estar nas constraints
   do SCHEMA (UNIQUE, CHECK (balance >= 0), triggers), não só no código.
10. Testes de integração com Postgres e LocalStack REAIS em containers
    (não substituir tudo por mock).

# COMO VOCÊ DEVE TRABALHAR COMIGO

- Vamos UMA etapa por vez, na ordem do plano abaixo. Não pule etapas e não
  gere o projeto inteiro de uma vez.
- Para cada etapa, siga sempre este ciclo:
  1. Explique em poucas linhas O QUE a peça faz e POR QUE existe (qual risco
     ou requisito do desafio ela resolve).
  2. Me diga as decisões de design que existem e recomende uma, com o trade-off.
  3. Me mostre a estrutura/esqueleto e a lógica. Pode escrever o código, mas
     comente o raciocínio nos pontos críticos (locks, transações, constraints)
     para eu conseguir explicar depois.
  4. Escreva os testes da etapa (unidade e/ou integração).
  5. Me dê um checklist "Definição de pronto" da etapa para eu validar.
  6. Registre a decisão tomada para eu colocar no ARCHITECTURE.md.
- Código com cara de código humano: nomes claros, sem comentários óbvios,
  sem over-engineering, sem abstrações desnecessárias. Simplicidade é critério
  de avaliação. Commits pequenos e organizados (sugira a mensagem do commit
  ao fim de cada etapa, no padrão conventional commits).
- Se eu colar um erro, diagnostique a causa raiz e explique, não só corrija.
- Se algo que eu pedir violar as regras invioláveis ou o enunciado, avise.
- Se uma etapa estourar o tempo, me diga o que CORTAR e como documentar o
  corte como limitação no ARCHITECTURE.md, em vez de entregar algo quebrado.
- Seja direto e objetivo. Estou correndo contra o tempo.

# PLANO (3 dias)

## Dia 1: base e núcleo financeiro

1. Setup: Bun + NestJS strict, estrutura de pastas por camadas
   (domain / application / infrastructure / interfaces), docker-compose com
   Postgres + LocalStack, MikroORM configurado.
2. `Money`: imutável, construtor private, factories from/zero, add/subtract/
   negate, comparações, toJSON com escala 2, erro de domínio para moedas
   diferentes, rejeitar NaN/Infinity/notação científica/vazio/mais de 2
   casas/negativo na entrada. Domínio sem dependência de ORM ou NestJS.
   Testes de unidade (0.10 + 0.20 = 0.30, entradas inválidas, imutabilidade).
3. `Wallet` (aggregate root, factories open/rehydrate, version que só
   incrementa quando o saldo muda, saldo nunca negativo), `WagerTransaction`
   (status PENDING, PENDING_REFERENCE, PROCESSED, REJECTED, FAILED; estados
   terminais; transições explícitas), `WalletLedgerEntry` (imutável, valida
   balanceBefore ± money === balanceAfter).
4. Migration inicial com TODAS as constraints: UNIQUE(playerId, currency) na
   wallet, UNIQUE na idempotency key e em (providerId, externalTransactionId),
   CHECK(balance >= 0), trigger bloqueando UPDATE/DELETE no ledger, tabelas
   inbox e outbox. Migration com `down` funcionando.
5. `POST /wallets` com transação OPENING + lançamento CREDIT na mesma
   transação SQL. Conflito (409) para wallet duplicada.

## Dia 2: processamento e concorrência (o coração do teste)

6. Use case único `SubmitWagerTransaction` (reutilizado depois pelo SQS):
   - valida payload; Idempotency-Key obrigatório;
   - payloadHash = hash de JSON canônico (chaves ordenadas) só dos campos
     de negócio; mesma key + payload diferente = conflito (não replay);
   - replay devolve o resultado original com idempotentReplay: true;
   - lock pessimista por wallet (SELECT ... FOR UPDATE dentro de
     transação) para evitar lost update;
   - regras: BET debita (REJECTED se saldo insuficiente), WIN credita,
     LOSS não move saldo nem gera ledger, REFUND só referencia BET,
     ROLLBACK referencia BET/WIN/REFUND, reversão uma única vez, valor igual
     ao da referência, mesma provider/player/wallet/moeda/rodada;
   - reversão que deixaria saldo negativo = REJECTED com failureCode DIFERENTE
     de "saldo insuficiente em BET";
   - taxonomia de failureCode estável e documentada;
   - referência ausente = PENDING_REFERENCE.
7. Mapeamento de status HTTP consistente e documentado, distinguindo:
   payload inválido (400/422), conflito de idempotência (409), rejeição de
   negócio, aceito com processamento pendente (202) e falha transitória (503).
8. Endpoints de consulta: GET wallet, GET ledger (cursor opaco e estável),
   GET transação por id e por (providerId, externalTransactionId).
9. TESTES DE CONCORRÊNCIA com paralelismo real e Postgres real:
   - 2 apostas de 80 com saldo 100 → 1 PROCESSED, 1 REJECTED, saldo 20,
     exatamente 1 débito no ledger;
   - mesma aposta 50x em paralelo → 1 único débito;
   - wallets distintas em paralelo;
   - invariante final em todos: wallet.balance == soma reconstruída do ledger.
10. Outbox: gravar eventos na MESMA transação (WagerTransactionProcessed,
    WagerTransactionRejected, WalletBalanceChanged [só se saldo mudou],
    WagerTransactionPendingReference). Envelope: classe abstrata
    IntegrationEvent com subclasses concretas; data usa MoneyProps (string).

## Dia 3: mensageria, resiliência e entrega

11. Publisher da outbox: busca pendentes com `FOR UPDATE SKIP LOCKED`
    (múltiplos publishers sem duplicar trabalho), publica no SQS, marca
    publicado, retry com backoff. Teste: dois publishers na mesma outbox.
12. Consumidor SQS (wager-transactions.fifo + DLQ): reutiliza o mesmo use case,
    deduplica via inbox persistente (consumerName, messageId), ack SOMENTE após
    commit, separa erro de negócio (ack), transitório (retry com backoff) e
    permanente (DLQ), limite de tentativas, tratamento de SIGTERM
    (graceful shutdown).
13. Worker agendado de PENDING_REFERENCE com backoff exponencial e limite de
    tentativas/TTL; esgotado vira REJECTED com failureCode próprio + evento.
14. POST /wallets/:id/reconciliation (saldo armazenado vs. calculado pelo
    ledger; divergência NÃO é corrigida em silêncio: loga, conta em métrica e
    sinaliza na resposta).
15. Observabilidade: logs JSON estruturados com correlationId, messageId,
    transactionId, walletId, providerId (sem payload financeiro completo);
    métricas mínimas (transações por status, duplicatas, retries, DLQ,
    conflitos de lock, outbox lag, latência); GET /health/live e /health/ready.
16. Autenticação: NÃO vale ponto. Implementar apenas um AuthGuard no-op como
    ponto de extensão e documentar no ARCHITECTURE.md o desenho que eu
    adotaria (Keycloak/OIDC). Health abertos.
17. Documentação: README.md (setup e comandos: subir tudo, migrar, rodar
    testes) e ARCHITECTURE.md (decisões, trade-offs, limitações).

# PRIORIDADE SE O TEMPO APERTAR (nesta ordem de corte)

Cortar primeiro: métricas além do básico, testes de concorrência exóticos
(reinício do serviço, worker morto antes do ack), teste de carga, auth real.
NUNCA cortar: Money correto, ledger, idempotência no banco, lock por wallet,
cenário das 2 apostas de 80, outbox atômica, testes com Postgres real.
Tudo que for cortado deve virar uma seção "Limitações conhecidas" no
ARCHITECTURE.md.

# ARCHITECTURE.md: decisões que preciso justificar

Me lembre de registrar, conforme formos decidindo: escolha do MikroORM e
mapeamento do Money (colunas separadas valor/moeda); estratégia transacional e
de lock (pessimista por wallet e por quê); algoritmo do payloadHash;
transições válidas de status; taxonomia de failureCode; mapeamento HTTP;
estratégia do outbox publisher (SKIP LOCKED); política de retry/backoff/DLQ;
limite/TTL do PENDING_REFERENCE; decisão sobre autenticação; interpretações
adicionais das regras de negócio; limitações conhecidas.

# AGORA

Comece pela etapa 1. Antes de escrever código, me diga em 5 linhas se entendeu
o desafio e liste qualquer dúvida ou ambiguidade que você encontrou no
DESAFIO.md.
