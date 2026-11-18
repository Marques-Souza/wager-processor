import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { MikroORM } from '@mikro-orm/core';
import { Pool } from 'pg';
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import mikroOrmConfig from '../../src/infrastructure/persistence/mikro-orm.config';
import { WageringService } from '../../src/application/wagering/wagering.service';
import { MetricsService } from '../../src/application/observability/metrics.service';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/entities/wager-transaction';
import { OutboxPublisherService } from '../../src/infrastructure/messaging/outbox-publisher.service';
import { WagerTransactionConsumerService } from '../../src/infrastructure/messaging/wager-transaction-consumer.service';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs-client';

interface Row {
  balance: string;
  ledger_count: string;
  debit_count: string;
  ledger_balance: string;
}

interface WorkerResult {
  body: {
    status: WagerTransactionStatus;
  };
}

interface ApiProcess {
  child: ChildProcess;
  port: number;
}

interface UnackedWorkerResult {
  result: WorkerResult;
  receiptHandle: string;
}

function runUnackedSqsWorker(queueUrl: string): Promise<UnackedWorkerResult> {
  const workerPath = fileURLToPath(
    new URL('../helpers/commit-sqs-without-ack.worker.ts', import.meta.url),
  );
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['run', workerPath, queueUrl], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout?.setEncoding('utf8');
    child.stderr?.setEncoding('utf8');
    child.stdout?.on('data', (chunk: string) => {
      stdout += chunk;
    });
    child.stderr?.on('data', (chunk: string) => {
      stderr += chunk;
    });
    child.once('error', reject);
    child.once('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Unacked SQS worker exited ${code}: ${stderr}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout.trim()) as UnackedWorkerResult);
      } catch {
        reject(new Error(`Unacked SQS worker returned invalid JSON: ${stdout}`));
      }
    });
  });
}

class PausingWageringService extends WageringService {
  private releaseGate!: () => void;
  startedExternalTransactionId: string | undefined;
  private readonly processingGate = new Promise<void>((resolve) => {
    this.releaseGate = resolve;
  });

  releaseProcessing(): void {
    this.releaseGate();
  }

  override async submitWager(
    ...args: Parameters<WageringService['submitWager']>
  ): ReturnType<WageringService['submitWager']> {
    this.startedExternalTransactionId = args[0].externalTransactionId;
    await this.processingGate;
    return super.submitWager(...args);
  }
}

async function startApiProcess(): Promise<ApiProcess> {
  const workerPath = fileURLToPath(
    new URL('../helpers/http-server.worker.ts', import.meta.url),
  );
  const child = spawn(process.execPath, ['run', workerPath], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill();
      reject(new Error(`HTTP server startup timed out: ${stderr}`));
    }, 20000);
    child.stdout?.setEncoding('utf8');
    child.stderr?.setEncoding('utf8');
    child.stdout?.on('data', (chunk: string) => {
      stdout += chunk;
      const line = stdout.split(/\r?\n/).find((value) => value.length > 0);
      if (line === undefined) {
        return;
      }
      try {
        const ready = JSON.parse(line) as { port: number };
        clearTimeout(timeout);
        resolve({ child, port: ready.port });
      } catch {
        reject(new Error(`HTTP server returned invalid startup output: ${line}`));
      }
    });
    child.stderr?.on('data', (chunk: string) => {
      stderr += chunk;
    });
    child.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once('close', (code) => {
      clearTimeout(timeout);
      reject(
        new Error(
          `HTTP server exited before startup (${code}): ${stderr || stdout}`,
        ),
      );
    });
  });
}

async function stopApiProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.killed) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('HTTP server did not stop after termination signal'));
    }, 10000);
    child.once('close', () => {
      clearTimeout(timeout);
      resolve();
    });
    child.kill('SIGKILL');
  });
}

function runSubmitInSeparateProcess(
  command: {
    providerId: string;
    externalTransactionId: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: WagerTransactionKind;
    money: { amount: string; currency: string };
  },
  idempotencyKey: string,
): Promise<WorkerResult> {
  const workerPath = fileURLToPath(
    new URL('../helpers/submit-wager.worker.ts', import.meta.url),
  );
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ['run', workerPath, JSON.stringify({ command, idempotencyKey })],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    let stdout = '';
    let stderr = '';
    const timeout = setTimeout(() => {
      child.kill();
      reject(new Error('Concurrent submit worker timed out'));
    }, 30000);

    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk: string) => {
      stderr += chunk;
    });
    child.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once('close', (code) => {
      clearTimeout(timeout);
      if (code !== 0) {
        reject(new Error(`Concurrent submit worker exited ${code}: ${stderr}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout) as WorkerResult);
      } catch {
        reject(new Error(`Concurrent submit worker returned invalid JSON: ${stdout}`));
      }
    });
  });
}

async function waitFor(
  predicate: () => Promise<boolean>,
  description: string,
): Promise<void> {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

describe('WageringService PostgreSQL integration', () => {
  let orm: MikroORM | undefined;
  let service!: WageringService;
  const sqsClient = createSqsClient();
  let eventQueueUrl: string | undefined;

  const getOrm = (): MikroORM => {
    if (orm === undefined) {
      throw new Error('PostgreSQL connection has not been initialized');
    }
    return orm;
  };

  const createConsumerQueuePair = async (): Promise<{
    inputQueueUrl: string;
    deadLetterQueueUrl: string;
  }> => {
    const deadLetterQueue = await sqsClient.send(
      new CreateQueueCommand({
        QueueName: `consumer-dlq-${randomUUID()}.fifo`,
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
        },
      }),
    );
    if (deadLetterQueue.QueueUrl === undefined) {
      throw new Error('LocalStack did not return a DLQ URL');
    }
    const deadLetterAttributes = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: deadLetterQueue.QueueUrl,
        AttributeNames: ['QueueArn'],
      }),
    );
    const deadLetterArn = deadLetterAttributes.Attributes?.QueueArn;
    if (deadLetterArn === undefined) {
      throw new Error('LocalStack did not return a DLQ ARN');
    }
    const inputQueue = await sqsClient.send(
      new CreateQueueCommand({
        QueueName: `consumer-input-${randomUUID()}.fifo`,
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: deadLetterArn,
            maxReceiveCount: '2',
          }),
        },
      }),
    );
    if (inputQueue.QueueUrl === undefined) {
      throw new Error('LocalStack did not return an input queue URL');
    }
    return {
      inputQueueUrl: inputQueue.QueueUrl,
      deadLetterQueueUrl: deadLetterQueue.QueueUrl,
    };
  };

  const deleteConsumerQueuePair = async (queues: {
    inputQueueUrl: string;
    deadLetterQueueUrl: string;
  }): Promise<void> => {
    await sqsClient.send(
      new DeleteQueueCommand({ QueueUrl: queues.inputQueueUrl }),
    );
    await sqsClient.send(
      new DeleteQueueCommand({ QueueUrl: queues.deadLetterQueueUrl }),
    );
  };

  beforeAll(async () => {
    const connectedOrm = await MikroORM.init(mikroOrmConfig);
    orm = connectedOrm;
    await connectedOrm.getMigrator().up();
    service = new WageringService(connectedOrm.em);
    const queue = await sqsClient.send(
      new CreateQueueCommand({
        QueueName: `outbox-test-${randomUUID()}.fifo`,
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
        },
      }),
    );
    eventQueueUrl = queue.QueueUrl;
  });

  afterAll(async () => {
    if (eventQueueUrl !== undefined) {
      await sqsClient.send(new DeleteQueueCommand({ QueueUrl: eventQueueUrl }));
    }
    sqsClient.destroy();
    if (orm) {
      await orm.close(true);
    }
  });

  it('serializes competing bets and keeps the materialized balance equal to the ledger', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const baseCommand = {
      providerId: 'integration-provider',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `integration-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '80.00', currency: 'BRL' },
    } as const;

    const outcomes = await Promise.all(
      [1, 2].map((attempt) =>
        service.submitWager(
          {
            ...baseCommand,
            externalTransactionId: `competing-bet-${randomUUID()}-${attempt}`,
          },
          `competing-idempotency-${randomUUID()}-${attempt}`,
        ),
      ),
    );

    expect(
      outcomes.filter(
        (outcome) =>
          outcome.body.status === WagerTransactionStatus.Processed,
      ),
    ).toHaveLength(1);
    expect(
      outcomes.filter(
        (outcome) =>
          outcome.body.status === WagerTransactionStatus.Rejected,
      ),
    ).toHaveLength(1);

    const rows = (await getOrm()
      .em.getConnection()
      .execute(
        `select w."balance"::text as balance,
                count(l."id")::text as ledger_count,
                count(l."id") filter (where l."direction" = 'DEBIT')::text
                  as debit_count,
                coalesce(sum(case when l."direction" = 'CREDIT'
                                  then l."amount" else -l."amount" end), 0)::text
                  as ledger_balance
         from "wallets" w
         left join "wallet_ledger_entries" l on l."wallet_id" = w."id"
         where w."id" = ?
         group by w."id"`,
        [wallet.id],
        'all',
      )) as Row[];

    expect(rows).toHaveLength(1);
    expect(rows[0]?.balance).toBe('20.00');
    expect(rows[0]?.ledger_count).toBe('2');
    expect(rows[0]?.debit_count).toBe('1');
    expect(rows[0]?.ledger_balance).toBe(rows[0]?.balance);
  });

  it('processes different wallets concurrently without cross-wallet blocking', async () => {
    const wallets = await Promise.all(
      [1, 2].map(() =>
        service.createWallet({
          playerId: `parallel-player-${randomUUID()}`,
          initialBalance: { amount: '100.00', currency: 'BRL' },
        }),
      ),
    );

    const outcomes = await Promise.all(
      wallets.map((wallet, index) =>
        service.submitWager(
          {
            providerId: 'integration-provider',
            externalTransactionId: `parallel-wallet-${randomUUID()}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `parallel-round-${index}`,
            gameId: 'integration-game',
            kind: WagerTransactionKind.Bet,
            money: { amount: '80.00', currency: 'BRL' },
          },
          `parallel-wallet-idempotency-${randomUUID()}`,
        ),
      ),
    );

    expect(
      outcomes.every(
        (outcome) => outcome.body.status === WagerTransactionStatus.Processed,
      ),
    ).toBe(true);
    for (const wallet of wallets) {
      expect((await service.getWallet(wallet.id)).balance).toEqual({
        amount: '20.00',
        currency: 'BRL',
      });
      expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);
    }
  });

  it('preserves the wallet invariant across three independent application processes', async () => {
    const wallet = await service.createWallet({
      playerId: `multi-process-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const attempts = [1, 2, 3].map((attempt) =>
      runSubmitInSeparateProcess(
        {
          providerId: 'integration-provider',
          externalTransactionId: `multi-process-bet-${randomUUID()}-${attempt}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `multi-process-round-${randomUUID()}`,
          gameId: 'integration-game',
          kind: WagerTransactionKind.Bet,
          money: { amount: '80.00', currency: 'BRL' },
        },
        `multi-process-key-${randomUUID()}-${attempt}`,
      ),
    );

    const results = await Promise.all(attempts);
    expect(
      results.filter(
        (result) => result.body.status === WagerTransactionStatus.Processed,
      ),
    ).toHaveLength(1);
    expect(
      results.filter(
        (result) => result.body.status === WagerTransactionStatus.Rejected,
      ),
    ).toHaveLength(2);
    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '20.00',
      currency: 'BRL',
    });
    expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);
  }, 20000);

  it('rolls back wallet, ledger, inbox, and outbox when event persistence fails', async () => {
    const wallet = await service.createWallet({
      playerId: `atomicity-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const idempotencyKey = `atomicity-key-${randomUUID()}`;
    const outboxBefore = (await getOrm().em.getConnection().execute(
      `select count(*)::text as count
       from "outbox_messages" where "aggregate_id" = ?`,
      [wallet.id],
      'get',
    )) as { count: string };

    await getOrm().em.getConnection().execute(
      `create or replace function "fail_outbox_insert_integration_test"()
       returns trigger language plpgsql as $$
       begin
         raise exception 'forced outbox failure';
       end;
       $$`,
    );
    await getOrm().em.getConnection().execute(
      `drop trigger if exists "fail_outbox_insert_integration_test"
       on "outbox_messages"`,
    );
    await getOrm().em.getConnection().execute(
      `create trigger "fail_outbox_insert_integration_test"
       before insert on "outbox_messages"
       for each row execute function "fail_outbox_insert_integration_test"()`,
    );

    try {
      await expect(
        service.submitWager(
          {
            providerId: 'integration-provider',
            externalTransactionId: `atomicity-bet-${randomUUID()}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `atomicity-round-${randomUUID()}`,
            gameId: 'integration-game',
            kind: WagerTransactionKind.Bet,
            money: { amount: '20.00', currency: 'BRL' },
          },
          idempotencyKey,
          {
            consumerName: 'atomicity-test',
            messageId: randomUUID(),
            payloadHash: 'b'.repeat(64),
          },
        ),
      ).rejects.toThrow();
    } finally {
      await getOrm().em.getConnection().execute(
        `drop trigger if exists "fail_outbox_insert_integration_test"
         on "outbox_messages"`,
      );
      await getOrm().em.getConnection().execute(
        `drop function if exists "fail_outbox_insert_integration_test"()`,
      );
    }

    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '100.00',
      currency: 'BRL',
    });
    const rolledBackRows = (await getOrm().em.getConnection().execute(
      `select
         (select count(*) from "wager_transactions"
          where "idempotency_key" = ?)::text as transaction_count,
         (select count(*) from "inbox_messages"
          where "consumer_name" = 'atomicity-test')::text as inbox_count,
         (select count(*) from "wallet_ledger_entries"
          where "wallet_id" = ?)::text as ledger_count,
         (select count(*) from "outbox_messages"
          where "aggregate_id" = ?)::text as outbox_count`,
      [idempotencyKey, wallet.id, wallet.id],
      'get',
    )) as {
      transaction_count: string;
      inbox_count: string;
      ledger_count: string;
      outbox_count: string;
    };
    expect(rolledBackRows.transaction_count).toBe('0');
    expect(rolledBackRows.inbox_count).toBe('0');
    expect(rolledBackRows.ledger_count).toBe('1');
    expect(rolledBackRows.outbox_count).toBe(outboxBefore.count);
  });

  it('deduplicates fifty concurrent submissions by persisted idempotency key', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const externalTransactionId = `duplicate-bet-${randomUUID()}`;
    const idempotencyKey = `duplicate-idempotency-${randomUUID()}`;
    const command = {
      providerId: 'integration-provider',
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `integration-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '50.00', currency: 'BRL' },
    } as const;

    const responses = await Promise.all(
      Array.from({ length: 50 }, () =>
        service.submitWager(command, idempotencyKey),
      ),
    );

    expect(
      responses.every(
        (response) => response.body.transactionId === responses[0]?.body.transactionId,
      ),
    ).toBe(true);
    expect(
      responses.filter((response) => !response.body.idempotentReplay),
    ).toHaveLength(1);

    const rows = (await getOrm()
      .em.getConnection()
      .execute(
        `select w."balance"::text as balance,
                count(l."id")::text as ledger_count,
                count(l."id") filter (where l."direction" = 'DEBIT')::text
                  as debit_count,
                coalesce(sum(case when l."direction" = 'CREDIT'
                                  then l."amount" else -l."amount" end), 0)::text
                  as ledger_balance
         from "wallets" w
         left join "wallet_ledger_entries" l on l."wallet_id" = w."id"
         where w."id" = ?
         group by w."id"`,
        [wallet.id],
        'all',
      )) as Row[];

    expect(rows[0]?.balance).toBe('50.00');
    expect(rows[0]?.ledger_count).toBe('2');
    expect(rows[0]?.debit_count).toBe('1');
    expect(rows[0]?.ledger_balance).toBe(rows[0]?.balance);
  }, 15000);

  it('applies WIN, LOSS, REFUND, and ROLLBACK with one-time reversals', async () => {
    const wallet = await service.createWallet({
      playerId: `wager-kinds-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const roundId = `wager-kinds-round-${randomUUID()}`;
    const submit = async (
      kind: Exclude<WagerTransactionKind, WagerTransactionKind.Opening>,
      amount: string,
      referenceExternalTransactionId?: string,
    ) => {
      const externalTransactionId = `wager-kind-${randomUUID()}`;
      const result = await service.submitWager(
        {
          providerId: 'integration-provider',
          externalTransactionId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId,
          gameId: 'integration-game',
          kind,
          money: { amount, currency: 'BRL' },
          ...(referenceExternalTransactionId === undefined
            ? {}
            : { referenceExternalTransactionId }),
        },
        `wager-kind-key-${randomUUID()}`,
      );
      return { result, externalTransactionId };
    };

    const bet = await submit(WagerTransactionKind.Bet, '20.00');
    const win = await submit(
      WagerTransactionKind.Win,
      '5.00',
      bet.externalTransactionId,
    );
    const loss = await submit(WagerTransactionKind.Loss, '30.00');
    const refund = await submit(
      WagerTransactionKind.Refund,
      '20.00',
      bet.externalTransactionId,
    );
    const duplicateRefund = await submit(
      WagerTransactionKind.Refund,
      '20.00',
      bet.externalTransactionId,
    );
    const rollbackWin = await submit(
      WagerTransactionKind.Rollback,
      '5.00',
      win.externalTransactionId,
    );
    const duplicateRollback = await submit(
      WagerTransactionKind.Rollback,
      '5.00',
      win.externalTransactionId,
    );

    expect(bet.result.body.status).toBe(WagerTransactionStatus.Processed);
    expect(win.result.body.status).toBe(WagerTransactionStatus.Processed);
    expect(loss.result.body.status).toBe(WagerTransactionStatus.Processed);
    expect(refund.result.body.status).toBe(WagerTransactionStatus.Processed);
    expect(duplicateRefund.result.body.status).toBe(
      WagerTransactionStatus.Rejected,
    );
    expect(rollbackWin.result.body.status).toBe(
      WagerTransactionStatus.Processed,
    );
    expect(duplicateRollback.result.body.status).toBe(
      WagerTransactionStatus.Rejected,
    );
    expect(
      (await service.getTransaction(duplicateRefund.result.body.transactionId))
        .failureCode,
    ).toBe('REFERENCE_ALREADY_REVERSED');
    expect(
      (await service.getTransaction(duplicateRollback.result.body.transactionId))
        .failureCode,
    ).toBe('REFERENCE_ALREADY_REVERSED');
    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '100.00',
      currency: 'BRL',
    });
    const ledgerCount = (await getOrm().em.getConnection().execute(
      `select count(*)::text as count
       from "wallet_ledger_entries" where "wallet_id" = ?`,
      [wallet.id],
      'get',
    )) as { count: string };
    expect(ledgerCount.count).toBe('5');
    expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);
  });

  it('rejects a rollback that would overdraw with a distinct failure code', async () => {
    const wallet = await service.createWallet({
      playerId: `rollback-overdraw-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const roundId = `rollback-overdraw-round-${randomUUID()}`;
    const winExternalId = `rollback-overdraw-win-${randomUUID()}`;
    const win = await service.submitWager(
      {
        providerId: 'integration-provider',
        externalTransactionId: winExternalId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: 'integration-game',
        kind: WagerTransactionKind.Win,
        money: { amount: '50.00', currency: 'BRL' },
      },
      `rollback-overdraw-win-key-${randomUUID()}`,
    );
    await service.submitWager(
      {
        providerId: 'integration-provider',
        externalTransactionId: `rollback-overdraw-bet-${randomUUID()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: 'integration-game',
        kind: WagerTransactionKind.Bet,
        money: { amount: '120.00', currency: 'BRL' },
      },
      `rollback-overdraw-bet-key-${randomUUID()}`,
    );
    const rollback = await service.submitWager(
      {
        providerId: 'integration-provider',
        externalTransactionId: `rollback-overdraw-reversal-${randomUUID()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: 'integration-game',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: winExternalId,
      },
      `rollback-overdraw-key-${randomUUID()}`,
    );

    expect(win.body.status).toBe(WagerTransactionStatus.Processed);
    expect(rollback.body.status).toBe(WagerTransactionStatus.Rejected);
    expect(
      (await service.getTransaction(rollback.body.transactionId)).failureCode,
    ).toBe('REVERSAL_WOULD_OVERDRAW');
    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '30.00',
      currency: 'BRL',
    });
    expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);
  });

  it('rejects reusing an idempotency key with a different business payload', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const idempotencyKey = `conflicting-idempotency-${randomUUID()}`;
    const command = {
      providerId: 'integration-provider',
      externalTransactionId: `conflicting-bet-${randomUUID()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `integration-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '50.00', currency: 'BRL' },
    } as const;
    await service.submitWager(command, idempotencyKey);

    await expect(
      service.submitWager(
        { ...command, money: { amount: '49.00', currency: 'BRL' } },
        idempotencyKey,
      ),
    ).rejects.toThrow();
    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '50.00',
      currency: 'BRL',
    });
  });

  it('deduplicates SQS redelivery through the persistent inbox', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const command = {
      providerId: 'integration-provider',
      externalTransactionId: `inbox-bet-${randomUUID()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `integration-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '50.00', currency: 'BRL' },
    } as const;
    const idempotencyKey = `inbox-idempotency-${randomUUID()}`;
    const inbox = {
      consumerName: 'integration-consumer',
      messageId: randomUUID(),
      payloadHash: 'a'.repeat(64),
    };

    const first = await service.submitWager(command, idempotencyKey, inbox);
    const replay = await service.submitWager(command, idempotencyKey, inbox);
    expect(first.body.status).toBe(WagerTransactionStatus.Processed);
    expect(replay.body.idempotentReplay).toBe(true);
    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '50.00',
      currency: 'BRL',
    });
  });

  it('preserves financial consistency after application restart', async () => {
    const playerId = `restart-player-${randomUUID()}`;
    const idempotencyKey = `restart-key-${randomUUID()}`;
    const command = {
      providerId: 'integration-provider',
      externalTransactionId: `restart-bet-${randomUUID()}`,
      playerId,
      walletId: '',
      roundId: `restart-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '50.00', currency: 'BRL' },
    };
    let api = await startApiProcess();

    try {
      const baseUrl = `http://127.0.0.1:${api.port}`;
      const walletResponse = await fetch(`${baseUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          playerId,
          initialBalance: { amount: '100.00', currency: 'BRL' },
        }),
      });
      expect(walletResponse.status).toBe(201);
      const wallet = (await walletResponse.json()) as { id: string };
      command.walletId = wallet.id;

      const firstResponse = await fetch(`${baseUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': idempotencyKey,
        },
        body: JSON.stringify(command),
      });
      const firstResult = (await firstResponse.json()) as {
        status: WagerTransactionStatus;
        idempotentReplay: boolean;
      };
      expect(firstResponse.status).toBe(200);
      expect(firstResult.status).toBe(WagerTransactionStatus.Processed);
      expect(firstResult.idempotentReplay).toBe(false);

      await stopApiProcess(api.child);
      api = await startApiProcess();

      const restartedUrl = `http://127.0.0.1:${api.port}`;
      const replayResponse = await fetch(
        `${restartedUrl}/wagering/transactions`,
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'idempotency-key': idempotencyKey,
          },
          body: JSON.stringify(command),
        },
      );
      const replay = (await replayResponse.json()) as {
        idempotentReplay: boolean;
      };
      expect(replayResponse.status).toBe(200);
      expect(replay.idempotentReplay).toBe(true);

      expect((await service.getWallet(wallet.id)).balance).toEqual({
        amount: '50.00',
        currency: 'BRL',
      });
      expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);
    } finally {
      await stopApiProcess(api.child);
    }
  });

  it('deduplicates a real SQS redelivery after commit before ack', async () => {
    const queues = await createConsumerQueuePair();
    const { inputQueueUrl } = queues;

    const wallet = await service.createWallet({
      playerId: `crash-before-ack-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const idempotencyKey = `crash-before-ack-key-${randomUUID()}`;
    const messageId = randomUUID();
    const command = {
      providerId: 'integration-provider',
      externalTransactionId: `crash-before-ack-bet-${randomUUID()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `crash-before-ack-round-${randomUUID()}`,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Bet,
      money: { amount: '40.00', currency: 'BRL' },
    } as const;

    const body = JSON.stringify({
      messageId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        idempotencyKey,
        ...command,
      },
    });
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: inputQueueUrl,
        MessageBody: body,
        MessageGroupId: 'crash-before-ack',
        MessageDeduplicationId: randomUUID(),
      }),
    );
    const crashedWorker = await runUnackedSqsWorker(inputQueueUrl);
    const committedBeforeAck = crashedWorker.result;
    expect(committedBeforeAck.body.status).toBe(
      WagerTransactionStatus.Processed,
    );

    // The child worker exited after commit without deleting the SQS message.
    await sqsClient.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: inputQueueUrl,
        ReceiptHandle: crashedWorker.receiptHandle,
        VisibilityTimeout: 0,
      }),
    );

    const previousInputUrl = process.env.SQS_INPUT_QUEUE_URL;
    process.env.SQS_INPUT_QUEUE_URL = inputQueueUrl;
    const consumer = new WagerTransactionConsumerService(
      service,
      new MetricsService(),
    );
    consumer.onModuleInit();
    try {
      await waitFor(
        async () => {
          const currentWallet = await service.getWallet(wallet.id);
          const queueAttributes = await sqsClient.send(
            new GetQueueAttributesCommand({
              QueueUrl: inputQueueUrl,
              AttributeNames: [
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
              ],
            }),
          );
          return (
            currentWallet.balance.amount === '60.00' &&
            Number(
              queueAttributes.Attributes?.ApproximateNumberOfMessages ?? '0',
            ) === 0 &&
            Number(
              queueAttributes.Attributes
                ?.ApproximateNumberOfMessagesNotVisible ?? '0',
            ) === 0
          );
        },
        'the redelivered message to be deduplicated and acknowledged',
      );
    } finally {
      await consumer.onModuleDestroy();
      await deleteConsumerQueuePair(queues);
      if (previousInputUrl === undefined) {
        delete process.env.SQS_INPUT_QUEUE_URL;
      } else {
        process.env.SQS_INPUT_QUEUE_URL = previousInputUrl;
      }
    }

    expect((await service.getWallet(wallet.id)).balance).toEqual({
      amount: '60.00',
      currency: 'BRL',
    });
    expect((await service.reconcileWallet(wallet.id)).consistent).toBe(true);

    const ledgerRows = (await getOrm()
      .em.getConnection()
      .execute(
        `select count(*)::text as count
         from "wallet_ledger_entries"
         where "wallet_id" = ? and "transaction_id" = ?`,
        [wallet.id, committedBeforeAck.body.transactionId],
        'all',
      )) as Array<{ count: string }>;
    expect(ledgerRows[0]?.count).toBe('1');
  });

  it('commits before acking, acks permanent business errors, and dead-letters malformed messages', async () => {
    const queues = await createConsumerQueuePair();
    const { inputQueueUrl, deadLetterQueueUrl } = queues;

    const previousInputUrl = process.env.SQS_INPUT_QUEUE_URL;
    const previousDlqUrl = process.env.SQS_DLQ_QUEUE_URL;
    process.env.SQS_INPUT_QUEUE_URL = inputQueueUrl;
    process.env.SQS_DLQ_QUEUE_URL = deadLetterQueueUrl;

    const wallet = await service.createWallet({
      playerId: `consumer-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const metrics = new MetricsService();
    const consumer = new WagerTransactionConsumerService(service, metrics);
    const queueDepth = async (queueUrl: string): Promise<number> => {
      const result = await sqsClient.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
          ],
        }),
      );
      return (
        Number(result.Attributes?.ApproximateNumberOfMessages ?? '0') +
        Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0')
      );
    };

    consumer.onModuleInit();
    try {
      const retryExternalId = `transient-retry-${randomUUID()}`;
      const exhaustedExternalId = `exhausted-retry-${randomUUID()}`;
      const installTransientFailure = async (
        failedIds: string[],
      ): Promise<void> => {
        const conditions = failedIds
          .map((id) => `new."external_transaction_id" = '${id}'`)
          .join(' or ');
        await getOrm().em.getConnection().execute(
          `create or replace function "fail_transient_wager_integration_test"()
           returns trigger language plpgsql as $$
           begin
             if ${conditions} then
               raise exception 'forced transient failure' using errcode = '40001';
             end if;
             return new;
           end;
           $$`,
        );
        await getOrm().em.getConnection().execute(
          `drop trigger if exists "fail_transient_wager_integration_test"
           on "wager_transactions"`,
        );
        await getOrm().em.getConnection().execute(
          `create trigger "fail_transient_wager_integration_test"
           before insert on "wager_transactions"
           for each row execute function "fail_transient_wager_integration_test"()`,
        );
      };

      await installTransientFailure([retryExternalId, exhaustedExternalId]);
      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: JSON.stringify({
            messageId: randomUUID(),
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: {
              idempotencyKey: `transient-retry-key-${randomUUID()}`,
              providerId: 'integration-provider',
              externalTransactionId: retryExternalId,
              playerId: wallet.playerId,
              walletId: wallet.id,
              roundId: `transient-retry-round-${randomUUID()}`,
              gameId: 'integration-game',
              kind: WagerTransactionKind.Bet,
              money: { amount: '10.00', currency: 'BRL' },
            },
          }),
          MessageGroupId: 'transient-retry',
          MessageDeduplicationId: randomUUID(),
        }),
      );
      await waitFor(
        async () =>
          metrics
            .renderPrometheus()
            .includes('wager_retries_total{source="sqs"} 1'),
        'the transient failure to schedule an SQS retry',
      );
      await installTransientFailure([exhaustedExternalId]);
      await waitFor(
        async () =>
          (await service.getWallet(wallet.id)).balance.amount === '90.00',
        'the transient SQS retry to succeed',
      );

      const transaction = {
        providerId: 'integration-provider',
        externalTransactionId: `consumer-bet-${randomUUID()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `consumer-round-${randomUUID()}`,
        gameId: 'integration-game',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
      };
      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: JSON.stringify({
            messageId: randomUUID(),
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: {
              idempotencyKey: `consumer-key-${randomUUID()}`,
              ...transaction,
            },
          }),
          MessageGroupId: 'valid-wagers',
          MessageDeduplicationId: randomUUID(),
        }),
      );
      await waitFor(
        async () =>
          (await service.getWallet(wallet.id)).balance.amount === '40.00' &&
          (await queueDepth(inputQueueUrl)) === 0,
        'a committed wager to be acknowledged',
      );

      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: JSON.stringify({
            idempotencyKey: `invalid-money-key-${randomUUID()}`,
            transaction: {
              ...transaction,
              externalTransactionId: `invalid-money-${randomUUID()}`,
              money: { amount: '1e3', currency: 'BRL' },
            },
          }),
          MessageGroupId: 'invalid-wagers',
          MessageDeduplicationId: randomUUID(),
        }),
      );
      await waitFor(
        async () =>
          (await queueDepth(inputQueueUrl)) === 0 &&
          (await queueDepth(deadLetterQueueUrl)) === 0,
        'a permanent business error to be acknowledged',
      );

      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: '{invalid-json',
          MessageGroupId: 'malformed-wagers',
          MessageDeduplicationId: randomUUID(),
        }),
      );
      await waitFor(
        async () =>
          (await queueDepth(inputQueueUrl)) === 0 &&
          (await queueDepth(deadLetterQueueUrl)) === 1,
        'a malformed message to be moved to the DLQ',
      );
      expect(metrics.renderPrometheus()).toContain(
        'wager_dlq_messages_total 1',
      );

      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: JSON.stringify({
            messageId: randomUUID(),
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: {
              idempotencyKey: `exhausted-retry-key-${randomUUID()}`,
              providerId: 'integration-provider',
              externalTransactionId: exhaustedExternalId,
              playerId: wallet.playerId,
              walletId: wallet.id,
              roundId: `exhausted-retry-round-${randomUUID()}`,
              gameId: 'integration-game',
              kind: WagerTransactionKind.Bet,
              money: { amount: '5.00', currency: 'BRL' },
            },
          }),
          MessageGroupId: 'exhausted-retry',
          MessageDeduplicationId: randomUUID(),
        }),
      );
      await waitFor(
        async () =>
          (await queueDepth(deadLetterQueueUrl)) === 2 &&
          metrics
            .renderPrometheus()
            .includes('wager_dlq_queue_depth 2'),
        'SQS to move an exhausted transient message to its redrive DLQ',
      );
    } finally {
      await getOrm().em.getConnection().execute(
        `drop trigger if exists "fail_transient_wager_integration_test"
         on "wager_transactions"`,
      );
      await getOrm().em.getConnection().execute(
        `drop function if exists "fail_transient_wager_integration_test"()`,
      );
      await consumer.onModuleDestroy();
      await deleteConsumerQueuePair(queues);
      if (previousInputUrl === undefined) {
        delete process.env.SQS_INPUT_QUEUE_URL;
      } else {
        process.env.SQS_INPUT_QUEUE_URL = previousInputUrl;
      }
      if (previousDlqUrl === undefined) {
        delete process.env.SQS_DLQ_QUEUE_URL;
      } else {
        process.env.SQS_DLQ_QUEUE_URL = previousDlqUrl;
      }
    }
  }, 45000);

  it('aborts the SQS long poll during graceful shutdown', async () => {
    const queues = await createConsumerQueuePair();
    const { inputQueueUrl } = queues;

    const previousInputUrl = process.env.SQS_INPUT_QUEUE_URL;
    process.env.SQS_INPUT_QUEUE_URL = inputQueueUrl;
    const consumer = new WagerTransactionConsumerService(
      service,
      new MetricsService(),
    );
    consumer.onModuleInit();
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const startedAt = Date.now();
      await consumer.onModuleDestroy();
      expect(Date.now() - startedAt).toBeLessThan(3000);
    } finally {
      if (previousInputUrl === undefined) {
        delete process.env.SQS_INPUT_QUEUE_URL;
      } else {
        process.env.SQS_INPUT_QUEUE_URL = previousInputUrl;
      }
      await deleteConsumerQueuePair(queues);
    }
  }, 10000);

  it('finishes in-flight work and returns unprocessed batch messages during shutdown', async () => {
    const queues = await createConsumerQueuePair();
    const { inputQueueUrl } = queues;

    const previousInputUrl = process.env.SQS_INPUT_QUEUE_URL;
    process.env.SQS_INPUT_QUEUE_URL = inputQueueUrl;
    const firstWallet = await service.createWallet({
      playerId: `shutdown-first-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const secondWallet = await service.createWallet({
      playerId: `shutdown-second-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const firstMessageId = randomUUID();
    const secondMessageId = randomUUID();
    const firstExternalId = `shutdown-first-${randomUUID()}`;
    const secondExternalId = `shutdown-second-${randomUUID()}`;
    const createEnvelope = (
      messageId: string,
      externalTransactionId: string,
      wallet: { id: string; playerId: string },
    ): string =>
      JSON.stringify({
        messageId,
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: {
          idempotencyKey: `shutdown-key-${messageId}`,
          providerId: 'integration-provider',
          externalTransactionId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `shutdown-round-${messageId}`,
          gameId: 'integration-game',
          kind: WagerTransactionKind.Bet,
          money: { amount: '10.00', currency: 'BRL' },
        },
      });

    await Promise.all([
      sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: createEnvelope(
            firstMessageId,
            firstExternalId,
            firstWallet,
          ),
          MessageGroupId: 'shutdown-first-group',
          MessageDeduplicationId: firstMessageId,
        }),
      ),
      sqsClient.send(
        new SendMessageCommand({
          QueueUrl: inputQueueUrl,
          MessageBody: createEnvelope(
            secondMessageId,
            secondExternalId,
            secondWallet,
          ),
          MessageGroupId: 'shutdown-second-group',
          MessageDeduplicationId: secondMessageId,
        }),
      ),
    ]);

    const pausingService = new PausingWageringService(getOrm().em);
    const consumer = new WagerTransactionConsumerService(
      pausingService,
      new MetricsService(),
    );
    let consumerStopped = false;
    consumer.onModuleInit();
    try {
      await waitFor(
        async () => pausingService.startedExternalTransactionId !== undefined,
        'consumer to start processing a batch',
      );
      await waitFor(async () => {
        const attributes = await sqsClient.send(
          new GetQueueAttributesCommand({
            QueueUrl: inputQueueUrl,
            AttributeNames: ['ApproximateNumberOfMessagesNotVisible'],
          }),
        );
        return Number(
          attributes.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0',
        ) >= 2;
      }, 'both FIFO messages to be received before shutdown');

      const shutdown = consumer.onModuleDestroy();
      pausingService.releaseProcessing();
      await shutdown;
      consumerStopped = true;
      const firstWasProcessed =
        pausingService.startedExternalTransactionId === firstExternalId;
      const processedWallet = firstWasProcessed ? firstWallet : secondWallet;
      const returnedExternalId = firstWasProcessed
        ? secondExternalId
        : firstExternalId;
      const unprocessedWallet = firstWasProcessed ? secondWallet : firstWallet;
      expect((await service.getWallet(processedWallet.id)).balance.amount).toBe(
        '90.00',
      );
      expect(
        (await service.getWallet(unprocessedWallet.id)).balance.amount,
      ).toBe('100.00');
      const returnedMessage = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: inputQueueUrl,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 2,
          VisibilityTimeout: 10,
        }),
      );
      const message = returnedMessage.Messages?.[0];
      expect(message?.Body).toContain(returnedExternalId);
      if (message?.ReceiptHandle !== undefined) {
        await sqsClient.send(
          new DeleteMessageCommand({
            QueueUrl: inputQueueUrl,
            ReceiptHandle: message.ReceiptHandle,
          }),
        );
      }
    } finally {
      pausingService.releaseProcessing();
      if (!consumerStopped) {
        await consumer.onModuleDestroy();
      }
      if (previousInputUrl === undefined) {
        delete process.env.SQS_INPUT_QUEUE_URL;
      } else {
        process.env.SQS_INPUT_QUEUE_URL = previousInputUrl;
      }
      await deleteConsumerQueuePair(queues);
    }
  }, 20000);

  it('blocks direct updates and deletes from the immutable ledger', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    const ledgerRows = (await getOrm()
      .em.getConnection()
      .execute(
        `select "id" from "wallet_ledger_entries" where "wallet_id" = ?`,
        [wallet.id],
        'all',
      )) as Array<{ id: string }>;
    const ledgerId = ledgerRows[0]?.id;

    expect(ledgerId).toBeString();
    await expect(
      getOrm().em.getConnection().execute(
        `update "wallet_ledger_entries" set "amount" = 1 where "id" = ?`,
        [ledgerId],
      ),
    ).rejects.toThrow();
    await expect(
      getOrm().em.getConnection().execute(
        `delete from "wallet_ledger_entries" where "id" = ?`,
        [ledgerId],
      ),
    ).rejects.toThrow();
  });

  it('enforces wallet, wager, and ledger constraints directly in PostgreSQL', async () => {
      const wallet = await service.createWallet({
        playerId: `schema-constraints-player-${randomUUID()}`,
        initialBalance: { amount: '0.00', currency: 'BRL' },
      });
      const connection = getOrm().em.getConnection();
      const now = new Date();

      await expect(
        connection.execute(
          `insert into "wallets"
            ("id", "player_id", "currency", "balance", "version", "created_at", "updated_at")
           values (?, ?, 'BRL', 0, 1, ?, ?)`,
          [randomUUID(), wallet.playerId, now, now],
        ),
      ).rejects.toThrow();
      await expect(
        connection.execute(`update "wallets" set "balance" = -1 where "id" = ?`, [
          wallet.id,
        ]),
      ).rejects.toThrow();

      const transactionId = randomUUID();
      const externalTransactionId = `schema-constraint-${randomUUID()}`;
      const idempotencyKey = `schema-constraint-key-${randomUUID()}`;
      const insertTransaction = (
        id: string,
        externalId: string,
        key: string,
      ) =>
        connection.execute(
          `insert into "wager_transactions"
            ("id", "provider_id", "external_transaction_id", "idempotency_key",
             "payload_hash", "wallet_id", "player_id", "currency", "round_id",
             "game_id", "kind", "status", "amount", "created_at")
           values (?, 'schema-test-provider', ?, ?, ?, ?, ?, 'BRL',
             'schema-round', 'schema-game', 'BET', 'PENDING', 1, now())`,
          [
            id,
            externalId,
            key,
            'd'.repeat(64),
            wallet.id,
            wallet.playerId,
          ],
        );

      await insertTransaction(
        transactionId,
        externalTransactionId,
        idempotencyKey,
      );
      await expect(
        insertTransaction(
          randomUUID(),
          `different-external-${randomUUID()}`,
          idempotencyKey,
        ),
      ).rejects.toThrow();
      await expect(
        insertTransaction(
          randomUUID(),
          externalTransactionId,
          `different-key-${randomUUID()}`,
        ),
      ).rejects.toThrow();
      await expect(
        connection.execute(
          `insert into "wallet_ledger_entries"
            ("id", "wallet_id", "transaction_id", "direction", "amount",
             "currency", "balance_before", "balance_after", "created_at")
           values (?, ?, ?, 'CREDIT', 1, 'BRL', 0, 2, now())`,
          [randomUUID(), wallet.id, transactionId],
        ),
      ).rejects.toThrow();
    });

  it('applies and reverses migrations in an isolated PostgreSQL database', async () => {
      const databaseName = `wager_migration_test_${randomUUID().replace(/-/g, '')}`;
      const adminPool = new Pool({
        host: process.env.DB_HOST ?? '127.0.0.1',
        port: Number.parseInt(process.env.DB_PORT ?? '5433', 10),
        database: 'postgres',
        user: process.env.DB_USER ?? 'wager',
        password: process.env.DB_PASSWORD ?? 'wager_dev_only',
      });
      let isolatedOrm: MikroORM | undefined;
      try {
        await adminPool.query(`create database "${databaseName}"`);
        isolatedOrm = await MikroORM.init({
          ...mikroOrmConfig,
          dbName: databaseName,
        });
        await isolatedOrm.getMigrator().up();
        const applied = await isolatedOrm
          .em.getConnection()
          .execute(`select to_regclass('public.wallets') as table_name`, [], 'get');
        expect(applied.table_name).toBe('wallets');

        await isolatedOrm.getMigrator().down();
        const reverted = await isolatedOrm
          .em.getConnection()
          .execute(`select to_regclass('public.wallets') as table_name`, [], 'get');
        expect(reverted.table_name).toBeNull();
      } finally {
        await isolatedOrm?.close(true);
        await adminPool.query(`drop database if exists "${databaseName}" with (force)`);
        await adminPool.end();
      }
    });
  it('reprocesses an out-of-order reference without changing the original pending response', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const providerId = 'integration-provider';
    const roundId = `integration-round-${randomUUID()}`;
    const betExternalId = `late-bet-${randomUUID()}`;
    const winCommand = {
      providerId,
      externalTransactionId: `early-win-${randomUUID()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId,
      gameId: 'integration-game',
      kind: WagerTransactionKind.Win,
      money: { amount: '10.00', currency: 'BRL' },
      referenceExternalTransactionId: betExternalId,
    } as const;
    const winIdempotencyKey = `early-win-idempotency-${randomUUID()}`;

    const pending = await service.submitWager(winCommand, winIdempotencyKey);
    expect(pending.body.status).toBe(WagerTransactionStatus.PendingReference);
    expect(pending.body.balance.amount).toBe('100.00');

    await service.submitWager(
      {
        providerId,
        externalTransactionId: betExternalId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: 'integration-game',
        kind: WagerTransactionKind.Bet,
        money: { amount: '10.00', currency: 'BRL' },
      },
      `late-bet-idempotency-${randomUUID()}`,
    );

    const originalReplay = await service.submitWager(
      winCommand,
      winIdempotencyKey,
    );
    expect(originalReplay.body.idempotentReplay).toBe(true);
    expect(originalReplay.body.balance.amount).toBe('100.00');

    await new Promise((resolve) => setTimeout(resolve, 1200));
    await service.processPendingReferences();

    const processed = await service.getTransaction(
      pending.body.transactionId,
    );
    expect(processed.status).toBe(WagerTransactionStatus.Processed);
  });

  it('reports wallet-ledger divergence without correcting the stored balance', async () => {
    const wallet = await service.createWallet({
      playerId: `integration-player-${randomUUID()}`,
      initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    await getOrm().em.getConnection().execute(
      `update "wallets" set "balance" = '11.00' where "id" = ?`,
      [wallet.id],
    );

    const result = await service.reconcileWallet(wallet.id);
    expect(result.consistent).toBe(false);
    expect(result.storedBalance).toEqual({ amount: '11.00', currency: 'BRL' });
    expect(result.ledgerBalance).toEqual({ amount: '10.00', currency: 'BRL' });
    expect(result.difference).toEqual({ amount: '1.00', currency: 'BRL' });

    const persisted = await service.getWallet(wallet.id);
    expect(persisted.balance).toEqual({ amount: '11.00', currency: 'BRL' });
  });

  it('lets concurrent publishers claim outbox work with SKIP LOCKED', async () => {
    const eventId = randomUUID();
    const aggregateId = randomUUID();
    await getOrm().em.getConnection().execute(
      `insert into "outbox_messages"
        ("id", "aggregate_id", "event_type", "payload", "occurred_at")
       values (?, ?, 'PublisherIntegrationTest',
         ?::jsonb, '2000-01-01T00:00:00Z')`,
      [
        eventId,
        aggregateId,
        JSON.stringify({ eventId, eventType: 'PublisherIntegrationTest' }),
      ],
    );

    if (eventQueueUrl === undefined) {
      throw new Error('LocalStack test queue was not created');
    }
    const previousQueueUrl = process.env.SQS_EVENTS_QUEUE_URL;
    const metrics = new MetricsService();
    process.env.SQS_EVENTS_QUEUE_URL =
      eventQueueUrl.replace(/[^/]+$/, `missing-${randomUUID()}.fifo`);
    const failingPublisher = new OutboxPublisherService(
      getOrm().em,
      metrics,
    );
    await failingPublisher.publishPendingOnce();
    await failingPublisher.onModuleDestroy();

    const retryRow = (await getOrm().em.getConnection().execute(
      `select "attempts", "next_attempt_at"
       from "outbox_messages" where "id" = ?`,
      [eventId],
      'get',
    )) as { attempts: string; next_attempt_at: Date | null };
    expect(Number(retryRow.attempts)).toBe(1);
    expect(retryRow.next_attempt_at).not.toBeNull();
    const retryMetric = metrics
      .renderPrometheus()
      .match(/wager_retries_total\{source="outbox"\} (\d+)/);
    expect(Number(retryMetric?.[1])).toBeGreaterThan(0);
    await getOrm().em.getConnection().execute(
      `update "outbox_messages" set "next_attempt_at" = now() - interval '1 second'
       where "id" = ?`,
      [eventId],
    );

    process.env.SQS_EVENTS_QUEUE_URL = eventQueueUrl;
    const firstPublisher = new OutboxPublisherService(getOrm().em, metrics);
    const secondPublisher = new OutboxPublisherService(getOrm().em, metrics);
    try {
      await Promise.all([
        firstPublisher.publishPendingOnce(),
        secondPublisher.publishPendingOnce(),
      ]);
    } finally {
      await Promise.all([
        firstPublisher.onModuleDestroy(),
        secondPublisher.onModuleDestroy(),
      ]);
      if (previousQueueUrl === undefined) {
        delete process.env.SQS_EVENTS_QUEUE_URL;
      } else {
        process.env.SQS_EVENTS_QUEUE_URL = previousQueueUrl;
      }
    }

    const publishedRows = await getOrm().em.getConnection().execute(
      `select "published_at" from "outbox_messages" where "id" = ?`,
      [eventId],
      'all',
    ) as Array<{ published_at: Date | null }>;
    expect(publishedRows[0]?.published_at).not.toBeNull();

    let matchingMessages = 0;
    for (let attempt = 0; attempt < 5 && matchingMessages === 0; attempt += 1) {
      const received = await sqsClient.send(
        new ReceiveMessageCommand({
          QueueUrl: eventQueueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
        }),
      );
      for (const message of received.Messages ?? []) {
        const body = JSON.parse(message.Body ?? '{}') as { eventId?: string };
        if (body.eventId === eventId) {
          matchingMessages += 1;
        }
        if (message.ReceiptHandle !== undefined) {
          await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: eventQueueUrl,
              ReceiptHandle: message.ReceiptHandle,
            }),
          );
        }
      }
    }
    expect(matchingMessages).toBe(1);
  }, 45000);
});
